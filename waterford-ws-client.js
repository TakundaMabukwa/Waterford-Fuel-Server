const WebSocket = require('ws');
const { decodeFuelData, hasFuelData } = require('./waterford-fuel-decoder');
const db = require('./waterford-db');
const { findFuelStop } = require('./waterford-geozone');

const createClient = (wsUrl) => {
  let ws = null;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let messageCount = 0;
  const theftTracking = {};
  const lastEngineStatus = {};
  const geozoneTracking = {};

  const parseMessage = (raw) => {
    if (!raw || raw.length < 3) return null;

    let trimmed = raw;
    if (trimmed.startsWith('^')) trimmed = trimmed.substring(1);
    if (trimmed.endsWith('^')) trimmed = trimmed.substring(0, trimmed.length - 1);

    const parts = trimmed.split('|');
    if (parts.length < 12) return null;

    const plate = (parts[0] || '').trim();
    if (!plate) return null;

    return {
      plate,
      speed: parseFloat(parts[1]) || 0,
      latitude: parseFloat(parts[2]) || 0,
      longitude: parseFloat(parts[3]) || 0,
      loc_time: (parts[4] || '').trim(),
      mileage: parseInt(parts[5]) || 0,
      pocsagstr: (parts[6] || '').trim(),
      status: (parts[7] || '').trim(),
      fuelDataRaw: (parts[8] || '').trim(),
      item_installed: (parts[9] || '').trim(),
      geozone: (parts[10] || '').trim(),
      driver_name: (parts[11] || '').trim()
    };
  };

  const buildRow = (msg, decoded) => {
    const row = {
      plate: msg.plate,
      cost_code: db.getCostCode(msg.plate),
      speed: msg.speed,
      latitude: msg.latitude,
      longitude: msg.longitude,
      loc_time: msg.loc_time,
      mileage: msg.mileage,
      pocsagstr: msg.pocsagstr,
      status: msg.status,
      message_type: decoded ? decoded.messageType : null,
      fuel_probe_1_level: null,
      fuel_probe_1_volume_in_tank: null,
      fuel_probe_1_temperature: null,
      fuel_probe_1_level_percentage: null,
      fuel_probe_2_level: null,
      fuel_probe_2_volume_in_tank: null,
      fuel_probe_2_temperature: null,
      fuel_probe_2_level_percentage: null,
      item_installed: msg.item_installed,
      geozone: msg.geozone,
      driver_name: msg.driver_name,
      raw_fuel_data: msg.fuelDataRaw || null
    };

    if (decoded?.tank1) {
      row.fuel_probe_1_level = decoded.tank1.level ?? null;
      row.fuel_probe_1_volume_in_tank = decoded.tank1.volume ?? null;
      row.fuel_probe_1_temperature = decoded.tank1.temperature ?? null;
      row.fuel_probe_1_level_percentage = decoded.tank1.percentage ?? null;
    }

    if (decoded?.tank2) {
      row.fuel_probe_2_level = decoded.tank2.level ?? null;
      row.fuel_probe_2_volume_in_tank = decoded.tank2.volume ?? null;
      row.fuel_probe_2_temperature = decoded.tank2.temperature ?? null;
      row.fuel_probe_2_level_percentage = decoded.tank2.percentage ?? null;
    }

    return row;
  };

  const logStatusEvents = (msg) => {
    const status = (msg.status || '').toUpperCase();
    if (!status) return;

    const plate = msg.plate;
    const time = msg.loc_time;

    if (status.includes('ENGINE ON') || status.includes('IGNITION ON') || status.includes('PTO ON')) {
      console.log(`[event] ENGINE ON: ${plate} at ${time}`);
    } else if (status.includes('ENGINE OFF') || status.includes('IGNITION OFF') || status.includes('PTO OFF')) {
      console.log(`[event] ENGINE OFF: ${plate} at ${time}`);
    } else if (status.includes('POSSIBLE FUEL THEFT')) {
      console.log(`[event] FUEL THEFT: ${plate} at ${time}`);
    }
  };

  const handleMessage = async (raw) => {
    const msg = parseMessage(raw);
    if (!msg || !db.isKnownVehicle(msg.plate)) return;

    const decoded = msg.fuelDataRaw ? decodeFuelData(msg.fuelDataRaw) : null;
    const messageType = decoded?.messageType;

    messageCount++;
    if (messageCount % 100 === 0) {
      console.log(`[ws] ${messageCount} messages processed (latest: ${msg.plate})`);
    }

    logStatusEvents(msg);
    trackEngineStatus(msg);

    const row = buildRow(msg, decoded);
    await db.insertHistory(row);

    if (messageType === 405 && hasFuelData(decoded)) {
      await db.upsertLatest(row);
    }

    await processTheftStatus(msg, decoded);
    await processTheftFuelReading(msg, decoded);
    await processGeozone(msg, decoded);
  };

  const trackEngineStatus = (msg) => {
    const status = (msg.status || '').toUpperCase();
    const plate = msg.plate;
    if (status.includes('ENGINE OFF') || status.includes('IGNITION OFF') || status.includes('PTO OFF')) {
      lastEngineStatus[plate] = { off: true, time: msg.loc_time };
    } else if (status.includes('ENGINE ON') || status.includes('IGNITION ON') || status.includes('PTO ON')) {
      lastEngineStatus[plate] = { off: false, time: msg.loc_time };
      delete theftTracking[plate];
    }
  };

  const isVehicleEngineOff = (plate) => {
    const state = lastEngineStatus[plate];
    return state && state.off;
  };

  const isEngineOn = (msg) => {
    const status = (msg.status || '').toUpperCase();
    return status.includes('ENGINE ON') || status.includes('IGNITION ON') || status.includes('PTO ON');
  };

  const isEngineOff = (msg) => {
    const status = (msg.status || '').toUpperCase();
    return status.includes('ENGINE OFF') || status.includes('IGNITION OFF') || status.includes('PTO OFF');
  };

  const getCombinedFuel = (decoded) => {
    if (!decoded || !hasFuelData(decoded)) return 0;
    const p1 = parseFloat(decoded?.tank1?.volume) || 0;
    const p2 = parseFloat(decoded?.tank2?.volume) || 0;
    return p1 + p2;
  };

  const processGeozone = async (msg, decoded) => {
    const plate = msg.plate;

    if (!msg.latitude || !msg.longitude) return;

    const fuelStop = await findFuelStop(msg.latitude, msg.longitude);
    const tracking = geozoneTracking[plate];
    const wasInZone = tracking && tracking.inZone;
    const isInZone = fuelStop !== null;

    if (!wasInZone && isInZone) {
      geozoneTracking[plate] = {
        inZone: true,
        fuelStopId: fuelStop.id,
        zoneName: fuelStop.name || fuelStop.geozone_name || 'Unknown',
        engineOffInZone: false,
        baselineFuel: 0,
        baselineProbe1: 0,
        baselineProbe2: 0,
        baselineLocTime: null,
        engineOnReceived: false,
        fuelReadingsCount: 0,
        fillCompleted: false,
      };

      console.log(`[geozone] ZONE ENTER: ${plate} entered "${fuelStop.name}" (id:${fuelStop.id}) at ${msg.loc_time}`);

      await db.insertGeozoneEvent({
        plate,
        fuel_stop_id: fuelStop.id,
        geozone_name: fuelStop.name || fuelStop.geozone_name,
        event_type: 'ZONE_ENTER',
        loc_time: msg.loc_time,
        latitude: msg.latitude,
        longitude: msg.longitude,
      });

      if (isEngineOff(msg)) {
        geozoneTracking[plate].engineOffInZone = true;

        const currentFuel = getCombinedFuel(decoded);
        let baselineFuel = currentFuel;

        if (currentFuel <= 0) {
          const latestFuel = await db.getLastNFuelReadings(plate, 1);
          if (latestFuel.length > 0) {
            baselineFuel = (latestFuel[0].fuel_probe_1_volume_in_tank || 0) +
                          (latestFuel[0].fuel_probe_2_volume_in_tank || 0);
            geozoneTracking[plate].baselineProbe1 = latestFuel[0].fuel_probe_1_volume_in_tank || 0;
            geozoneTracking[plate].baselineProbe2 = latestFuel[0].fuel_probe_2_volume_in_tank || 0;
          }
        } else {
          geozoneTracking[plate].baselineProbe1 = parseFloat(decoded?.tank1?.volume) || 0;
          geozoneTracking[plate].baselineProbe2 = parseFloat(decoded?.tank2?.volume) || 0;
        }

        geozoneTracking[plate].baselineFuel = baselineFuel;
        geozoneTracking[plate].baselineLocTime = msg.loc_time;

        console.log(`[geozone] ENGINE OFF IN ZONE: ${plate} - baseline: ${baselineFuel}L at ${msg.loc_time}`);
      }

      return;
    }

    if (wasInZone && !isInZone) {
      console.log(`[geozone] ZONE EXIT: ${plate} left "${tracking.zoneName}" at ${msg.loc_time}`);

      await db.insertGeozoneEvent({
        plate,
        fuel_stop_id: tracking.fuelStopId,
        geozone_name: tracking.zoneName,
        event_type: 'ZONE_EXIT',
        loc_time: msg.loc_time,
        latitude: msg.latitude,
        longitude: msg.longitude,
      });

      delete geozoneTracking[plate];
      return;
    }

    if (!wasInZone || !isInZone) return;

    if (isEngineOff(msg) && !tracking.engineOffInZone) {
      tracking.engineOffInZone = true;
      tracking.engineOnReceived = false;
      tracking.fuelReadingsCount = 0;
      tracking.fillCompleted = false;

      const currentFuel = getCombinedFuel(decoded);
      let baselineFuel = currentFuel;

      if (currentFuel <= 0) {
        const latestFuel = await db.getLastNFuelReadings(plate, 1);
        if (latestFuel.length > 0) {
          baselineFuel = (latestFuel[0].fuel_probe_1_volume_in_tank || 0) +
                        (latestFuel[0].fuel_probe_2_volume_in_tank || 0);
          tracking.baselineProbe1 = latestFuel[0].fuel_probe_1_volume_in_tank || 0;
          tracking.baselineProbe2 = latestFuel[0].fuel_probe_2_volume_in_tank || 0;
        }
      } else {
        tracking.baselineProbe1 = parseFloat(decoded?.tank1?.volume) || 0;
        tracking.baselineProbe2 = parseFloat(decoded?.tank2?.volume) || 0;
      }

      tracking.baselineFuel = baselineFuel;
      tracking.baselineLocTime = msg.loc_time;

      console.log(`[geozone] ENGINE OFF IN ZONE: ${plate} - baseline: ${baselineFuel}L at ${msg.loc_time}`);
      return;
    }

    if (isEngineOn(msg) && tracking.engineOffInZone && !tracking.engineOnReceived) {
      tracking.engineOnReceived = true;
      tracking.fuelReadingsCount = 0;

      console.log(`[geozone] ENGINE ON IN ZONE: ${plate} - will read next 3 fuel messages at ${msg.loc_time}`);
      return;
    }

    if (tracking.engineOnReceived && !tracking.fillCompleted && hasFuelData(decoded)) {
      tracking.fuelReadingsCount++;
      const currentFuel = getCombinedFuel(decoded);

      if (currentFuel > 0 && tracking.fuelReadingsCount >= 3) {
        const fillAmount = currentFuel - tracking.baselineFuel;

        if (fillAmount > 1) {
          const startTimeIso = tracking.baselineLocTime
            ? new Date(tracking.baselineLocTime).toISOString()
            : new Date().toISOString();
          const endTimeIso = msg.loc_time ? new Date(msg.loc_time).toISOString() : new Date().toISOString();
          const startDate = tracking.baselineLocTime
            ? tracking.baselineLocTime.split('T')[0]
            : new Date().toISOString().split('T')[0];

          try {
            await db.insertFillSession({
              branch: plate,
              company: 'WATERFORD',
              cost_code: db.getCostCode(plate),
              session_date: startDate,
              session_start_time: startTimeIso,
              session_end_time: endTimeIso,
              operating_hours: 0,
              opening_fuel: tracking.baselineFuel,
              opening_fuel_probe_1: tracking.baselineProbe1,
              opening_fuel_probe_2: tracking.baselineProbe2,
              opening_percentage: 0,
              opening_percentage_probe_1: 0,
              opening_percentage_probe_2: 0,
              closing_fuel: currentFuel,
              closing_fuel_probe_1: parseFloat(decoded?.tank1?.volume) || 0,
              closing_fuel_probe_2: parseFloat(decoded?.tank2?.volume) || 0,
              closing_percentage: 0,
              closing_percentage_probe_1: 0,
              closing_percentage_probe_2: 0,
              total_fill: fillAmount,
              session_status: 'FUEL_FILL_COMPLETED',
              notes: `Geozone fill detected at "${tracking.zoneName}". Baseline: ${tracking.baselineFuel}L, After: ${currentFuel}L, Filled: ${fillAmount.toFixed(1)}L`,
              fill_data: {
                engine_off_time: tracking.baselineLocTime,
                fuel_stop_id: tracking.fuelStopId,
                zone_name: tracking.zoneName,
                detection_method: 'geozone'
              }
            });

            await db.insertGeozoneEvent({
              plate,
              fuel_stop_id: tracking.fuelStopId,
              geozone_name: tracking.zoneName,
              event_type: 'FILL_DETECTED',
              loc_time: msg.loc_time,
              latitude: msg.latitude,
              longitude: msg.longitude,
              fuel_before: tracking.baselineFuel,
              fuel_after: currentFuel,
              fill_amount: fillAmount,
            });

            console.log(`[geozone] FILL RECORDED: ${plate} at "${tracking.zoneName}" - ${tracking.baselineFuel}L -> ${currentFuel}L = +${fillAmount.toFixed(1)}L`);
          } catch (err) {
            console.error(`[geozone] Failed to record fill for ${plate}: ${err.message}`);
          }
        } else {
          console.log(`[geozone] FILL TOO SMALL: ${plate} at "${tracking.zoneName}" - ${tracking.baselineFuel}L -> ${currentFuel}L = ${fillAmount.toFixed(1)}L (skipped)`);
        }

        tracking.fillCompleted = true;
      }
    }
  };

  const processTheftStatus = async (msg, decoded) => {
    const status = (msg.status || '').toUpperCase();
    const plate = msg.plate;
    if (!status.includes('POSSIBLE FUEL THEFT')) return;
    if (theftTracking[plate]) return;
    if (!isVehicleEngineOff(plate)) {
      console.log(`[theft] Ignored POSSIBLE FUEL THEFT for ${plate} - engine not OFF`);
      return;
    }

    const alreadyRecorded = await db.checkTheftRecorded(plate, lastEngineStatus[plate].time);
    if (alreadyRecorded) {
      console.log(`[theft] Ignored POSSIBLE FUEL THEFT for ${plate} - already recorded`);
      return;
    }

    const engineOffTime = lastEngineStatus[plate].time;
    const readings = await db.getFuelReadingsBetween(plate, engineOffTime, msg.loc_time);
    if (readings.length === 0) {
      console.log(`[theft] No fuel readings found between engine off and theft status for ${plate}`);
      return;
    }

    let highestFuel = 0;
    let highestProbe1 = 0;
    let highestProbe2 = 0;
    let highestLocTime = null;

    for (const r of readings) {
      const p1 = r.fuel_probe_1_volume_in_tank || 0;
      const p2 = r.fuel_probe_2_volume_in_tank || 0;
      const combined = p1 + p2;
      if (combined > highestFuel) {
        highestFuel = combined;
        highestProbe1 = p1;
        highestProbe2 = p2;
        highestLocTime = r.loc_time;
      }
    }

    if (highestFuel <= 0) {
      console.log(`[theft] No valid fuel baseline found for ${plate}`);
      return;
    }

    const currentP1 = parseFloat(decoded?.tank1?.volume) || 0;
    const currentP2 = parseFloat(decoded?.tank2?.volume) || 0;
    const currentFuel = currentP1 + currentP2;

    theftTracking[plate] = {
      baselineFuel: highestFuel,
      baselineProbe1: highestProbe1,
      baselineProbe2: highestProbe2,
      lowestFuel: currentFuel,
      lowestProbe1: currentP1,
      lowestProbe2: currentP2,
      lowestLocTime: msg.loc_time,
      consecutiveNoDecrease: 0,
      startTime: msg.loc_time,
      engineOffTime: engineOffTime,
      costCode: db.getCostCode(plate)
    };

    console.log(`[theft] THEFT TRACKING STARTED: ${plate} - baseline: ${highestFuel}L at ${highestLocTime}`);
  };

  const processTheftFuelReading = async (msg, decoded) => {
    const plate = msg.plate;
    const tracking = theftTracking[plate];
    if (!tracking) return;
    if (!decoded || !hasFuelData(decoded)) return;

    const currentP1 = parseFloat(decoded?.tank1?.volume) || 0;
    const currentP2 = parseFloat(decoded?.tank2?.volume) || 0;
    const currentFuel = currentP1 + currentP2;
    if (currentFuel <= 0) return;

    if (currentFuel < tracking.lowestFuel) {
      tracking.lowestFuel = currentFuel;
      tracking.lowestProbe1 = currentP1;
      tracking.lowestProbe2 = currentP2;
      tracking.lowestLocTime = msg.loc_time;
      tracking.consecutiveNoDecrease = 0;
    } else {
      tracking.consecutiveNoDecrease++;
    }

    if (tracking.consecutiveNoDecrease >= 3) {
      await completeTheft(plate, tracking);
    }
  };

  const completeTheft = async (plate, tracking) => {
    const theftAmount = tracking.baselineFuel - tracking.lowestFuel;
    if (theftAmount < 1) {
      console.log(`[theft] Skipping theft for ${plate} - change too small (${theftAmount.toFixed(1)}L)`);
      delete theftTracking[plate];
      return;
    }

    const startDate = tracking.startTime.split('T')[0];
    const startTimeIso = new Date(tracking.startTime).toISOString();
    const endTimeIso = tracking.lowestLocTime
      ? new Date(tracking.lowestLocTime).toISOString()
      : new Date().toISOString();
    const durationSeconds = (new Date(endTimeIso).getTime() - new Date(startTimeIso).getTime()) / 1000;

    try {
      await db.insertTheftSession({
        branch: plate,
        company: 'WATERFORD',
        cost_code: tracking.costCode,
        session_date: startDate,
        session_start_time: startTimeIso,
        session_end_time: endTimeIso,
        operating_hours: durationSeconds / 3600,
        opening_fuel: tracking.baselineFuel,
        opening_fuel_probe_1: tracking.baselineProbe1,
        opening_fuel_probe_2: tracking.baselineProbe2,
        opening_percentage: 0,
        opening_percentage_probe_1: 0,
        opening_percentage_probe_2: 0,
        closing_fuel: tracking.lowestFuel,
        closing_fuel_probe_1: tracking.lowestProbe1,
        closing_fuel_probe_2: tracking.lowestProbe2,
        closing_percentage: 0,
        closing_percentage_probe_1: 0,
        closing_percentage_probe_2: 0,
        total_fill: -theftAmount,
        session_status: 'FUEL_THEFT_COMPLETED',
        notes: `Theft detected. Baseline: ${tracking.baselineFuel}L, Lowest: ${tracking.lowestFuel}L, Lost: ${theftAmount.toFixed(1)}L`,
        fill_data: { engine_off_time: tracking.engineOffTime }
      });

      console.log(`[theft] THEFT RECORDED: ${plate} - ${tracking.baselineFuel}L -> ${tracking.lowestFuel}L = -${theftAmount.toFixed(1)}L`);
    } catch (err) {
      console.error(`[theft] Failed to record theft for ${plate}:`, err.message);
    }

    delete theftTracking[plate];
  };

  const scheduleReconnect = () => {
    if (reconnectTimer) return;
    const delay = Math.min(5000 * Math.pow(2, reconnectAttempts), 60000);
    reconnectAttempts++;
    console.log(`[ws] Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delay);
  };

  const connect = () => {
    console.log(`[ws] Connecting to ${wsUrl}`);
    ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      console.log('[ws] Connected');
      reconnectAttempts = 0;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    });

    ws.on('message', (data) => {
      handleMessage(data.toString().trim()).catch((err) => {
        console.error('[ws] Error:', err.message);
      });
    });

    ws.on('close', () => {
      console.log('[ws] Disconnected');
      scheduleReconnect();
    });

    ws.on('error', (err) => {
      console.error('[ws] Error:', err.message);
    });
  };

  const close = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (ws) {
      ws.close();
      ws = null;
    }
  };

  return { connect, close };
};

module.exports = { createClient };
