const WebSocket = require('ws');
const { decodeFuelData, hasFuelData } = require('./waterford-fuel-decoder');
const db = require('./waterford-db');
const { findFuelStop, insertFuelReviewAction } = require('./waterford-geozone');

const createClient = (wsUrl) => {
  let ws = null;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let messageCount = 0;
  const geozoneTracking = {};
  const theftMonitoring = {};

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
      console.log(`[event] POSSIBLE FUEL THEFT STATUS: ${plate} at ${time}`);
    }
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

  const combinedFuelFromRow = (row) => (row.fuel_probe_1_volume_in_tank || 0) + (row.fuel_probe_2_volume_in_tank || 0);

  const locTimeDiffMinutes = (time1, time2) => {
    if (!time1 || !time2) return 0;
    const d1 = new Date(time1);
    const d2 = new Date(time2);
    if (isNaN(d1.getTime()) || isNaN(d2.getTime())) return 0;
    return Math.abs(d2.getTime() - d1.getTime()) / (1000 * 60);
  };

  const handleMessage = async (raw) => {
    const msg = parseMessage(raw);
    if (!msg || !db.isKnownVehicle(msg.plate)) return;

    const decoded = msg.fuelDataRaw ? decodeFuelData(msg.fuelDataRaw) : null;

    messageCount++;
    if (messageCount % 100 === 0) {
      console.log(`[ws] ${messageCount} messages processed (latest: ${msg.plate})`);
    }

    logStatusEvents(msg);

    const row = buildRow(msg, decoded);
    await db.insertHistory(row);

    if (hasFuelData(decoded)) {
      await db.upsertLatest(row);
    }

    await processGeozone(msg, decoded);
    await processTheftDetection(msg, decoded);
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
        zoneEnterTime: msg.loc_time,
        minFuel: null,
        minTime: null,
        maxFuel: null,
        maxTime: null,
        fuelFound: false,
      };

      console.log(`[geozone] ZONE ENTER: ${plate} entered "${fuelStop.name}" at ${msg.loc_time}`);

      await db.insertGeozoneEvent({
        plate,
        fuel_stop_id: fuelStop.id,
        geozone_name: fuelStop.name || fuelStop.geozone_name,
        event_type: 'ZONE_ENTER',
        loc_time: msg.loc_time,
        latitude: msg.latitude,
        longitude: msg.longitude,
      });

      await initFuelBaseline(plate, msg.loc_time);
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

      if (tracking.fuelFound && tracking.minFuel !== null && tracking.maxFuel !== null) {
        const diff = tracking.maxFuel - tracking.minFuel;

        if (diff > 0) {
          await recordGeozoneFill(plate, tracking, diff);
        } else {
          console.log(`[geozone] NO FILL: ${plate} - min: ${tracking.minFuel}L, max: ${tracking.maxFuel}L, diff: ${diff.toFixed(1)}L (skipped)`);
        }
      } else {
        console.log(`[geozone] NO FUEL DATA: ${plate} - no fuel readings during zone visit (skipped)`);
      }

      delete geozoneTracking[plate];
      return;
    }

    if (!wasInZone || !isInZone) return;

    await updateFuelTracking(plate, msg.loc_time);
  };

  const initFuelBaseline = async (plate, locTime) => {
    const tracking = geozoneTracking[plate];
    if (!tracking) return;

    const lowest = await db.getLowestFuelBetween(plate, locTime, locTime);
    const highest = await db.getHighestFuelBetween(plate, locTime, locTime);

    if (lowest) {
      const low = combinedFuelFromRow(lowest);
      tracking.minFuel = low;
      tracking.minTime = lowest.loc_time;
      tracking.maxFuel = low;
      tracking.maxTime = lowest.loc_time;
      tracking.fuelFound = true;
      console.log(`[geozone] BASELINE SET: ${plate} - fuel: ${low}L at ${lowest.loc_time}`);
    } else {
      console.log(`[geozone] WAITING FOR FUEL: ${plate} - no fuel data yet after ${locTime}`);
      startFuelRetry(plate, locTime);
    }
  };

  const startFuelRetry = async (plate, locTime) => {
    const tracking = geozoneTracking[plate];
    if (!tracking || !tracking.inZone) return;

    tracking.retryTimer = setTimeout(async () => {
      if (!geozoneTracking[plate] || !geozoneTracking[plate].inZone) return;

      const lowest = await db.getLowestFuelBetween(plate, locTime, locTime);
      if (lowest) {
        const low = combinedFuelFromRow(lowest);
        tracking.minFuel = low;
        tracking.minTime = lowest.loc_time;
        tracking.maxFuel = low;
        tracking.maxTime = lowest.loc_time;
        tracking.fuelFound = true;
        console.log(`[geozone] BASELINE SET (retry): ${plate} - fuel: ${low}L at ${lowest.loc_time}`);
      } else {
        console.log(`[geozone] STILL WAITING: ${plate} - retrying in 10 min`);
        startFuelRetry(plate, locTime);
      }
    }, 10 * 60 * 1000);
  };

  const updateFuelTracking = async (plate, locTime) => {
    const tracking = geozoneTracking[plate];
    if (!tracking || !tracking.inZone || !tracking.fuelFound) return;

    const lowest = await db.getLowestFuelBetween(plate, tracking.zoneEnterTime, locTime);
    const highest = await db.getHighestFuelBetween(plate, tracking.zoneEnterTime, locTime);

    if (lowest) {
      const low = combinedFuelFromRow(lowest);
      if (tracking.minFuel === null || low < tracking.minFuel) {
        tracking.minFuel = low;
        tracking.minTime = lowest.loc_time;
      }
    }

    if (highest) {
      const high = combinedFuelFromRow(highest);
      if (tracking.maxFuel === null || high > tracking.maxFuel) {
        tracking.maxFuel = high;
        tracking.maxTime = highest.loc_time;
      }
    }
  };

  const recordGeozoneFill = async (plate, tracking, diff) => {
    const sessionDate = tracking.zoneEnterTime ? tracking.zoneEnterTime.split('T')[0] : new Date().toISOString().split('T')[0];
    const startTime = tracking.minTime ? new Date(tracking.minTime).toISOString() : new Date().toISOString();
    const endTime = tracking.maxTime ? new Date(tracking.maxTime).toISOString() : new Date().toISOString();

    console.log(`[geozone] FILL RECORDED: ${plate} at "${tracking.zoneName}" - ${tracking.minFuel}L -> ${tracking.maxFuel}L = +${diff.toFixed(1)}L`);

    try {
      await db.insertFillSession({
        branch: plate,
        company: 'WATERFORD',
        cost_code: db.getCostCode(plate),
        session_date: sessionDate,
        session_start_time: startTime,
        session_end_time: endTime,
        operating_hours: 0,
        opening_fuel: tracking.minFuel,
        opening_fuel_probe_1: 0,
        opening_fuel_probe_2: 0,
        opening_percentage: 0,
        opening_percentage_probe_1: 0,
        opening_percentage_probe_2: 0,
        closing_fuel: tracking.maxFuel,
        closing_fuel_probe_1: 0,
        closing_fuel_probe_2: 0,
        closing_percentage: 0,
        closing_percentage_probe_1: 0,
        closing_percentage_probe_2: 0,
        total_fill: diff,
        total_usage: 0,
        fill_events: 1,
        fill_amount_during_session: diff,
        session_status: 'FUEL_FILL_COMPLETED',
        notes: `Geozone fill at "${tracking.zoneName}". Min: ${tracking.minFuel}L (${tracking.minTime}), Max: ${tracking.maxFuel}L (${tracking.maxTime}), Filled: ${diff.toFixed(1)}L | zone_id: ${tracking.fuelStopId} | detection: geozone`,
      });

      await db.insertGeozoneEvent({
        plate,
        fuel_stop_id: tracking.fuelStopId,
        geozone_name: tracking.zoneName,
        event_type: 'FILL_DETECTED',
        loc_time: tracking.maxTime,
        latitude: 0,
        longitude: 0,
        fuel_before: tracking.minFuel,
        fuel_after: tracking.maxFuel,
        fill_amount: diff,
      });

      await insertFuelReviewAction(plate, 'fill', diff, tracking.minFuel, tracking.maxFuel, tracking.maxTime, `zone: ${tracking.zoneName} | min: ${tracking.minFuel}L | max: ${tracking.maxFuel}L | detection: geozone`);
    } catch (err) {
      console.error(`[geozone] Failed to record fill for ${plate}: ${err.message}`);
    }
  };

  const processTheftDetection = async (msg, decoded) => {
    const plate = msg.plate;
    const monitoring = theftMonitoring[plate];

    if (!isEngineOff(msg)) {
      if (monitoring) {
        delete theftMonitoring[plate];
        console.log(`[theft] CLEARED: ${plate} - engine turned on`);
      }
      return;
    }

    const latestReading = await db.getLatestFuelReading(plate);
    if (!latestReading) return;

    const currentFuel = combinedFuelFromRow(latestReading);
    const currentTime = latestReading.loc_time;

    if (currentFuel <= 0) return;

    if (!monitoring) {
      theftMonitoring[plate] = {
        lastFuel: currentFuel,
        lastTime: currentTime,
        baselineFuel: currentFuel,
        baselineTime: currentTime,
        lowestFuel: currentFuel,
        lowestTime: currentTime,
        consecutiveNoDecrease: 0,
        trackingActive: false,
      };
      return;
    }

    const timeDiff = locTimeDiffMinutes(monitoring.lastTime, currentTime);
    const fuelDiff = monitoring.lastFuel - currentFuel;

    if (!monitoring.trackingActive) {
      if (timeDiff > 2 && fuelDiff >= 10) {
        monitoring.trackingActive = true;
        monitoring.baselineFuel = monitoring.lastFuel;
        monitoring.baselineTime = monitoring.lastTime;
        monitoring.lowestFuel = currentFuel;
        monitoring.lowestTime = currentTime;
        monitoring.consecutiveNoDecrease = 0;
        console.log(`[theft] THEFT DETECTED: ${plate} - ${monitoring.baselineFuel}L -> ${currentFuel}L = -${fuelDiff.toFixed(1)}L in ${timeDiff.toFixed(1)} min`);
      } else {
        monitoring.lastFuel = currentFuel;
        monitoring.lastTime = currentTime;
      }
      return;
    }

    if (currentFuel < monitoring.lowestFuel) {
      monitoring.lowestFuel = currentFuel;
      monitoring.lowestTime = currentTime;
      monitoring.consecutiveNoDecrease = 0;
    } else {
      monitoring.consecutiveNoDecrease++;
    }

    if (monitoring.consecutiveNoDecrease >= 3) {
      await completeTheft(plate, monitoring);
    }
  };

  const completeTheft = async (plate, monitoring) => {
    const theftAmount = monitoring.baselineFuel - monitoring.lowestFuel;

    if (theftAmount < 1) {
      console.log(`[theft] SKIP: ${plate} - change too small (${theftAmount.toFixed(1)}L)`);
      delete theftMonitoring[plate];
      return;
    }

    const sessionDate = monitoring.baselineTime ? monitoring.baselineTime.split('T')[0] : new Date().toISOString().split('T')[0];
    const startTime = monitoring.baselineTime ? new Date(monitoring.baselineTime).toISOString() : new Date().toISOString();
    const endTime = monitoring.lowestTime ? new Date(monitoring.lowestTime).toISOString() : new Date().toISOString();

    console.log(`[theft] THEFT RECORDED: ${plate} - ${monitoring.baselineFuel}L -> ${monitoring.lowestFuel}L = -${theftAmount.toFixed(1)}L`);

    try {
      await db.insertTheftSession({
        branch: plate,
        company: 'WATERFORD',
        cost_code: db.getCostCode(plate),
        session_date: sessionDate,
        session_start_time: startTime,
        session_end_time: endTime,
        operating_hours: 0,
        opening_fuel: monitoring.baselineFuel,
        opening_fuel_probe_1: 0,
        opening_fuel_probe_2: 0,
        opening_percentage: 0,
        opening_percentage_probe_1: 0,
        opening_percentage_probe_2: 0,
        closing_fuel: monitoring.lowestFuel,
        closing_fuel_probe_1: 0,
        closing_fuel_probe_2: 0,
        closing_percentage: 0,
        closing_percentage_probe_1: 0,
        closing_percentage_probe_2: 0,
        total_theft: theftAmount,
        total_usage: 0,
        session_status: 'FUEL_THEFT_COMPLETED',
        notes: `Theft detected. Baseline: ${monitoring.baselineFuel}L (${monitoring.baselineTime}), Lowest: ${monitoring.lowestFuel}L (${monitoring.lowestTime}), Lost: ${theftAmount.toFixed(1)}L | detection: engine-off monitoring`,
      });

      await insertFuelReviewAction(plate, 'theft', theftAmount, monitoring.baselineFuel, monitoring.lowestFuel, monitoring.lowestTime, `baseline: ${monitoring.baselineFuel}L | lowest: ${monitoring.lowestFuel}L | detection: engine-off monitoring`);
    } catch (err) {
      console.error(`[theft] Failed to record theft for ${plate}: ${err.message}`);
    }

    delete theftMonitoring[plate];
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
    for (const plate of Object.keys(geozoneTracking)) {
      if (geozoneTracking[plate].retryTimer) {
        clearTimeout(geozoneTracking[plate].retryTimer);
      }
    }
    if (ws) {
      ws.close();
      ws = null;
    }
  };

  return { connect, close };
};

module.exports = { createClient };
