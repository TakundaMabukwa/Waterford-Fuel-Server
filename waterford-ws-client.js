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

    const rawLocTime = (parts[4] || '').trim();
    const locTime = rawLocTime.includes('+') || rawLocTime.endsWith('Z')
      ? rawLocTime
      : rawLocTime + '+00:00';

    return {
      plate,
      speed: parseFloat(parts[1]) || 0,
      latitude: parseFloat(parts[2]) || 0,
      longitude: parseFloat(parts[3]) || 0,
      loc_time: locTime,
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
      loc_time: msg.loc_time || '',
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
    let tracking = geozoneTracking[plate];
    const isInZone = fuelStop !== null;
    const msgFuel = getCombinedFuel(decoded);

    const wasInZone = tracking && tracking.inZone;

    if (!wasInZone && isInZone) {
      geozoneTracking[plate] = {
        inZone: true,
        fuelStopId: fuelStop.id,
        zoneName: fuelStop.name || fuelStop.geozone_name || 'Unknown',
        zoneEnterTime: msg.loc_time,
        preFill: null,
        preFillLocTime: null,
        postFill: null,
        postFillLocTime: null,
        exitLatitude: null,
        exitLongitude: null,
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

      await initPreFill(plate, msgFuel);
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

      tracking.exitLatitude = msg.latitude;
      tracking.exitLongitude = msg.longitude;

      if (msgFuel > 0) {
        tracking.postFill = msgFuel;
        tracking.postFillLocTime = msg.loc_time;
      } else {
        const row = await db.getLatestFuelBefore(plate, msg.loc_time);
        if (row) {
          tracking.postFill = combinedFuelFromRow(row);
          tracking.postFillLocTime = row.loc_time;
        }
      }
      await finalizeFill(plate, tracking);
      return;
    }

    if (!wasInZone || !isInZone) return;

    if (tracking.preFill === null) {
      await initPreFill(plate, msgFuel);
    }
  };

  const initPreFill = async (plate, msgFuel) => {
    const tracking = geozoneTracking[plate];
    if (!tracking) return;

    if (msgFuel > 0) {
      tracking.preFill = msgFuel;
      tracking.preFillLocTime = tracking.zoneEnterTime;
      console.log(`[geozone] PRE-FILL SET: ${plate} - ${msgFuel}L at entry`);
      return;
    }

    const row = await db.getLatestFuelBefore(plate, tracking.zoneEnterTime);
    if (row) {
      const f = combinedFuelFromRow(row);
      tracking.preFill = f;
      tracking.preFillLocTime = row.loc_time;
      console.log(`[geozone] PRE-FILL SET: ${plate} - ${f}L (from ${row.loc_time})`);
    } else {
      console.log(`[geozone] NO PRE-FILL DATA: ${plate} - will discard at exit`);
    }
  };

  const finalizeFill = async (plate, tracking) => {
    if (tracking.preFill === null || tracking.preFill === undefined ||
        tracking.postFill === null || tracking.postFill === undefined) {
      console.log(`[geozone] INCOMPLETE FILL DATA: ${plate} - pre: ${tracking.preFill}, post: ${tracking.postFill} (discarded)`);
      delete geozoneTracking[plate];
      return;
    }

    const fill = tracking.postFill - tracking.preFill;

    if (fill <= 0) {
      console.log(`[geozone] NO FILL: ${plate} - ${tracking.preFill}L -> ${tracking.postFill}L = ${fill.toFixed(1)}L (discarded)`);
      delete geozoneTracking[plate];
      return;
    }

    if (fill < 10) {
      console.log(`[geozone] BELOW THRESHOLD: ${plate} - ${tracking.preFill}L -> ${tracking.postFill}L = ${fill.toFixed(1)}L (discarded)`);
      delete geozoneTracking[plate];
      return;
    }

    await recordGeozoneFill(plate, tracking, fill);
    delete geozoneTracking[plate];
  };

  const recordGeozoneFill = async (plate, tracking, fill) => {
    const rawTime = tracking.zoneEnterTime || new Date().toISOString();
    const sessionDate = rawTime.split(/[T ]/)[0];
    const startTime = tracking.preFillLocTime ? new Date(tracking.preFillLocTime).toISOString() : new Date().toISOString();
    const endTime = tracking.postFillLocTime ? new Date(tracking.postFillLocTime).toISOString() : new Date().toISOString();

    console.log(`[geozone] FILL RECORDED: ${plate} at "${tracking.zoneName}" - ${tracking.preFill}L -> ${tracking.postFill}L = ${fill.toFixed(1)}L`);

    try {
      await db.insertFillSession({
        branch: plate,
        company: 'WATERFORD',
        cost_code: db.getCostCode(plate),
        session_date: sessionDate,
        session_start_time: startTime,
        session_end_time: endTime,
        operating_hours: 0,
        opening_fuel: tracking.preFill,
        opening_fuel_probe_1: 0,
        opening_fuel_probe_2: 0,
        opening_percentage: 0,
        opening_percentage_probe_1: 0,
        opening_percentage_probe_2: 0,
        closing_fuel: tracking.postFill,
        closing_fuel_probe_1: 0,
        closing_fuel_probe_2: 0,
        closing_percentage: 0,
        closing_percentage_probe_1: 0,
        closing_percentage_probe_2: 0,
        total_fill: fill,
        total_usage: 0,
        fill_events: 1,
        fill_amount_during_session: fill,
        session_status: 'FUEL_FILL_COMPLETED',
        notes: `Geozone fill at "${tracking.zoneName}". Pre: ${tracking.preFill}L (${tracking.preFillLocTime}), Post: ${tracking.postFill}L (${tracking.postFillLocTime}), Diff: ${fill.toFixed(1)}L | zone_id: ${tracking.fuelStopId} | detection: geozone-minmax`,
      });

      await db.insertGeozoneEvent({
        plate,
        fuel_stop_id: tracking.fuelStopId,
        geozone_name: tracking.zoneName,
        event_type: 'FILL_DETECTED',
        loc_time: tracking.postFillLocTime,
        latitude: tracking.exitLatitude ?? 0,
        longitude: tracking.exitLongitude ?? 0,
        fuel_before: tracking.preFill,
        fuel_after: tracking.postFill,
        fill_amount: fill,
      });

      await insertFuelReviewAction(plate, 'fill', fill, tracking.postFillLocTime, `zone: ${tracking.zoneName} | pre: ${tracking.preFill}L | post: ${tracking.postFill}L | detection: geozone-minmax`);
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

    const rawBaseTime = monitoring.baselineTime || new Date().toISOString();
    const sessionDate = rawBaseTime.split(/[T ]/)[0];
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

      await insertFuelReviewAction(plate, 'theft', theftAmount, monitoring.lowestTime, `baseline: ${monitoring.baselineFuel}L | lowest: ${monitoring.lowestFuel}L | detection: engine-off monitoring`);
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
    if (ws) {
      ws.close();
      ws = null;
    }
  };

  return { connect, close };
};

module.exports = { createClient };
