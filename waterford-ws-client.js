const WebSocket = require('ws');
const { decodeFuelData, hasFuelData } = require('./waterford-fuel-decoder');
const db = require('./waterford-db');
const { supabase } = require('./supabase-client');

const createClient = (wsUrl) => {
  let ws = null;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let messageCount = 0;
  let rawCount = 0;

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

  const logStatusEvents = async (msg, decoded) => {
    const status = (msg.status || '').toUpperCase();
    if (!status) return;

    const plate = msg.plate;
    const time = msg.loc_time;
    const currentFuel1 = decoded?.tank1?.volume ?? null;
    const currentPct1 = decoded?.tank1?.percentage ?? null;
    const currentFuel2 = decoded?.tank2?.volume ?? null;
    const currentPct2 = decoded?.tank2?.percentage ?? null;

    if (status.includes('ENGINE ON') || status.includes('IGNITION ON')) {
      console.log(`[event] ENGINE ON: ${plate} at ${time}`);

      const fuel = await db.getLowestFuelBefore(plate, time);
      const sessionDate = time.split(' ')[0];

      // Use current message fuel if available, otherwise fall back to query
      const openingFuel1 = currentFuel1 ?? fuel?.fuel_probe_1_volume_in_tank ?? null;
      const openingPct1 = currentPct1 ?? fuel?.fuel_probe_1_level_percentage ?? null;
      const openingFuel2 = currentFuel2 ?? fuel?.fuel_probe_2_volume_in_tank ?? null;
      const openingPct2 = currentPct2 ?? fuel?.fuel_probe_2_level_percentage ?? null;

      const { data, error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert({
          branch: plate,
          company: 'KFC',
          cost_code: db.getCostCode(plate),
          session_date: sessionDate,
          session_start_time: time,
          session_status: 'ONGOING',
          opening_fuel_probe_1: openingFuel1,
          opening_percentage_probe_1: openingPct1,
          opening_fuel_probe_2: openingFuel2,
          opening_percentage_probe_2: openingPct2
        })
        .select('id')
        .single();

      if (error) {
        console.error(`[session] INSERT ERROR: ${plate}`, error.message);
      } else {
        console.log(`[session] OPEN: ${plate} session #${data.id} opening_fuel_1=${openingFuel1 ?? 'N/A'}`);
      }

      // Fuel fill detection: check if a fill happened between last engine off and this engine on
      try {
        const lastEngineOff = await db.getLastEngineEventBefore(plate, time);
        if (lastEngineOff) {
          const fillEvent = await db.getFuelFillBetween(plate, lastEngineOff.loc_time, time);
          if (fillEvent) {
            // Get the actual fuel level at engine off time (Engine Off messages have no fuel data)
            const fuelAtOff = await db.getLastFuelBefore(plate, lastEngineOff.loc_time);
            const fuelBefore = fuelAtOff?.fuel_probe_1_volume_in_tank ?? null;
            const fuelAfter = openingFuel1;
            const pctBefore = fuelAtOff?.fuel_probe_1_level_percentage ?? null;
            const pctAfter = openingPct1;

            if (fuelBefore != null && fuelAfter != null && fuelAfter > fuelBefore) {
              const fillAmount = fuelAfter - fuelBefore;
              const fillPct = (pctAfter != null && pctBefore != null) ? pctAfter - pctBefore : null;

              console.log(`[fill] DETECTED: ${plate} fill=${fillAmount.toFixed(1)}L (${fuelBefore}→${fuelAfter})`);

              const { error: fillError } = await supabase
                .from('energy_rite_fuel_fills')
                .insert({
                  plate,
                  fill_date: sessionDate,
                  fuel_before: fuelBefore,
                  fuel_after: fuelAfter,
                  fill_amount: fillAmount,
                  fill_percentage: fillPct,
                  detection_method: 'ENGINE_ON_COMPARISON',
                  status: 'COMPLETED',
                  fill_data: JSON.stringify({
                    engine_off_time: lastEngineOff.loc_time,
                    engine_on_time: time,
                    fill_event_time: fillEvent.loc_time,
                    engine_off_fuel: fuelBefore,
                    engine_on_fuel: fuelAfter
                  })
                });

              if (fillError) {
                console.error(`[fill] INSERT ERROR: ${plate}`, fillError.message);
              } else {
                console.log(`[fill] INSERTED: ${plate} ${fillAmount.toFixed(1)}L`);
              }

              if (data?.id) {
                const currentCount = data.fill_events || 0;
                const { error: updateErr } = await supabase
                  .from('energy_rite_operating_sessions')
                  .update({
                    fill_events: currentCount + 1,
                    fill_amount_during_session: (data.fill_amount_during_session || 0) + fillAmount
                  })
                  .eq('id', data.id);

                if (updateErr) {
                  console.error(`[fill] SESSION UPDATE ERROR: ${plate}`, updateErr.message);
                }
              }
            }
          }
        }
      } catch (err) {
        console.error(`[fill] Error: ${err.message}`);
      }

    } else if (status.includes('ENGINE OFF') || status.includes('IGNITION OFF')) {
      console.log(`[event] ENGINE OFF: ${plate} at ${time}`);

      const { data: openSession } = await supabase
        .from('energy_rite_operating_sessions')
        .select('id, session_start_time, opening_fuel_probe_1, opening_fuel_probe_2')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .order('created_at', { ascending: false })
        .limit(1)
        .single();

      if (!openSession) {
        console.log(`[session] NO OPEN SESSION: ${plate}`);
        return;
      }

      // Engine Off messages have no fuel data — always use last known fuel reading
      const lastFuel = await db.getLastFuelBefore(plate, time);
      const closingFuel1 = lastFuel?.fuel_probe_1_volume_in_tank ?? null;
      const closingPct1 = lastFuel?.fuel_probe_1_level_percentage ?? null;
      const closingFuel2 = lastFuel?.fuel_probe_2_volume_in_tank ?? null;
      const closingPct2 = lastFuel?.fuel_probe_2_level_percentage ?? null;

      const startTime = new Date(openSession.session_start_time);
      const endTime = new Date(time);
      const operatingHours = Math.max(0, (endTime - startTime) / (1000 * 60 * 60));

      const totalUsage = (openSession.opening_fuel_probe_1 != null && closingFuel1 != null)
        ? openSession.opening_fuel_probe_1 - closingFuel1
        : null;

      const literUsagePerHour = (totalUsage != null && operatingHours > 0)
        ? totalUsage / operatingHours
        : null;

      const costPerLiter = 20.00;
      const costForUsage = (totalUsage != null) ? totalUsage * costPerLiter : null;

      const durationH = operatingHours.toFixed(2);
      const openingL = openSession.opening_fuel_probe_1 ?? 'N/A';
      const closingL = closingFuel1 ?? 'N/A';
      const usedL = totalUsage != null ? totalUsage.toFixed(1) : 'N/A';
      const notes = `Engine stopped. Duration: ${durationH}h, Opening: ${openingL}L, Closing: ${closingL}L, Used: ${usedL}L`;

      const { error } = await supabase
        .from('energy_rite_operating_sessions')
        .update({
          session_end_time: time,
          session_status: 'COMPLETED',
          closing_fuel_probe_1: closingFuel1,
          closing_percentage_probe_1: closingPct1,
          closing_fuel_probe_2: closingFuel2,
          closing_percentage_probe_2: closingPct2,
          operating_hours: operatingHours,
          total_usage: totalUsage,
          liter_usage_per_hour: literUsagePerHour,
          cost_per_liter: costPerLiter,
          cost_for_usage: costForUsage,
          notes
        })
        .eq('id', openSession.id);

      if (error) {
        console.error(`[session] UPDATE ERROR: ${plate}`, error.message);
      } else {
        console.log(`[session] CLOSE: ${plate} session #${openSession.id} used=${usedL}L hours=${durationH}h`);
      }

    } else if (status.includes('POSSIBLE FUEL FILL')) {
      console.log(`[event] FUEL FILL: ${plate} at ${time}`);
    } else if (status.includes('POSSIBLE FUEL THEFT')) {
      console.log(`[event] FUEL THEFT: ${plate} at ${time}`);
    }
  };

  const handleMessage = async (raw) => {
    rawCount++;
    if (rawCount <= 5) console.log(`[ws] RAW: ${raw.substring(0, 200)}`);

    const msg = parseMessage(raw);
    if (!msg) { if (rawCount <= 5) console.log(`[ws] PARSE FAILED`); return; }
    if (!db.isKnownVehicle(msg.plate)) { if (rawCount <= 5) console.log(`[ws] UNKNOWN: ${msg.plate}`); return; }

    const decoded = msg.fuelDataRaw ? decodeFuelData(msg.fuelDataRaw) : null;
    const messageType = decoded?.messageType;

    messageCount++;
    if (messageCount % 100 === 0) {
      console.log(`[ws] ${messageCount} messages processed (latest: ${msg.plate})`);
    }

    const row = buildRow(msg, decoded);

    try {
      await db.insertHistory(row);
    } catch (err) {
      console.error(`[db] INSERT HISTORY ERROR: ${msg.plate}`, err.message);
    }

    try {
      await logStatusEvents(msg, decoded);
    } catch (err) {
      console.error(`[session] Error: ${err.message}`);
    }

    if (messageType === 405 && hasFuelData(decoded)) {
      try {
        await db.upsertLatest(row);
      } catch (err) {
        console.error(`[db] UPSERT LATEST ERROR: ${msg.plate}`, err.message);
      }
    }
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
