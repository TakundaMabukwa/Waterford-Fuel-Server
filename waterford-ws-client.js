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
  const fillTracking = {};
  const pendingSessionClose = {};

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

    if (status.includes('ENGINE ON') || status.includes('IGNITION ON')) {
      console.log(`[event] ENGINE ON: ${plate} at ${time}`);

      if (pendingSessionClose[plate]) {
        const pending = pendingSessionClose[plate];
        const lastFuel = await db.getLastFuelReading(plate, time);
        const closingFuel1 = lastFuel?.fuel_probe_1_volume_in_tank ?? null;
        const closingPct1 = lastFuel?.fuel_probe_1_level_percentage ?? null;
        const closingFuel2 = lastFuel?.fuel_probe_2_volume_in_tank ?? null;
        const closingPct2 = lastFuel?.fuel_probe_2_level_percentage ?? null;

        if (closingFuel1 != null) {
          const startTime = new Date(pending.sessionStartTime);
          const endTime = new Date(pending.engineOffTime);
          const operatingHours = Math.max(0, (endTime - startTime) / (1000 * 60 * 60));
          let usage1 = (pending.openingFuel1 != null) ? Math.max(0, pending.openingFuel1 - closingFuel1) : 0;
          let usage2 = (pending.openingFuel2 != null) ? Math.max(0, pending.openingFuel2 - closingFuel2) : 0;
          let totalUsage = usage1 + usage2;
          const literUsagePerHour = (operatingHours > 0) ? totalUsage / operatingHours : null;
          const costPerLiter = 20.00;
          const durationH = operatingHours.toFixed(2);
          const usedL = totalUsage.toFixed(1);
          const notes = `Engine stopped. Duration: ${durationH}h, Opening1: ${pending.openingFuel1 ?? 'N/A'}L, Closing1: ${closingFuel1}L, Used: ${usedL}L (probe1: ${usage1.toFixed(1)}L, probe2: ${usage2.toFixed(1)}L)`;

          const { error } = await supabase
            .from('energy_rite_operating_sessions')
            .update({
              session_end_time: pending.engineOffTime,
              session_status: 'COMPLETED',
              closing_fuel_probe_1: closingFuel1,
              closing_percentage_probe_1: closingPct1,
              closing_fuel_probe_2: closingFuel2,
              closing_percentage_probe_2: closingPct2,
              operating_hours: operatingHours,
              total_usage: totalUsage,
              liter_usage_per_hour: literUsagePerHour,
              cost_per_liter: costPerLiter,
              cost_for_usage: totalUsage * costPerLiter,
              notes
            })
            .eq('id', pending.sessionId);

          if (error) {
            console.error(`[session] CLOSE ERROR: ${plate}`, error.message);
          } else {
            console.log(`[session] CLOSE (before new ON): ${plate} session #${pending.sessionId} used=${usedL}L`);
          }
        }
        delete pendingSessionClose[plate];
      }

      const lastFuel = await db.getLastFuelReading(plate, time);
      let openingFuel1 = lastFuel?.fuel_probe_1_volume_in_tank ?? null;
      let openingPct1 = lastFuel?.fuel_probe_1_level_percentage ?? null;
      let openingFuel2 = lastFuel?.fuel_probe_2_volume_in_tank ?? null;
      let openingPct2 = lastFuel?.fuel_probe_2_level_percentage ?? null;

      const sessionDate = time.split(' ')[0];

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
        console.log(`[session] OPEN: ${plate} session #${data.id} opening_fuel_1=${openingFuel1 ?? 'N/A'} opening_fuel_2=${openingFuel2 ?? 'N/A'}`);
      }

    } else if (status.includes('ENGINE OFF') || status.includes('IGNITION OFF')) {
      console.log(`[event] ENGINE OFF: ${plate} at ${time}`);
      delete fillTracking[plate];

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

      pendingSessionClose[plate] = {
        sessionId: openSession.id,
        sessionStartTime: openSession.session_start_time,
        openingFuel1: openSession.opening_fuel_probe_1,
        openingFuel2: openSession.opening_fuel_probe_2,
        engineOffTime: time
      };
      console.log(`[session] PENDING CLOSE: ${plate} session #${openSession.id} — waiting for next fuel reading`);

    } else if (status.includes('POSSIBLE FUEL FILL')) {
      if (!fillTracking[plate]) {
        const fuel1 = decoded?.tank1?.volume ?? null;
        const fuel2 = decoded?.tank2?.volume ?? null;
        if (fuel1 == null || fuel2 == null) return;

        const engineOff = await db.getLastEngineOffBefore(plate, time);
        if (!engineOff) {
          console.log(`[fill] POSSIBLE FUEL FILL but no ENGINE OFF found for ${plate}`);
          return;
        }

        const alreadyRecorded = await db.checkFillRecorded(plate, engineOff.loc_time);
        if (alreadyRecorded) {
          console.log(`[fill] Fill already recorded for ${plate} after engine off at ${engineOff.loc_time}`);
          return;
        }

        const lowest = await db.getLowestFuelBetweenTimes(plate, engineOff.loc_time, time);
        if (!lowest) {
          console.log(`[fill] No fuel readings found between ENGINE OFF and fill trigger for ${plate}`);
          return;
        }

        fillTracking[plate] = {
          triggerTime: time,
          preFuel1: lowest.fuel_probe_1_volume_in_tank,
          preFuel2: lowest.fuel_probe_2_volume_in_tank,
          maxFuel1: fuel1,
          maxFuel2: fuel2,
          lastFuel1: fuel1,
          lastFuel2: fuel2,
          noIncreaseCount: 0,
          engineOffTime: engineOff.loc_time
        };
        console.log(`[fill] FILL STARTED: ${plate} baseline=${lowest.fuel_probe_1_volume_in_tank}/${lowest.fuel_probe_2_volume_in_tank} current=${fuel1}/${fuel2}`);
      }
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

    if (pendingSessionClose[msg.plate] && messageType === 405 && hasFuelData(decoded)) {
      try {
        const pending = pendingSessionClose[msg.plate];
        const lastFuel = await db.getLastFuelReading(msg.plate, msg.loc_time);
        const closingFuel1 = lastFuel?.fuel_probe_1_volume_in_tank ?? null;
        const closingPct1 = lastFuel?.fuel_probe_1_level_percentage ?? null;
        const closingFuel2 = lastFuel?.fuel_probe_2_volume_in_tank ?? null;
        const closingPct2 = lastFuel?.fuel_probe_2_level_percentage ?? null;

        if (closingFuel1 != null) {
          const startTime = new Date(pending.sessionStartTime);
          const endTime = new Date(pending.engineOffTime);
          const operatingHours = Math.max(0, (endTime - startTime) / (1000 * 60 * 60));

          let usage1 = (pending.openingFuel1 != null && closingFuel1 != null)
            ? Math.max(0, pending.openingFuel1 - closingFuel1) : 0;
          let usage2 = (pending.openingFuel2 != null && closingFuel2 != null)
            ? Math.max(0, pending.openingFuel2 - closingFuel2) : 0;
          let totalUsage = usage1 + usage2;

          const literUsagePerHour = (operatingHours > 0) ? totalUsage / operatingHours : null;
          const costPerLiter = 20.00;
          const costForUsage = totalUsage * costPerLiter;
          const durationH = operatingHours.toFixed(2);
          const opening1 = pending.openingFuel1 ?? 'N/A';
          const closing1 = closingFuel1 ?? 'N/A';
          const usedL = totalUsage.toFixed(1);
          const notes = `Engine stopped. Duration: ${durationH}h, Opening1: ${opening1}L, Closing1: ${closing1}L, Used: ${usedL}L (probe1: ${usage1.toFixed(1)}L, probe2: ${usage2.toFixed(1)}L)`;

          const { error } = await supabase
            .from('energy_rite_operating_sessions')
            .update({
              session_end_time: pending.engineOffTime,
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
            .eq('id', pending.sessionId);

          if (error) {
            console.error(`[session] UPDATE ERROR: ${msg.plate}`, error.message);
          } else {
            console.log(`[session] CLOSE: ${msg.plate} session #${pending.sessionId} used=${usedL}L probe1=${usage1.toFixed(1)}L probe2=${usage2.toFixed(1)}L hours=${durationH}h`);
          }
        } else {
          console.log(`[session] NO FUEL IN READING: ${msg.plate} — cannot close session #${pending.sessionId}`);
        }

        delete pendingSessionClose[msg.plate];
      } catch (err) {
        console.error(`[session] Close error: ${err.message}`);
        delete pendingSessionClose[msg.plate];
      }
    }

    if (fillTracking[msg.plate] && messageType === 405 && hasFuelData(decoded)) {
      try {
        const ft = fillTracking[msg.plate];
        const fuel1 = decoded?.tank1?.volume ?? null;
        const fuel2 = decoded?.tank2?.volume ?? null;

        if (fuel1 != null && fuel2 != null) {
          if (fuel1 > ft.maxFuel1) ft.maxFuel1 = fuel1;
          if (fuel2 > ft.maxFuel2) ft.maxFuel2 = fuel2;

          if (fuel1 <= ft.lastFuel1 && fuel2 <= ft.lastFuel2) {
            ft.noIncreaseCount++;
          } else {
            ft.noIncreaseCount = 0;
          }

          ft.lastFuel1 = fuel1;
          ft.lastFuel2 = fuel2;

          if (ft.noIncreaseCount >= 3) {
            const fillAmount1 = Math.max(0, ft.maxFuel1 - ft.preFuel1);
            const fillAmount2 = Math.max(0, ft.maxFuel2 - ft.preFuel2);
            const totalFill = fillAmount1 + fillAmount2;

            if (totalFill > 0) {
              console.log(`[fill] STABILIZED: ${msg.plate} probe1=${ft.preFuel1}→${ft.maxFuel1} (${fillAmount1.toFixed(1)}L) probe2=${ft.preFuel2}→${ft.maxFuel2} (${fillAmount2.toFixed(1)}L) total=${totalFill.toFixed(1)}L`);

              const sessionDate = msg.loc_time.split(' ')[0];

              const { error: fillError } = await supabase
                .from('energy_rite_fuel_fills')
                .insert({
                  plate: msg.plate,
                  fill_date: sessionDate,
                  fuel_before: ft.preFuel1,
                  fuel_after: ft.maxFuel1,
                  fill_amount: totalFill,
                  fill_percentage: null,
                  detection_method: 'FIRMWARE_FILL',
                  status: 'COMPLETED',
                  fill_data: JSON.stringify({
                    engine_off_time: ft.engineOffTime,
                    trigger_time: ft.triggerTime,
                    stabilized_time: msg.loc_time,
                    pre_fuel_1: ft.preFuel1,
                    pre_fuel_2: ft.preFuel2,
                    max_fuel_1: ft.maxFuel1,
                    max_fuel_2: ft.maxFuel2,
                    fill_amount_1: fillAmount1,
                    fill_amount_2: fillAmount2,
                    total_fill: totalFill
                  })
                });

              if (fillError) {
                console.error(`[fill] INSERT ERROR: ${msg.plate}`, fillError.message);
              } else {
                console.log(`[fill] INSERTED: ${msg.plate} ${totalFill.toFixed(1)}L`);
              }

              try {
                const { data: openSession } = await supabase
                  .from('energy_rite_operating_sessions')
                  .select('id, fill_events, fill_amount_during_session')
                  .eq('branch', msg.plate)
                  .eq('session_status', 'ONGOING')
                  .order('created_at', { ascending: false })
                  .limit(1)
                  .single();

                if (openSession) {
                  const { error: updateErr } = await supabase
                    .from('energy_rite_operating_sessions')
                    .update({
                      fill_events: (openSession.fill_events || 0) + 1,
                      fill_amount_during_session: (openSession.fill_amount_during_session || 0) + totalFill
                    })
                    .eq('id', openSession.id);

                  if (updateErr) {
                    console.error(`[fill] SESSION UPDATE ERROR: ${msg.plate}`, updateErr.message);
                  }
                }
              } catch (err) {
                console.error(`[fill] Session update error: ${err.message}`);
              }
            }

            delete fillTracking[msg.plate];
          }
        }
      } catch (err) {
        console.error(`[fill-check] Error: ${err.message}`);
      }
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
