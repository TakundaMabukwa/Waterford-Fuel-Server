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

  const checkFuelFillDuringOperation = async (msg, decoded, msgType) => {
    if (msgType !== 405 || !hasFuelData(decoded)) return;

    const plate = msg.plate;
    const time = msg.loc_time;
    const currentFuel1 = decoded?.tank1?.volume ?? null;

    if (currentFuel1 == null || currentFuel1 <= 0) return;

    try {
      const { data: openSession } = await supabase
        .from('energy_rite_operating_sessions')
        .select('id, session_start_time')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .order('created_at', { ascending: false })
        .limit(1)
        .single();

      if (!openSession) return;

      const timeDate = new Date(time.replace(' ', 'T') + 'Z');
      const twoMinAgo = new Date(timeDate.getTime() - 2 * 60 * 1000);
      const twoMinAgoStr = twoMinAgo.toISOString().replace('T', ' ').replace('Z', '').substring(0, 19);

      const readings = await db.getFuelReadingsBetween(plate, twoMinAgoStr, time);

      if (readings.length < 2) return;

      const earliest = readings[0];
      const latest = readings[readings.length - 1];

      const earliestFuel = earliest.fuel_probe_1_volume_in_tank;
      const latestFuel = latest.fuel_probe_1_volume_in_tank;

      if (earliestFuel == null || latestFuel == null) return;

      const fuelIncrease = latestFuel - earliestFuel;

      if (fuelIncrease >= 10) {
        const earliestTime = earliest.loc_time;
        const timeDiffMin = (new Date(time.replace(' ', 'T') + 'Z') - new Date(earliestTime.replace(' ', 'T') + 'Z')) / 60000;

        if (timeDiffMin <= 2) {
          console.log(`[fill] DETECTED DURING OPERATION: ${plate} +${fuelIncrease.toFixed(1)}L in ${timeDiffMin.toFixed(1)}min (${earliestFuel}→${latestFuel})`);

          const sessionDate = time.split(' ')[0];

          const { error: fillError } = await supabase
            .from('energy_rite_fuel_fills')
            .insert({
              plate,
              fill_date: sessionDate,
              fuel_before: earliestFuel,
              fuel_after: latestFuel,
              fill_amount: fuelIncrease,
              fill_percentage: null,
              detection_method: 'OPERATION_10L_2MIN',
              status: 'COMPLETED',
              fill_data: JSON.stringify({
                session_id: openSession.id,
                earliest_time: earliestTime,
                latest_time: time,
                earliest_fuel: earliestFuel,
                latest_fuel: latestFuel,
                time_diff_minutes: timeDiffMin
              })
            });

          if (fillError) {
            console.error(`[fill] INSERT ERROR: ${plate}`, fillError.message);
          } else {
            console.log(`[fill] INSERTED: ${plate} ${fuelIncrease.toFixed(1)}L`);

            const { error: updateErr } = await supabase
              .from('energy_rite_operating_sessions')
              .update({
                fill_events: (openSession.fill_events || 0) + 1,
                fill_amount_during_session: (openSession.fill_amount_during_session || 0) + fuelIncrease
              })
              .eq('id', openSession.id);

            if (updateErr) {
              console.error(`[fill] SESSION UPDATE ERROR: ${plate}`, updateErr.message);
            }
          }
        }
      }
    } catch (err) {
      console.error(`[fill] Error during operation check: ${err.message}`);
    }
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

      let openingFuel1 = currentFuel1;
      let openingPct1 = currentPct1;
      let openingFuel2 = currentFuel2;
      let openingPct2 = currentPct2;

      if (openingFuel1 == null) {
        const fallback = await db.getLastFuelReading(plate, time);
        if (fallback) {
          openingFuel1 = fallback.fuel_probe_1_volume_in_tank;
          openingPct1 = fallback.fuel_probe_1_level_percentage;
          openingFuel2 = fallback.fuel_probe_2_volume_in_tank;
          openingPct2 = fallback.fuel_probe_2_level_percentage;
        }
      }

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

      try {
        const prevSession = await db.getPreviousSessionClosing(plate, time);
        if (prevSession && prevSession.session_end_time && openingFuel1 != null) {
          const lowestBetween = await db.getLowestFuelBetween(plate, prevSession.session_end_time, time);
          const preFillFuel = lowestBetween?.fuel_probe_1_volume_in_tank ?? prevSession.closing_fuel_probe_1;

          if (preFillFuel != null) {
            const diff = openingFuel1 - preFillFuel;
            if (diff >= 10) {
              const fillAmount = diff;
              const pctDiff = (openingPct1 != null && lowestBetween?.fuel_probe_1_level_percentage != null)
                ? openingPct1 - lowestBetween.fuel_probe_1_level_percentage
                : (openingPct1 != null && prevSession.closing_percentage_probe_1 != null)
                  ? openingPct1 - prevSession.closing_percentage_probe_1 : null;

              console.log(`[fill] DETECTED: ${plate} fill=${fillAmount.toFixed(1)}L (${preFillFuel}→${openingFuel1})`);

              const { error: fillError } = await supabase
                .from('energy_rite_fuel_fills')
                .insert({
                  plate,
                  fill_date: sessionDate,
                  fuel_before: preFillFuel,
                  fuel_after: openingFuel1,
                  fill_amount: fillAmount,
                  fill_percentage: pctDiff,
                  detection_method: 'SESSION_COMPARISON',
                  status: 'COMPLETED',
                  fill_data: JSON.stringify({
                    previous_session_end: prevSession.session_end_time,
                    current_session_start: time,
                    lowest_between: lowestBetween?.fuel_probe_1_volume_in_tank ?? null,
                    previous_closing_fuel: prevSession.closing_fuel_probe_1,
                    current_opening_fuel: openingFuel1
                  })
                });

              if (fillError) {
                console.error(`[fill] INSERT ERROR: ${plate}`, fillError.message);
              } else {
                console.log(`[fill] INSERTED: ${plate} ${fillAmount.toFixed(1)}L`);
              }

              if (data?.id) {
                const { error: updateErr } = await supabase
                  .from('energy_rite_operating_sessions')
                  .update({
                    fill_events: 1,
                    fill_amount_during_session: fillAmount
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

      let closingFuel1 = currentFuel1;
      let closingPct1 = currentPct1;
      let closingFuel2 = currentFuel2;
      let closingPct2 = currentPct2;

      if (closingFuel1 == null) {
        const lastFuel = await db.getLastFuelReading(plate, time);
        closingFuel1 = lastFuel?.fuel_probe_1_volume_in_tank ?? null;
        closingPct1 = lastFuel?.fuel_probe_1_level_percentage ?? null;
        closingFuel2 = lastFuel?.fuel_probe_2_volume_in_tank ?? null;
        closingPct2 = lastFuel?.fuel_probe_2_level_percentage ?? null;
      }

      const startTime = new Date(openSession.session_start_time);
      const endTime = new Date(time);
      const operatingHours = Math.max(0, (endTime - startTime) / (1000 * 60 * 60));

      let usage1 = (openSession.opening_fuel_probe_1 != null && closingFuel1 != null)
        ? Math.max(0, openSession.opening_fuel_probe_1 - closingFuel1) : 0;
      let usage2 = (openSession.opening_fuel_probe_2 != null && closingFuel2 != null)
        ? Math.max(0, openSession.opening_fuel_probe_2 - closingFuel2) : 0;
      let totalUsage = usage1 + usage2;

      if (totalUsage === 0 && openSession.opening_fuel_probe_1 != null && closingFuel1 != null) {
        console.log(`[session] ZERO USAGE: ${plate} opening=${openSession.opening_fuel_probe_1} closing=${closingFuel1} — fuel values present but no difference`);
      } else if (totalUsage === 0) {
        console.log(`[session] NO FUEL DATA: ${plate} opening=${openSession.opening_fuel_probe_1 ?? 'null'} closing=${closingFuel1 ?? 'null'}`);
      }

      const literUsagePerHour = (operatingHours > 0) ? totalUsage / operatingHours : null;

      const costPerLiter = 20.00;
      const costForUsage = totalUsage * costPerLiter;

      const durationH = operatingHours.toFixed(2);
      const opening1 = openSession.opening_fuel_probe_1 ?? 'N/A';
      const closing1 = closingFuel1 ?? 'N/A';
      const usedL = totalUsage.toFixed(1);
      const notes = `Engine stopped. Duration: ${durationH}h, Opening1: ${opening1}L, Closing1: ${closing1}L, Used: ${usedL}L (probe1: ${usage1.toFixed(1)}L, probe2: ${usage2.toFixed(1)}L)`;

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
        console.log(`[session] CLOSE: ${plate} session #${openSession.id} used=${usedL}L probe1=${usage1.toFixed(1)}L probe2=${usage2.toFixed(1)}L hours=${durationH}h`);
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

    try {
      await checkFuelFillDuringOperation(msg, decoded, messageType);
    } catch (err) {
      console.error(`[fill-check] Error: ${err.message}`);
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
