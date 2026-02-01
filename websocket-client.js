const WebSocket = require('ws');
const { supabase } = require('./supabase-client');
const { detectFuelTheft } = require('./helpers/fuel-theft-detector');
const { detectFuelFill } = require('./helpers/fuel-fill-detector');
const ImprovedOnOffDetection = require('./fix-onoff-detection');
const pendingFuelDb = require('./pending-fuel-db');

class EnergyRiteWebSocketClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.onOffDetector = new ImprovedOnOffDetection();
    this.testMode = wsUrl === 'dummy';
    this.pendingFuelUpdates = new Map();
    this.pendingClosures = new Map();
    this.recentFuelData = new Map();
    // pendingFuelFills, fuelFillWatchers, preFillWatchers now use SQLite via pendingFuelDb
    
    // Message queue sorter
    this.messageQueue = [];
    this.processingQueue = false;
    
    // Initialize SQLite database for pending fuel states
    this.dbInitialized = false;
    
    // Stabilization check interval (every 30 seconds)
    this.stabilizationCheckInterval = null;
  }
  
  async initDb() {
    if (!this.dbInitialized) {
      await pendingFuelDb.initDatabase();
      this.dbInitialized = true;
      
      // Restore expired watchers on startup and schedule their completion
      this.restoreWatcherTimeouts();
      
      // Start periodic check for stabilized fills (fuel stopped increasing)
      this.startStabilizationChecker();
    }
  }
  
  startStabilizationChecker() {
    // Check every 30 seconds for fills that have stabilized
    this.stabilizationCheckInterval = setInterval(() => {
      this.checkStabilizedFills();
    }, 30 * 1000);
  }
  
  checkStabilizedFills() {
    // Get watchers where fuel hasn't increased for 2 minutes
    const stabilizedWatchers = pendingFuelDb.getStabilizedFuelFillWatchers(2 * 60 * 1000);
    for (const watcher of stabilizedWatchers) {
      console.log(`🎯 FILL STABILIZED: ${watcher.plate} - Fuel hasn't increased for 2+ minutes, completing fill`);
      this.completeFuelFillWatcher(watcher.plate);
    }
    
    // Also cleanup old fuel history (keep last hour)
    pendingFuelDb.cleanupOldFuelHistory(60 * 60 * 1000);
  }
  
  restoreWatcherTimeouts() {
    // Check for any expired watchers that need completion (max timeout reached)
    const expiredWatchers = pendingFuelDb.getExpiredFuelFillWatchers();
    for (const watcher of expiredWatchers) {
      console.log(`⏰ Completing expired watcher for ${watcher.plate} on startup`);
      this.completeFuelFillWatcher(watcher.plate);
    }
    
    // Also check for stabilized watchers on startup
    const stabilizedWatchers = pendingFuelDb.getStabilizedFuelFillWatchers(2 * 60 * 1000);
    for (const watcher of stabilizedWatchers) {
      console.log(`🎯 Completing stabilized watcher for ${watcher.plate} on startup`);
      this.completeFuelFillWatcher(watcher.plate);
    }
  }

  // Convert LocTime and add 2 hours
  convertLocTime(locTime) {
    if (!locTime) return new Date().toISOString();
    
    // Parse the LocTime and add 2 hours
    const date = new Date(locTime.replace(' ', 'T') + 'Z');
    date.setHours(date.getHours() + 2);
    return date.toISOString();
  }

  connect() {
    if (this.testMode) {
      console.log('🧪 Test mode - skipping WebSocket connection');
      return;
    }
    
    // Initialize SQLite database first
    this.initDb().then(() => {
      console.log(`🔌 Connecting to WebSocket: ${this.wsUrl}`);
      this.ws = new WebSocket(this.wsUrl);

      this.ws.on('open', () => {
        console.log('✅ WebSocket connected');
        this.reconnectAttempts = 0;
      });

      this.ws.on('message', async (data) => {
        try {
          const message = JSON.parse(data);
          console.log('📨 RAW MESSAGE:', JSON.stringify(message, null, 2));
          
          if (message.Plate && message.LocTime) {
            this.addToQueue(message);
          }
        } catch (error) {
          console.error('❌ Error parsing message:', error);
        }
      });

      this.ws.on('close', () => {
        console.log('🔌 WebSocket disconnected');
        this.reconnect();
      });

      this.ws.on('error', (error) => {
        console.error('❌ WebSocket error:', error);
      });
    });
  }

  addToQueue(message) {
    // Pre-calculate timestamp for faster sorting
    message._sortTime = new Date(this.convertLocTime(message.LocTime)).getTime();
    this.messageQueue.push(message);
    
    if (!this.processingQueue) {
      setImmediate(() => this.processQueue());
    }
  }

  async processQueue() {
    if (this.processingQueue || this.messageQueue.length === 0) return;
    
    this.processingQueue = true;
    const batch = this.messageQueue.splice(0); // Faster than [...array]
    
    // Fast sort using pre-calculated timestamps
    if (batch.length > 1) {
      batch.sort((a, b) => a._sortTime - b._sortTime);
      console.log(`📦 Sorted ${batch.length} messages`);
    }
    
    // Process without awaiting each (parallel where safe)
    for (const message of batch) {
      console.log(`📋 [${message.Plate}] ${message.LocTime}`);
      await this.processVehicleUpdate(message);
    }
    
    this.processingQueue = false;
    
    if (this.messageQueue.length > 0) {
      setImmediate(() => this.processQueue());
    }
  }

  async processVehicleUpdate(vehicleData) {
    try {
      const actualBranch = vehicleData.Plate;
      console.log(`🔍 Processing ${actualBranch}:`, {
        DriverName: vehicleData.DriverName,
        fuel_probe_1_level: vehicleData.fuel_probe_1_level,
        fuel_probe_1_volume_in_tank: vehicleData.fuel_probe_1_volume_in_tank,
        Temperature: vehicleData.Temperature
      });
      
      // Parse fuel data from Temperature field if present
      const fuelData = this.parseFuelData(vehicleData);
      
      // Store fuel data if available
      if (fuelData.hasFuelData) {
        await this.storeFuelData({ ...vehicleData, actualBranch, ...fuelData });
        
        const fuelTimestamp = new Date(this.convertLocTime(vehicleData.LocTime)).getTime();
        
        // Store in recent fuel data cache with LocTime (in-memory)
        if (!this.recentFuelData.has(actualBranch)) {
          this.recentFuelData.set(actualBranch, []);
        }
        const fuelHistory = this.recentFuelData.get(actualBranch);
        fuelHistory.push({
          locTime: vehicleData.LocTime,
          timestamp: fuelTimestamp,
          ...fuelData
        });
        // Keep only last 20 entries in memory
        if (fuelHistory.length > 20) fuelHistory.shift();
        
        // Also store in SQLite for persistence across restarts
        // Store all fuel values including 0 (empty tank)
        if (fuelData.fuel_probe_1_volume_in_tank >= 0) {
          pendingFuelDb.storeFuelHistory(
            actualBranch,
            fuelData.fuel_probe_1_volume_in_tank,
            fuelData.fuel_probe_1_level_percentage,
            vehicleData.LocTime,
            fuelTimestamp
          );
        }
        
        // Update any pending sessions waiting for opening fuel data
        await this.updatePendingSessionFuel(actualBranch, vehicleData);
        
        // Update any pending closures waiting for closing fuel data
        await this.updatePendingClosure(actualBranch, vehicleData);
        
        // ❌ DISABLED: Passive fill detection - only use status-based detection
        // await this.detectPassiveFill(actualBranch, fuelData, vehicleData.LocTime);
      }
      
      // Handle engine status FIRST (before fuel fill processing)
      const engineStatus = this.parseEngineStatus(vehicleData.DriverName);
      if (engineStatus) {
        console.log(`🔧 Processing engine ${engineStatus} for ${actualBranch}`);
        await this.handleSessionChange(actualBranch, engineStatus, vehicleData);
      }
      
      // Then handle fuel fill status
      const hasFuelFillStatus = this.parseFuelFillStatus(vehicleData.DriverName);
      
      // ALWAYS track lowest fuel level (even during fill status)
      if (fuelData.hasFuelData) {
        this.trackPreFillLowest(actualBranch, fuelData, vehicleData.LocTime);
      }
      
      if (hasFuelFillStatus) {
        await this.handleFuelFillStart(actualBranch, vehicleData);
      } else {
        // Status disappeared - start watcher if we have pending fill (using SQLite)
        const pending = pendingFuelDb.getPendingFuelFill(actualBranch);
        if (pending && fuelData.hasFuelData) {
          if (pending.opening_fuel) {
            // Start watcher to track highest fuel until it stabilizes
            pendingFuelDb.setFuelFillWatcher(actualBranch, {
              startTime: pending.start_time,
              startLocTime: pending.start_loc_time,
              openingFuel: pending.opening_fuel,
              openingPercentage: pending.opening_percentage,
              highestFuel: fuelData.fuel_probe_1_volume_in_tank,
              highestPercentage: fuelData.fuel_probe_1_level_percentage,
              highestLocTime: vehicleData.LocTime
            });
            pendingFuelDb.deletePendingFuelFill(actualBranch);
            console.log(`🔍 Status ended for ${actualBranch} - Watching for highest fuel (until stabilizes)`);
            // Note: Completion happens when fuel stabilizes (2 min no increase) or max 10 min timeout
          } else if (pending.waitingForOpeningFuel) {
            // Got opening fuel when status disappeared, start watcher immediately
            pendingFuelDb.setFuelFillWatcher(actualBranch, {
              startTime: pending.start_time,
              startLocTime: pending.start_loc_time,
              openingFuel: fuelData.fuel_probe_1_volume_in_tank,
              openingPercentage: fuelData.fuel_probe_1_level_percentage,
              highestFuel: fuelData.fuel_probe_1_volume_in_tank,
              highestPercentage: fuelData.fuel_probe_1_level_percentage,
              highestLocTime: vehicleData.LocTime
            });
            pendingFuelDb.deletePendingFuelFill(actualBranch);
            console.log(`🔍 Status ended for ${actualBranch} - Got opening fuel, watching for highest (until stabilizes)`);
            // Note: Completion happens when fuel stabilizes (2 min no increase) or max 10 min timeout
          }
        }
        
        if (fuelData.hasFuelData) {
          await this.checkPendingFuelFill(actualBranch, fuelData, vehicleData.LocTime);
        }
      }

    } catch (error) {
      console.error('❌ Error processing vehicle update:', error);
    }
  }



  parseFuelData(vehicleData) {
    // Check if fuel data already parsed in message
    if (vehicleData.fuel_probe_1_level) {
      return {
        hasFuelData: true,
        fuel_probe_1_level: parseFloat(vehicleData.fuel_probe_1_level),
        fuel_probe_1_level_percentage: parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0,
        fuel_probe_1_volume_in_tank: parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0,
        fuel_probe_1_temperature: parseFloat(vehicleData.fuel_probe_1_temperature) || 0
      };
    }
    
    // Parse from Temperature field if present
    if (vehicleData.Temperature && vehicleData.Temperature.trim() !== '') {
      // Temperature format: "25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37"
      // This contains fuel data encoded
      return {
        hasFuelData: true,
        fuel_probe_1_level: parseFloat(vehicleData.fuel_probe_1_level) || 0,
        fuel_probe_1_level_percentage: parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0,
        fuel_probe_1_volume_in_tank: parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0,
        fuel_probe_1_temperature: parseFloat(vehicleData.fuel_probe_1_temperature) || 0
      };
    }
    
    return { hasFuelData: false };
  }

  trackPreFillLowest(plate, fuelData, locTime) {
    try {
      const existing = pendingFuelDb.getPreFillWatcher(plate);
      
      if (!existing) {
        pendingFuelDb.setPreFillWatcher(plate, {
          lowestFuel: fuelData.fuel_probe_1_volume_in_tank,
          lowestPercentage: fuelData.fuel_probe_1_level_percentage,
          lowestLocTime: locTime
        });
      } else {
        const currentFuel = fuelData.fuel_probe_1_volume_in_tank;
        
        // Update if lower
        if (currentFuel < existing.lowest_fuel) {
          pendingFuelDb.setPreFillWatcher(plate, {
            lowestFuel: currentFuel,
            lowestPercentage: fuelData.fuel_probe_1_level_percentage,
            lowestLocTime: locTime
          });
        } else {
          // Just update last_update timestamp
          pendingFuelDb.setPreFillWatcher(plate, {
            lowestFuel: existing.lowest_fuel,
            lowestPercentage: existing.lowest_percentage,
            lowestLocTime: existing.lowest_loc_time
          });
        }
      }
      
      // Clean up old watchers periodically
      pendingFuelDb.cleanupOldPreFillWatchers(30 * 60 * 1000);
    } catch (error) {
      console.error(`❌ Error tracking pre-fill lowest for ${plate}:`, error.message);
    }
  }

  /**
   * Passive fill detection - detects fills without "FUEL FILL" status message
   * Triggers when fuel increases by 10L+ within 2 minutes
   */
  async detectPassiveFill(plate, fuelData, locTime) {
    try {
      const currentFuel = fuelData.fuel_probe_1_volume_in_tank;
      
      // Check if we already have an active fill watcher - if so, just update highest fuel
      const existingWatcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (existingWatcher) {
        if (currentFuel > existingWatcher.highest_fuel) {
          pendingFuelDb.updateFuelFillWatcherHighest(plate, currentFuel, fuelData.fuel_probe_1_level_percentage, locTime);
          console.log(`📈 Fill watcher update: ${plate} highest fuel now ${currentFuel}L`);
        }
        return; // Already tracking this fill
      }
      
      // Skip if we have a pending fill (status-based detection in progress)
      const existingPending = pendingFuelDb.getPendingFuelFill(plate);
      if (existingPending) {
        return; // Don't interfere with status-based fill detection
      }
      
      const currentTime = new Date(this.convertLocTime(locTime)).getTime();
      const twoMinutesAgo = currentTime - (2 * 60 * 1000);
      
      // Find the lowest fuel reading within the last 2 minutes
      // First check in-memory cache
      let lowestRecentFuel = null;
      let lowestRecentTime = null;
      let lowestRecentLocTime = null;
      let lowestRecentPercentage = null;
      
      const fuelHistory = this.recentFuelData.get(plate);
      if (fuelHistory && fuelHistory.length > 0) {
        for (const entry of fuelHistory) {
          if (entry.timestamp >= twoMinutesAgo && entry.timestamp < currentTime) {
            if (lowestRecentFuel === null || entry.fuel_probe_1_volume_in_tank < lowestRecentFuel) {
              lowestRecentFuel = entry.fuel_probe_1_volume_in_tank;
              lowestRecentTime = entry.timestamp;
              lowestRecentLocTime = entry.locTime;
              lowestRecentPercentage = entry.fuel_probe_1_level_percentage;
            }
          }
        }
      }
      
      // Also check SQLite for any readings not in memory (after restart)
      const dbLowest = pendingFuelDb.getLowestFuelInRange(plate, twoMinutesAgo, currentTime);
      if (dbLowest && (lowestRecentFuel === null || dbLowest.fuel_volume < lowestRecentFuel)) {
        lowestRecentFuel = dbLowest.fuel_volume;
        lowestRecentTime = dbLowest.timestamp;
        lowestRecentLocTime = dbLowest.loc_time;
        lowestRecentPercentage = dbLowest.fuel_percentage;
      }
      
      if (lowestRecentFuel === null) return;
      
      // Check if fuel increased by 10L+ from the lowest reading
      const fuelIncrease = currentFuel - lowestRecentFuel;
      
      if (fuelIncrease >= 10) {
        console.log(`🔎 PASSIVE FILL DETECTED for ${plate}: +${fuelIncrease.toFixed(1)}L in ${((currentTime - lowestRecentTime) / 1000).toFixed(0)}s`);
        
        // Get the pre-fill lowest if available (might be even lower than 2-min window)
        const preFill = pendingFuelDb.getPreFillWatcher(plate);
        let openingFuel, openingPercentage, openingLocTime;
        
        if (preFill && preFill.lowest_fuel < lowestRecentFuel) {
          openingFuel = preFill.lowest_fuel;
          openingPercentage = preFill.lowest_percentage;
          openingLocTime = preFill.lowest_loc_time;
          console.log(`🔎 Using pre-fill lowest: ${openingFuel}L`);
        } else {
          openingFuel = lowestRecentFuel;
          openingPercentage = lowestRecentPercentage;
          openingLocTime = lowestRecentLocTime;
          console.log(`🔎 Using 2-min lowest: ${openingFuel}L`);
        }
        
        // Start a watcher to track highest fuel (fill might still be ongoing)
        const startTime = this.convertLocTime(openingLocTime);
        pendingFuelDb.setFuelFillWatcher(plate, {
          startTime: startTime,
          startLocTime: openingLocTime,
          openingFuel: openingFuel,
          openingPercentage: openingPercentage,
          highestFuel: currentFuel,
          highestPercentage: fuelData.fuel_probe_1_level_percentage,
          highestLocTime: locTime
        });
        
        // Clean up pre-fill watcher since we used it
        pendingFuelDb.deletePreFillWatcher(plate);
        
        console.log(`🔎 PASSIVE FILL WATCHER started for ${plate} - Opening: ${openingFuel}L, Current: ${currentFuel}L`);
        
        // Note: No fixed timeout - fill will complete when fuel stabilizes (stops increasing for 2 min)
        // The stabilization checker runs every 30 seconds
      }
    } catch (error) {
      console.error(`❌ Error in passive fill detection for ${plate}:`, error.message);
    }
  }

  async handleFuelFillStart(plate, vehicleData) {
    try {
      const currentTime = this.convertLocTime(vehicleData.LocTime);
      const hasFuelData = vehicleData.fuel_probe_1_volume_in_tank && parseFloat(vehicleData.fuel_probe_1_volume_in_tank) > 0;
      
      // Check if passive detection already started a watcher for this fill
      const existingWatcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (existingWatcher) {
        console.log(`ℹ️ FUEL FILL status for ${plate} - Watcher already active (passive detected earlier)`);
        return; // Let the existing watcher handle it
      }
      
      // Check if we already have a pending fill (status already received)
      const existingPending = pendingFuelDb.getPendingFuelFill(plate);
      if (existingPending) {
        console.log(`ℹ️ FUEL FILL status for ${plate} - Already tracking this fill`);
        return;
      }
      
      // Get fuel data - prioritize pre-fill watcher (tracks true lowest)
      let openingFuel, openingPercentage;
      const preFill = pendingFuelDb.getPreFillWatcher(plate);
      const closestFuel = this.findClosestFuelDataBefore(plate, vehicleData.LocTime);
      
      console.log(`🔍 FILL START DEBUG for ${plate}:`);
      console.log(`   Fill status LocTime: ${vehicleData.LocTime}`);
      console.log(`   Pre-fill lowest: ${preFill ? preFill.lowest_fuel + 'L' : 'NONE'}`);
      console.log(`   Closest fuel found: ${closestFuel ? closestFuel.fuel_probe_1_volume_in_tank + 'L' : 'NONE'}`);
      
      if (preFill && preFill.lowest_fuel > 0) {
        openingFuel = preFill.lowest_fuel;
        openingPercentage = preFill.lowest_percentage;
        console.log(`⛽ FUEL FILL START: ${plate} - Using pre-fill lowest: ${openingFuel}L (tracked since ${preFill.lowest_loc_time})`);
        // Delete watcher ONLY after successfully using it
        pendingFuelDb.deletePreFillWatcher(plate);
      } else if (closestFuel && closestFuel.fuel_probe_1_volume_in_tank > 0) {
        openingFuel = closestFuel.fuel_probe_1_volume_in_tank;
        openingPercentage = closestFuel.fuel_probe_1_level_percentage;
        console.log(`⛽ FUEL FILL START: ${plate} - Using closest fuel before status: ${openingFuel}L`);
      } else if (hasFuelData) {
        openingFuel = parseFloat(vehicleData.fuel_probe_1_volume_in_tank);
        openingPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
        console.log(`⛽ FUEL FILL START: ${plate} - Using current fuel: ${openingFuel}L`);
      }
      
      if (openingFuel && openingFuel > 0) {
        pendingFuelDb.setPendingFuelFill(plate, {
          startTime: currentTime,
          startLocTime: vehicleData.LocTime,
          openingFuel: openingFuel,
          openingPercentage: openingPercentage,
          waitingForOpeningFuel: false
        });
        
        console.log(`⛽ FUEL FILL START: ${plate} - Opening: ${openingFuel}L`);
      } else {
        pendingFuelDb.setPendingFuelFill(plate, {
          startTime: currentTime,
          startLocTime: vehicleData.LocTime,
          waitingForOpeningFuel: true
        });
        console.log(`⏳ FUEL FILL START: ${plate} - Waiting for opening fuel data`);
      }
    } catch (error) {
      console.error(`❌ Error starting fuel fill for ${plate}:`, error.message);
    }
  }

  async checkPendingFuelFill(plate, fuelData, locTime) {
    try {
      // Check if we have a watcher tracking highest fuel (SQLite)
      const watcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (watcher) {
        const currentFuel = fuelData.fuel_probe_1_volume_in_tank;
        
        if (currentFuel > watcher.highest_fuel) {
          pendingFuelDb.updateFuelFillWatcherHighest(plate, currentFuel, fuelData.fuel_probe_1_level_percentage, locTime);
          console.log(`📈 New highest fuel for ${plate}: ${currentFuel}L`);
        }
        return;
      }
      
      const pending = pendingFuelDb.getPendingFuelFill(plate);
      if (!pending) return;
      
      const currentTime = new Date(this.convertLocTime(locTime)).getTime();
      const startTime = new Date(pending.start_time).getTime();
      
      if (pending.waitingForOpeningFuel && currentTime > startTime) {
        pendingFuelDb.updatePendingFuelFillOpening(plate, fuelData.fuel_probe_1_volume_in_tank, fuelData.fuel_probe_1_level_percentage);
        console.log(`✅ Got opening fuel for fill: ${plate} - ${fuelData.fuel_probe_1_volume_in_tank}L`);
      }
    } catch (error) {
      console.error(`❌ Error checking fuel fill for ${plate}:`, error.message);
    }
  }

  async completeFuelFillWatcher(plate) {
    try {
      const watcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (!watcher) return;
      
      const fillAmount = Math.max(0, watcher.highest_fuel - watcher.opening_fuel);
      const endTime = watcher.highest_loc_time ? this.convertLocTime(watcher.highest_loc_time) : new Date().toISOString();
      const startTime = new Date(watcher.start_time).getTime();
      const endTimeMs = new Date(endTime).getTime();
      const duration = (endTimeMs - startTime) / 1000;
      
      await supabase.from('energy_rite_operating_sessions').insert({
        branch: plate,
        company: 'KFC',
        session_date: watcher.start_time.split('T')[0],
        session_start_time: watcher.start_time,
        session_end_time: endTime,
        operating_hours: duration / 3600,
        opening_fuel: watcher.opening_fuel,
        opening_percentage: watcher.opening_percentage,
        closing_fuel: watcher.highest_fuel,
        closing_percentage: watcher.highest_percentage,
        total_fill: fillAmount,
        session_status: 'FUEL_FILL_COMPLETED',
        notes: `Fuel fill completed. Duration: ${duration.toFixed(1)}s, Opening: ${watcher.opening_fuel}L, Highest: ${watcher.highest_fuel}L, Filled: ${fillAmount.toFixed(1)}L`
      });
      
      console.log(`⛽ FUEL FILL COMPLETE: ${plate} - ${watcher.opening_fuel}L → ${watcher.highest_fuel}L = +${fillAmount.toFixed(1)}L`);
      
      // Update engine session if active
      const { data: engineSessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .order('session_start_time', { ascending: false })
        .limit(1);
        
      if (engineSessions && engineSessions.length > 0) {
        const session = engineSessions[0];
        
        // Calculate usage BEFORE fill (opening_fuel - pre_fill_fuel)
        const usageBeforeFill = Math.max(0, session.opening_fuel - watcher.opening_fuel);
        
        console.log(`🔧 ENGINE SESSION UPDATE: ${plate}`);
        console.log(`   Usage before fill: ${session.opening_fuel}L - ${watcher.opening_fuel}L = ${usageBeforeFill.toFixed(1)}L`);
        console.log(`   Resetting opening fuel: ${session.opening_fuel}L → ${watcher.highest_fuel}L`);
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            // Add usage accumulated before fill
            total_usage: (session.total_usage || 0) + usageBeforeFill,
            // RESET opening fuel to post-fill level (like C# controller)
            opening_fuel: watcher.highest_fuel,
            opening_percentage: watcher.highest_percentage,
            // Track fill events
            fill_events: (session.fill_events || 0) + 1,
            fill_amount_during_session: (session.fill_amount_during_session || 0) + fillAmount,
            total_fill: (session.total_fill || 0) + fillAmount,
            notes: `${session.notes || ''} | Fill: +${fillAmount.toFixed(1)}L, Usage before fill: ${usageBeforeFill.toFixed(1)}L, Opening reset to ${watcher.highest_fuel}L`
          })
          .eq('id', session.id);
      }
      
      pendingFuelDb.deleteFuelFillWatcher(plate);
    } catch (error) {
      console.error(`❌ Error completing fuel fill watcher for ${plate}:`, error.message);
    }
  }

  reconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
      
      console.log(`🔄 Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
      
      setTimeout(() => {
        this.connect();
      }, delay);
    } else {
      console.error('❌ Max reconnection attempts reached');
    }
  }



  async storeFuelData(vehicleData) {
    try {
      const fuelLevel = parseFloat(vehicleData.fuel_probe_1_level);
      const fuelPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage);
      
      if (isNaN(fuelLevel) || fuelLevel <= 0) return;
      
      const timestamp = this.convertLocTime(vehicleData.LocTime);
      
      await supabase.from('energy_rite_fuel_data').insert({
        plate: vehicleData.actualBranch || vehicleData.Plate,
        fuel_probe_1_level: fuelLevel,
        fuel_probe_1_level_percentage: fuelPercentage,
        fuel_probe_1_volume_in_tank: parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0,
        created_at: timestamp
      });
      
      console.log(`⛽ Stored fuel data for ${vehicleData.actualBranch || vehicleData.Plate}: ${fuelLevel}L (${fuelPercentage}%)`);
      
    } catch (error) {
      console.error(`❌ Error storing fuel data for ${vehicleData.actualBranch || vehicleData.Plate}:`, error.message);
    }
  }

  async completeFuelFillSession(plate, vehicleData) {
    try {
      // Complete any ongoing fuel fill session
      const { data: sessions, error: sessionError } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('branch', plate)
        .eq('session_status', 'FUEL_FILL_ONGOING')
        .order('session_start_time', { ascending: false })
        .limit(1);
        
      if (!sessionError && sessions && sessions.length > 0) {
        const session = sessions[0];
        const currentTime = new Date(this.convertLocTime(vehicleData.LocTime));
        const startTime = new Date(session.session_start_time);
        const durationMs = currentTime.getTime() - startTime.getTime();
        const duration = durationMs / 1000;
        const startingFuel = session.opening_fuel || 0;
        
        // Get current fuel data from WebSocket only
        let currentFuel = 0;
        let currentPercentage = 0;
        
        if (vehicleData?.fuel_probe_1_volume_in_tank) {
          currentFuel = parseFloat(vehicleData.fuel_probe_1_volume_in_tank);
          currentPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
        } else {
          console.log(`⏳ No fuel data in WebSocket for ${plate}, waiting for next message`);
          return; // Don't complete session without fuel data
        }
        
        const fillAmount = Math.max(0, currentFuel - startingFuel);
        console.log(`⛽ FUEL FILL COMPLETE: ${plate} - ${startingFuel}L → ${currentFuel}L = +${fillAmount.toFixed(1)}L`);
        
        await supabase.from('energy_rite_operating_sessions')
          .update({
            session_end_time: this.convertLocTime(vehicleData.LocTime),
            operating_hours: durationMs / (1000 * 60 * 60),
            closing_fuel: currentFuel,
            closing_percentage: currentPercentage,
            total_fill: fillAmount,
            session_status: 'FUEL_FILL_COMPLETED',
            notes: `Fuel fill completed. Duration: ${(durationMs / 1000).toFixed(3)}s (${durationMs}ms), Opening: ${startingFuel}L, Closing: ${currentFuel}L, Filled: ${fillAmount.toFixed(1)}L`
          })
          .eq('id', session.id);
          
        console.log(`⛽ FUEL FILL COMPLETE: ${plate} - Duration: ${duration.toFixed(0)}s, Filled: ${fillAmount.toFixed(1)}L`);
        
        // Update any ongoing engine session
        if (fillAmount > 0) {
          const { data: engineSessions } = await supabase
            .from('energy_rite_operating_sessions')
            .select('*')
            .eq('branch', plate)
            .eq('session_status', 'ONGOING')
            .order('session_start_time', { ascending: false })
            .limit(1);
            
          if (engineSessions && Array.isArray(engineSessions) && engineSessions.length > 0) {
            const engineSession = engineSessions[0];
            await supabase
              .from('energy_rite_operating_sessions')
              .update({
                fill_events: (engineSession.fill_events || 0) + 1,
                fill_amount_during_session: (engineSession.fill_amount_during_session || 0) + fillAmount,
                total_fill: (engineSession.total_fill || 0) + fillAmount,
                notes: `${engineSession.notes || ''} | Fill: +${fillAmount.toFixed(1)}L`
              })
              .eq('id', engineSession.id);
          }
        }
      }
      
    } catch (error) {
      console.error(`❌ Error completing fuel fill session for ${plate}:`, error.message);
    }
  }

  parseEngineStatus(driverName) {
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    console.log(`🔍 Checking engine status: "${driverName}" -> "${normalized}"`);
    
    // Check for ENGINE and PTO status
    if (normalized.includes('ENGINE ON') || normalized.includes('PTO ON')) {
      console.log(`🟢 ENGINE ON DETECTED: ${driverName}`);
      return 'ON';
    }
    if (normalized.includes('ENGINE OFF') || normalized.includes('PTO OFF')) {
      console.log(`🔴 ENGINE OFF DETECTED: ${driverName}`);
      return 'OFF';
    }
    
    console.log(`ℹ️ No engine status detected in: "${driverName}"`);
    return null;
  }

  parseFuelFillStatus(driverName) {
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    console.log(`🔍 Checking fuel fill status: "${driverName}" -> "${normalized}"`);
    
    // Check for fuel fill patterns
    if (normalized.includes('POSSIBLE FUEL FILL') ||
        normalized.includes('FUEL FILL') ||
        normalized.includes('REFUEL') ||
        normalized.includes('FILLING')) {
      console.log(`⛽ FUEL FILL DETECTED: ${driverName}`);
      return 'START';
    }
    
    console.log(`ℹ️ No fuel fill status detected in: "${driverName}"`);
    return null;
  }

  async handleFuelFill(plate, fillResult) {
    try {
      console.log(`⛽ Handling fuel fill for ${plate}:`, fillResult.fillDetails);
      
      // Update any ongoing session with fill information
      const { data: ongoingSessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .order('session_start_time', { ascending: false })
        .limit(1);
        
      if (ongoingSessions && ongoingSessions.length > 0) {
        const session = ongoingSessions[0];
        const currentFillEvents = session.fill_events || 0;
        const currentFillAmount = session.fill_amount_during_session || 0;
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            fill_events: currentFillEvents + 1,
            fill_amount_during_session: currentFillAmount + fillResult.fillDetails.fillAmount,
            total_fill: (session.total_fill || 0) + fillResult.fillDetails.fillAmount,
            notes: `${session.notes || ''} | Fill: +${fillResult.fillDetails.fillAmount.toFixed(1)}L at ${new Date().toLocaleTimeString()}`
          })
          .eq('id', session.id);
          
        console.log(`🔋 Updated session ${session.id} with fill: +${fillResult.fillDetails.fillAmount.toFixed(1)}L`);
      }
      
    } catch (error) {
      console.error(`❌ Error handling fuel fill for ${plate}:`, error.message);
    }
  }

  async handleFuelFillSessionChange(plate, fuelFillStatus, vehicleData) {
    try {
      const currentTime = this.convertLocTime(vehicleData.LocTime);
      
      if (fuelFillStatus === 'START') {
        // Check if fuel fill session already exists
        const { data: existing } = await supabase
          .from('energy_rite_operating_sessions')
          .select('id')
          .eq('branch', plate)
          .eq('session_status', 'FUEL_FILL_ONGOING')
          .limit(1);
          
        if (!existing || existing.length === 0) {
          const openingFuel = parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0;
          const openingPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
          
          await supabase.from('energy_rite_operating_sessions').insert({
            branch: plate,
            company: 'KFC',
            session_date: currentTime.split('T')[0],
            session_start_time: currentTime,
            opening_fuel: openingFuel,
            opening_percentage: openingPercentage,
            session_status: 'FUEL_FILL_ONGOING',
            notes: `Fuel fill started. Opening: ${openingFuel}L (${openingPercentage}%)`
          });
          
          console.log(`⛽ FUEL FILL START: ${plate} - Opening: ${openingFuel}L (${openingPercentage}%)`);
        } else {
          console.log(`⛽ FUEL FILL ONGOING: ${plate} - Session already exists`);
        }
      }
      
    } catch (error) {
      console.error(`❌ Error handling fuel fill session for ${plate}:`, error.message);
    }
  }

  findClosestFuelData(plate, targetLocTime) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const maxWindow = 30 * 60 * 1000; // 30 minutes in LocTime
    
    let nextFuel = null;
    let minDiff = Infinity;
    
    // First check SQLite (priority for session data)
    const dbHistory = pendingFuelDb.getFuelHistoryAfter(plate, targetTime, 50);
    for (const entry of dbHistory) {
      const diff = entry.timestamp - targetTime;
      if (diff > 0 && diff <= maxWindow && diff < minDiff && entry.fuel_volume >= 0) {
        minDiff = diff;
        nextFuel = {
          fuel_probe_1_volume_in_tank: entry.fuel_volume,
          fuel_probe_1_level_percentage: entry.fuel_percentage,
          locTime: entry.loc_time,
          timestamp: entry.timestamp
        };
      }
    }
    
    // Fallback to in-memory cache if not found in SQLite
    if (!nextFuel) {
      const fuelHistory = this.recentFuelData.get(plate);
      if (fuelHistory && fuelHistory.length > 0) {
        for (const fuelData of fuelHistory) {
          const diff = fuelData.timestamp - targetTime;
          if (diff > 0 && diff <= maxWindow && diff < minDiff && fuelData.fuel_probe_1_volume_in_tank >= 0) {
            minDiff = diff;
            nextFuel = fuelData;
          }
        }
      }
    }
    
    if (nextFuel) {
      console.log(`🎯 Found next fuel data for ${plate}: ${(minDiff / 1000).toFixed(0)}s after status (LocTime-based)`);
      return nextFuel;
    }
    
    console.log(`⏳ No fuel data found within 30-min LocTime window after status for ${plate}, will retry when more data arrives`);
    return null;
  }

  findClosestFuelDataBefore(plate, targetLocTime) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const maxWindow = 30 * 60 * 1000; // 30 minutes in LocTime
    
    let closestFuel = null;
    let minDiff = Infinity;
    
    // First check SQLite (priority for session data)
    const dbHistory = pendingFuelDb.getFuelHistoryBefore(plate, targetTime, 50);
    for (const entry of dbHistory) {
      const diff = targetTime - entry.timestamp;
      if (diff > 0 && diff <= maxWindow && diff < minDiff && entry.fuel_volume >= 0) {
        minDiff = diff;
        closestFuel = {
          fuel_probe_1_volume_in_tank: entry.fuel_volume,
          fuel_probe_1_level_percentage: entry.fuel_percentage,
          locTime: entry.loc_time,
          timestamp: entry.timestamp
        };
      }
    }
    
    // Fallback to in-memory cache if not found in SQLite
    if (!closestFuel) {
      const fuelHistory = this.recentFuelData.get(plate);
      if (fuelHistory && fuelHistory.length > 0) {
        for (const fuelData of fuelHistory) {
          const diff = targetTime - fuelData.timestamp;
          if (diff > 0 && diff <= maxWindow && diff < minDiff && fuelData.fuel_probe_1_volume_in_tank >= 0) {
            minDiff = diff;
            closestFuel = fuelData;
          }
        }
      }
    }
    
    if (closestFuel) {
      console.log(`🎯 Found fuel data BEFORE ${plate}: ${closestFuel.fuel_probe_1_volume_in_tank}L at ${(minDiff / 1000).toFixed(0)}s before status (LocTime-based)`);
      return closestFuel;
    }
    
    console.log(`⏳ No fuel data found within 30-min LocTime window before status for ${plate}, will retry when more data arrives`);
    return null;
  }

  async handleSessionChange(plate, engineStatus, wsMessage = null) {
    try {
      const currentTime = this.convertLocTime(wsMessage?.LocTime);
      
      if (engineStatus === 'ON') {
        // Check if session already exists
        const { data: existing } = await supabase
          .from('energy_rite_operating_sessions')
          .select('id')
          .eq('branch', plate)
          .eq('session_status', 'ONGOING')
          .limit(1);
          
        if (!existing || existing.length === 0) {
          // C# uses fuel from SAME message for ENGINE ON (line 168: openingFuel=item.FuelProbe1VolumeInTank)
          const hasFuelData = wsMessage?.fuel_probe_1_volume_in_tank && parseFloat(wsMessage.fuel_probe_1_volume_in_tank) > 0;
          
          if (hasFuelData) {
            const openingFuel = parseFloat(wsMessage.fuel_probe_1_volume_in_tank);
            const openingPercentage = parseFloat(wsMessage.fuel_probe_1_level_percentage) || 0;
            
            // Validate: if fuel > 0 but percentage = 0, data is incomplete - wait for next message
            if (openingFuel > 10 && openingPercentage === 0) {
              console.log(`⚠️ ENGINE ON: ${plate} - Incomplete fuel data (${openingFuel}L but 0%), waiting for complete data`);
              const { data: newSession } = await supabase.from('energy_rite_operating_sessions').insert({
                branch: plate,
                company: 'KFC',
                session_date: currentTime.split('T')[0],
                session_start_time: currentTime,
                opening_fuel: 0,
                opening_percentage: 0,
                session_status: 'ONGOING',
                notes: 'Engine started. Waiting for complete fuel data...'
              }).select().single();
              
              if (newSession) {
                this.pendingFuelUpdates.set(plate, { 
                  sessionId: newSession.id, 
                  statusLocTime: wsMessage.LocTime,
                  timestamp: Date.now() 
                });
              }
              return;
            }
            
            await supabase.from('energy_rite_operating_sessions').insert({
              branch: plate,
              company: 'KFC',
              session_date: currentTime.split('T')[0],
              session_start_time: currentTime,
              opening_fuel: openingFuel,
              opening_percentage: openingPercentage,
              session_status: 'ONGOING',
              notes: `Engine started. Opening: ${openingFuel}L (${openingPercentage}%)`
            });
            
            console.log(`✅ ENGINE ON: ${plate} - Opening: ${openingFuel}L (${openingPercentage}%) (from same message)`);
          } else {
            // Wait for NEXT fuel data (like ENGINE OFF)
            const { data: newSession } = await supabase.from('energy_rite_operating_sessions').insert({
              branch: plate,
              company: 'KFC',
              session_date: currentTime.split('T')[0],
              session_start_time: currentTime,
              opening_fuel: 0,
              opening_percentage: 0,
              session_status: 'ONGOING',
              notes: 'Engine started. Waiting for fuel data...'
            }).select().single();
            
            if (newSession) {
              this.pendingFuelUpdates.set(plate, { 
                sessionId: newSession.id, 
                statusLocTime: wsMessage.LocTime,
                timestamp: Date.now() 
              });
              console.log(`⏳ ENGINE ON: ${plate} - Waiting for NEXT fuel data`);
            }
          }
        } else {
          console.log(`ℹ️ Engine session already exists for ${plate}`);
        }
      } else if (engineStatus === 'OFF') {
        // ALWAYS wait for NEXT fuel data (C# controller behavior)
        const { data: sessions } = await supabase
          .from('energy_rite_operating_sessions')
          .select('*')
          .eq('branch', plate)
          .eq('session_status', 'ONGOING')
          .order('session_start_time', { ascending: false })
          .limit(1);
          
        if (sessions && Array.isArray(sessions) && sessions.length > 0) {
          const session = sessions[0];
          // Mark session for closure, wait for NEXT fuel data after OFF status
          this.pendingClosures.set(plate, { 
            sessionId: session.id, 
            endTime: currentTime,
            statusLocTime: wsMessage.LocTime,
            timestamp: Date.now() 
          });
          console.log(`⏳ Engine OFF for ${plate} - Waiting for NEXT fuel data to complete session`);
        }
      }
      
      // Log activity
      await supabase.from('energy_rite_activity_log').insert({
        branch: plate,
        activity_type: engineStatus === 'ON' ? 'ENGINE_ON' : 'ENGINE_OFF',
        activity_time: currentTime,
        notes: `Engine ${engineStatus} detected`
      });
      
    } catch (error) {
      console.error(`❌ Error handling session change for ${plate}:`, error.message);
    }
  }



  async handleFuelFillEvent(plate, vehicleData) {
    try {
      const currentTime = vehicleData.LocTime ? new Date(vehicleData.LocTime) : new Date();
      const preFillLevel = parseFloat(vehicleData.fuel_probe_1_level) || 0;
      const preFillPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
      
      console.log(`⛽ Fuel fill detected for ${plate} - Pre-fill: ${preFillLevel}L (${preFillPercentage}%)`);
      
      if (!this.fuelFillTracking) this.fuelFillTracking = new Map();
      
      this.fuelFillTracking.set(plate, {
        preFillLevel,
        preFillPercentage,
        fillStartTime: currentTime,
        isTracking: true
      });
      
    } catch (error) {
      console.error(`❌ Error handling fuel fill start for ${plate}:`, error.message);
    }
  }

  async handleFuelFillEnd(plate, vehicleData) {
    try {
      if (!this.fuelFillTracking?.has(plate)) return;
      
      const tracking = this.fuelFillTracking.get(plate);
      if (!tracking.isTracking) return;
      
      const currentFuelLevel = parseFloat(vehicleData.fuel_probe_1_level) || 0;
      const currentFuelPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
      const fillAmount = Math.max(0, currentFuelLevel - tracking.preFillLevel);
      const currentTime = vehicleData.LocTime ? new Date(vehicleData.LocTime) : new Date();
      const fillDuration = (currentTime - tracking.fillStartTime) / 1000;
      
      console.log(`⛽ Fuel fill ended for ${plate}: +${fillAmount.toFixed(1)}L`);
      
      if (fillAmount > 5) { // Only record significant fills
        // Get vehicle info
        const { data: vehicleInfo } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('cost_code, company')
          .eq('plate', plate)
          .single();
        
        // Create fuel fill session in operating_sessions
        await supabase.from('energy_rite_operating_sessions').insert({
          branch: plate,
          company: vehicleInfo?.company || 'KFC',
          cost_code: vehicleInfo?.cost_code,
          session_date: currentTime.toISOString().split('T')[0],
          session_start_time: tracking.fillStartTime.toISOString(),
          session_end_time: currentTime.toISOString(),
          operating_hours: fillDuration / 3600,
          opening_fuel: tracking.preFillLevel,
          opening_percentage: tracking.preFillPercentage,
          closing_fuel: currentFuelLevel,
          closing_percentage: currentFuelPercentage,
          total_fill: fillAmount,
          session_status: 'FUEL_FILL',
          notes: `Fuel Fill: +${fillAmount.toFixed(1)}L (${tracking.preFillLevel}L → ${currentFuelLevel}L) Duration: ${fillDuration.toFixed(0)}s`
        });
        
        console.log(`⛽ Fuel fill session created: ${plate} +${fillAmount.toFixed(1)}L`);
        
        // Also update any ongoing engine session
        const { data: ongoingSessions } = await supabase
          .from('energy_rite_operating_sessions')
          .select('*')
          .eq('branch', plate)
          .eq('session_status', 'ONGOING')
          .order('session_start_time', { ascending: false })
          .limit(1);
          
        if (ongoingSessions && Array.isArray(ongoingSessions) && ongoingSessions.length > 0) {
          const session = ongoingSessions[0];
          await supabase
            .from('energy_rite_operating_sessions')
            .update({
              fill_events: (session.fill_events || 0) + 1,
              fill_amount_during_session: (session.fill_amount_during_session || 0) + fillAmount,
              total_fill: (session.total_fill || 0) + fillAmount,
              notes: `${session.notes || ''} | Fill: +${fillAmount.toFixed(1)}L at ${currentTime.toLocaleTimeString()}`
            })
            .eq('id', session.id);
        }
      }
      
      this.fuelFillTracking.delete(plate);
      
    } catch (error) {
      console.error(`❌ Error handling fuel fill end for ${plate}:`, error.message);
    }
  }

  async checkFuelFillCompletion(plate, vehicleData) {
    // This method is now mainly for backup/timeout scenarios
    // Primary detection is through status change
  }

  async completeSession(session, endTime, vehicleData) {
    try {
      const closingFuel = parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0;
      const closingPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
      const closingVolume = parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0;
      const closingTemperature = parseFloat(vehicleData.fuel_probe_1_temperature) || 0;
      
      const startTime = new Date(session.session_start_time);
      const endTimeDate = new Date(endTime);
      const durationMs = endTimeDate.getTime() - startTime.getTime();
      const operatingHours = Math.max(0, durationMs / (1000 * 60 * 60));
      const startingFuel = session.opening_fuel || 0;
      
      // Add any accumulated usage from fills during session
      const accumulatedUsage = session.total_usage || 0;
      
      // Calculate final usage: accumulated + (opening - closing)
      const finalUsage = accumulatedUsage + Math.max(0, startingFuel - closingFuel);
      const fuelCost = finalUsage * 20;
      const literUsagePerHour = operatingHours > 0 ? finalUsage / operatingHours : 0;
      
      await supabase.from('energy_rite_operating_sessions')
        .update({
          session_end_time: endTime,
          operating_hours: operatingHours,
          closing_fuel: closingFuel,
          closing_percentage: closingPercentage,
          closing_volume: closingVolume,
          closing_temperature: closingTemperature,
          total_usage: finalUsage,
          liter_usage_per_hour: literUsagePerHour,
          cost_for_usage: fuelCost,
          session_status: 'COMPLETED',
          notes: `Engine stopped. Duration: ${operatingHours.toFixed(2)}h, Opening: ${startingFuel}L, Closing: ${closingFuel}L, Used: ${finalUsage.toFixed(1)}L`
        })
        .eq('id', session.id);
        
      console.log(`🔴 Engine OFF: ${session.branch} - Used: ${finalUsage.toFixed(1)}L in ${operatingHours.toFixed(2)}h`);
    } catch (error) {
      console.error(`❌ Error completing session:`, error.message);
    }
  }

  async updatePendingClosure(plate, vehicleData) {
    try {
      if (!this.pendingClosures.has(plate)) return;
      
      const pending = this.pendingClosures.get(plate);
      
      // Find closest fuel data to ENGINE OFF LocTime
      const closestFuel = this.findClosestFuelData(plate, pending.statusLocTime);
      if (!closestFuel) {
        console.log(`⏳ Waiting for fuel data closer to ENGINE OFF time for ${plate}`);
        return;
      }
      
      // Get the session
      const { data: session } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('id', pending.sessionId)
        .single();
        
      if (session) {
        await this.completeSession(session, pending.endTime, closestFuel);
        this.pendingClosures.delete(plate);
        this.pendingFuelUpdates.delete(plate);
      }
      
    } catch (error) {
      console.error(`❌ Error updating pending closure for ${plate}:`, error.message);
    }
  }

  async updatePendingSessionFuel(plate, vehicleData) {
    try {
      if (!this.pendingFuelUpdates.has(plate)) return;
      
      const pending = this.pendingFuelUpdates.get(plate);
      
      // C# uses fuel from SAME message as ENGINE ON (line 168)
      // Find fuel BEFORE the ON status (closest to same message)
      const closestFuel = this.findClosestFuelDataBefore(plate, pending.statusLocTime);
      
      if (!closestFuel) {
        console.log(`⏳ Waiting for fuel data BEFORE ENGINE ON time for ${plate}`);
        return;
      }
      
      // Update session with fuel BEFORE engine on
      await supabase.from('energy_rite_operating_sessions')
        .update({
          opening_fuel: closestFuel.fuel_probe_1_volume_in_tank,
          opening_percentage: closestFuel.fuel_probe_1_level_percentage,
          notes: `Engine started. Opening: ${closestFuel.fuel_probe_1_volume_in_tank}L (${closestFuel.fuel_probe_1_level_percentage}%) [before ON status]`
        })
        .eq('id', pending.sessionId);
        
      console.log(`✅ Updated pending session for ${plate} with fuel BEFORE ON: ${closestFuel.fuel_probe_1_volume_in_tank}L`);
      this.pendingFuelUpdates.delete(plate);
      
    } catch (error) {
      console.error(`❌ Error updating pending session fuel for ${plate}:`, error.message);
    }
  }

  close() {
    if (this.ws) {
      this.ws.close();
    }
    // Close SQLite database on shutdown
    pendingFuelDb.closeDatabase();
  }
}

module.exports = EnergyRiteWebSocketClient;