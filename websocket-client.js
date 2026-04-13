const WebSocket = require('ws');
const { supabase } = require('./supabase-client');
const { detectFuelTheft } = require('./helpers/fuel-theft-detector');
const { detectFuelFill } = require('./helpers/fuel-fill-detector');
const pendingFuelDb = require('./pending-fuel-db');

const FUEL_STABILITY_CONFIG = {
  engineOnLookbackMs: 2 * 60 * 1000,
  engineOnFallbackMs: 2 * 60 * 1000,
  engineOffSettleDelayMs: 30 * 1000,
  engineOffWindowMs: 3 * 60 * 1000,
  fillSettleWindowMs: 2 * 60 * 1000,
  medianSampleSize: 5,
  minStableSamples: 2,
  maxStableSpreadLiters: 3
};

class EnergyRiteWebSocketClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
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
    // Get watchers where fuel hasn't changed for required duration
    // FILL: 2 minutes, THEFT: 10 minutes
    const allWatchers = pendingFuelDb.getAllFuelFillWatchers();
    const now = Date.now();
    
    for (const watcher of allWatchers) {
      const timeSinceLastChange = now - watcher.last_increased_at;
      const watcherType = watcher.watcher_type || 'FILL';
      
      // Different stabilization times
      const requiredStableTime = watcherType === 'THEFT' ? 10 * 60 * 1000 : 2 * 60 * 1000;
      
      if (timeSinceLastChange >= requiredStableTime) {
        console.log(`🎯 ${watcherType} STABILIZED: ${watcher.plate} - No change for ${(timeSinceLastChange/1000).toFixed(0)}s`);
        this.completeFuelFillWatcher(watcher.plate);
      }
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
    const allWatchers = pendingFuelDb.getAllFuelFillWatchers();
    const now = Date.now();
    
    for (const watcher of allWatchers) {
      const timeSinceLastChange = now - watcher.last_increased_at;
      const watcherType = watcher.watcher_type || 'FILL';
      const requiredStableTime = watcherType === 'THEFT' ? 10 * 60 * 1000 : 2 * 60 * 1000;
      
      if (timeSinceLastChange >= requiredStableTime) {
        console.log(`🎯 Completing stabilized ${watcherType} watcher for ${watcher.plate} on startup`);
        this.completeFuelFillWatcher(watcher.plate);
      }
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
        Speed: vehicleData.Speed,
        Temperature: vehicleData.Temperature
      });
      
      // Parse fuel data from Temperature field if present
      const fuelData = this.parseFuelData(vehicleData);
      const speed = parseFloat(vehicleData.Speed) || 0;
      
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
          speed: speed,
          ...fuelData
        });
        // Keep last 30 minutes in memory (for fuel theft detection)
        const thirtyMinutesAgo = fuelTimestamp - (30 * 60 * 1000);
        while (fuelHistory.length > 0 && fuelHistory[0].timestamp < thirtyMinutesAgo) {
          fuelHistory.shift();
        }
        
        // Also store in SQLite for persistence across restarts.
        // Only persist packets that actually carried fuel probe fields.
        pendingFuelDb.storeFuelHistory(actualBranch, {
          combinedFuelVolume: fuelData.combined_fuel_volume_in_tank,
          combinedFuelPercentage: fuelData.combined_fuel_percentage,
          fuelVolumeProbe1: fuelData.fuel_probe_1_volume_in_tank,
          fuelVolumeProbe2: fuelData.fuel_probe_2_volume_in_tank,
          fuelPercentageProbe1: fuelData.fuel_probe_1_level_percentage,
          fuelPercentageProbe2: fuelData.fuel_probe_2_level_percentage,
          locTime: vehicleData.LocTime,
          timestamp: fuelTimestamp
        }, vehicleData.LocTime, fuelTimestamp);
        // Update any pending sessions waiting for opening fuel data
        await this.updatePendingSessionFuel(actualBranch, vehicleData);
        
        // Update any pending closures waiting for closing fuel data
        await this.updatePendingClosure(actualBranch, vehicleData);
      }
      
      // Handle engine/ignition status FIRST
      const engineStatus = this.parseEngineStatus(vehicleData.DriverName);
      if (engineStatus) {
        console.log(`🔧 Processing engine ${engineStatus} for ${actualBranch}`);
        await this.handleSessionChange(actualBranch, engineStatus, vehicleData);
      }
      
      // Only start fuel fills when the status explicitly says "Possible Fuel Fill"
      const fuelFillStatus = this.parseFuelFillStatus(vehicleData.DriverName);
      if (fuelFillStatus) {
        await this.handleFuelFillStart(actualBranch, vehicleData);
      }

      // Fuel thefts are handled by unified engine-off fuel-change detection.
      
      // Waterford fuel monitoring: fills and thefts when engine OFF
      if (fuelData.hasFuelData) {
        await this.detectWaterfordFuelChanges(actualBranch, fuelData, vehicleData.LocTime, speed);
      }

    } catch (error) {
      console.error('❌ Error processing vehicle update:', error);
    }
  }



  parseFuelData(vehicleData) {
    if (!this.hasExplicitFuelFields(vehicleData)) {
      return { hasFuelData: false };
    }

    const fuelProbe1Level = this.parseNumericFuelValue(vehicleData.fuel_probe_1_level);
    const fuelProbe1Percentage = this.parseNumericFuelValue(vehicleData.fuel_probe_1_level_percentage);
    const fuelProbe1Volume = this.parseNumericFuelValue(vehicleData.fuel_probe_1_volume_in_tank);
    const fuelProbe1Temperature = this.parseNumericFuelValue(vehicleData.fuel_probe_1_temperature);
    const fuelProbe2Level = this.parseNumericFuelValue(vehicleData.fuel_probe_2_level);
    const fuelProbe2Percentage = this.parseNumericFuelValue(vehicleData.fuel_probe_2_level_percentage);
    const fuelProbe2Volume = this.parseNumericFuelValue(vehicleData.fuel_probe_2_volume_in_tank);
    const fuelProbe2Temperature = this.parseNumericFuelValue(vehicleData.fuel_probe_2_temperature);
    const percentageValues = [];
    const temperatureValues = [];

    if (this.hasFuelValue(vehicleData.fuel_probe_1_level_percentage)) percentageValues.push(fuelProbe1Percentage);
    if (this.hasFuelValue(vehicleData.fuel_probe_2_level_percentage)) percentageValues.push(fuelProbe2Percentage);
    if (this.hasFuelValue(vehicleData.fuel_probe_1_temperature)) temperatureValues.push(fuelProbe1Temperature);
    if (this.hasFuelValue(vehicleData.fuel_probe_2_temperature)) temperatureValues.push(fuelProbe2Temperature);

    return {
      hasFuelData: true,
      fuel_probe_1_level: fuelProbe1Level,
      fuel_probe_1_level_percentage: fuelProbe1Percentage,
      fuel_probe_1_volume_in_tank: fuelProbe1Volume,
      fuel_probe_1_temperature: fuelProbe1Temperature,
      fuel_probe_2_level: fuelProbe2Level,
      fuel_probe_2_level_percentage: fuelProbe2Percentage,
      fuel_probe_2_volume_in_tank: fuelProbe2Volume,
      fuel_probe_2_temperature: fuelProbe2Temperature,
      combined_fuel_level: fuelProbe1Level + fuelProbe2Level,
      combined_fuel_volume_in_tank: fuelProbe1Volume + fuelProbe2Volume,
      combined_fuel_percentage: percentageValues.length > 0
        ? percentageValues.reduce((sum, value) => sum + value, 0) / percentageValues.length
        : 0,
      combined_fuel_temperature: temperatureValues.length > 0
        ? temperatureValues.reduce((sum, value) => sum + value, 0) / temperatureValues.length
        : 0
    };
  }

  parseNumericFuelValue(value) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  hasFuelValue(value) {
    return value !== undefined && value !== null && String(value).trim() !== '';
  }

  hasExplicitFuelFields(vehicleData) {
    if (!vehicleData) return false;

    const fuelKeys = [
      'fuel_probe_1_level',
      'fuel_probe_1_volume_in_tank',
      'fuel_probe_1_level_percentage',
      'fuel_probe_1_temperature',
      'fuel_probe_2_level',
      'fuel_probe_2_volume_in_tank',
      'fuel_probe_2_level_percentage',
      'fuel_probe_2_temperature'
    ];

    return fuelKeys.some((key) => this.hasFuelValue(vehicleData[key]));
  }

  buildFuelSnapshot(source, fallbackLocTime = null, fallbackTimestamp = null) {
    if (!source) return null;

    const hasSignal =
      source.hasFuelData === true ||
      this.hasExplicitFuelFields(source) ||
      source.fuel_volume !== undefined ||
      source.fuel_probe_1_volume_in_tank !== undefined;

    if (!hasSignal) {
      return null;
    }

    const fuelProbe1Volume = this.parseNumericFuelValue(source.fuel_probe_1_volume_in_tank ?? source.fuel_volume);
    const fuelProbe2Volume = this.parseNumericFuelValue(source.fuel_probe_2_volume_in_tank);
    const fuelProbe1Percentage = this.parseNumericFuelValue(source.fuel_probe_1_level_percentage ?? source.fuel_percentage);
    const fuelProbe2Percentage = this.parseNumericFuelValue(source.fuel_probe_2_level_percentage);
    const percentageValues = [];
    if (this.hasFuelValue(source.fuel_probe_1_level_percentage ?? source.fuel_percentage)) percentageValues.push(fuelProbe1Percentage);
    if (this.hasFuelValue(source.fuel_probe_2_level_percentage)) percentageValues.push(fuelProbe2Percentage);
    const locTime = source.locTime ?? source.loc_time ?? fallbackLocTime ?? source.LocTime ?? null;
    const timestamp = source.timestamp ?? fallbackTimestamp ?? (locTime ? new Date(this.convertLocTime(locTime)).getTime() : null);

    if (timestamp === null) {
      return null;
    }

    return {
      fuel_probe_1_volume_in_tank: fuelProbe1Volume,
      fuel_probe_1_level_percentage: fuelProbe1Percentage,
      fuel_probe_2_volume_in_tank: fuelProbe2Volume,
      fuel_probe_2_level_percentage: fuelProbe2Percentage,
      combined_fuel_volume_in_tank: fuelProbe1Volume + fuelProbe2Volume,
      combined_fuel_percentage: percentageValues.length > 0
        ? percentageValues.reduce((sum, value) => sum + value, 0) / percentageValues.length
        : 0,
      locTime,
      timestamp
    };
  }

  buildSessionFuelFields(prefix, fuelData) {
    if (!fuelData) {
      return {};
    }

    const fuelProbe1 = this.parseNumericFuelValue(fuelData.fuel_probe_1_volume_in_tank);
    const fuelProbe2 = this.parseNumericFuelValue(fuelData.fuel_probe_2_volume_in_tank);
    const percentageProbe1 = this.parseNumericFuelValue(fuelData.fuel_probe_1_level_percentage);
    const percentageProbe2 = this.parseNumericFuelValue(fuelData.fuel_probe_2_level_percentage);
    const combinedFuel = this.hasFuelValue(fuelData.combined_fuel_volume_in_tank)
      ? this.parseNumericFuelValue(fuelData.combined_fuel_volume_in_tank)
      : this.hasFuelValue(fuelData.combined_fuel_level)
        ? this.parseNumericFuelValue(fuelData.combined_fuel_level)
      : fuelProbe1 + fuelProbe2;
    const percentageValues = [];

    if (this.hasFuelValue(fuelData.fuel_probe_1_level_percentage)) percentageValues.push(percentageProbe1);
    if (this.hasFuelValue(fuelData.fuel_probe_2_level_percentage)) percentageValues.push(percentageProbe2);

    const combinedPercentage = this.hasFuelValue(fuelData.combined_fuel_percentage)
      ? this.parseNumericFuelValue(fuelData.combined_fuel_percentage)
      : (percentageValues.length > 0
        ? percentageValues.reduce((sum, value) => sum + value, 0) / percentageValues.length
        : 0);

    return {
      [`${prefix}_fuel`]: combinedFuel,
      [`${prefix}_percentage`]: combinedPercentage,
      [`${prefix}_fuel_probe_1`]: fuelProbe1,
      [`${prefix}_fuel_probe_2`]: fuelProbe2,
      [`${prefix}_percentage_probe_1`]: percentageProbe1,
      [`${prefix}_percentage_probe_2`]: percentageProbe2
    };
  }

  getClosestFuelSnapshot(plate, targetLocTime, direction, currentMessage = null, maxWindowMs = 5 * 60 * 1000) {
    const targetTimestamp = new Date(this.convertLocTime(targetLocTime)).getTime();
    const candidates = [];
    const seen = new Set();

    const addCandidate = (snapshot, source) => {
      if (!snapshot || snapshot.timestamp === null || snapshot.timestamp === undefined) return;
      const diffMs = direction === 'before'
        ? targetTimestamp - snapshot.timestamp
        : snapshot.timestamp - targetTimestamp;

      if (diffMs < 0 || diffMs > maxWindowMs) return;
      if ((snapshot.combined_fuel_volume_in_tank || 0) <= 0) return;

      const key = `${snapshot.timestamp}:${snapshot.locTime || ''}:${source}`;
      if (seen.has(key)) return;
      seen.add(key);
      candidates.push({ ...snapshot, diffMs, source });
    };

    const currentSnapshot = this.buildFuelSnapshot(
      currentMessage,
      currentMessage?.LocTime,
      currentMessage?.LocTime ? new Date(this.convertLocTime(currentMessage.LocTime)).getTime() : null
    );
    addCandidate(currentSnapshot, 'current');

    const memoryHistory = this.recentFuelData.get(plate) || [];
    for (const entry of memoryHistory) {
      const normalized = this.buildFuelSnapshot(entry, entry.locTime, entry.timestamp);
      addCandidate(normalized, 'memory');
    }

    const dbHistory = direction === 'before'
      ? pendingFuelDb.getFuelHistoryBefore(plate, targetTimestamp, 50)
      : pendingFuelDb.getFuelHistoryAfter(plate, targetTimestamp, 50);

    for (const entry of dbHistory) {
      const normalized = this.buildFuelSnapshot(entry, entry.loc_time, entry.timestamp);
      addCandidate(normalized, 'sqlite');
    }

    if (candidates.length === 0) {
      return null;
    }

    candidates.sort((a, b) => {
      if (a.diffMs !== b.diffMs) return a.diffMs - b.diffMs;
      return b.timestamp - a.timestamp;
    });

    return candidates[0];
  }

  findFuelSnapshotAtOrBefore(plate, targetLocTime, currentMessage = null) {
    const closest = this.getClosestFuelSnapshot(plate, targetLocTime, 'before', currentMessage, 2 * 60 * 1000);
    if (closest) {
      console.log(
        `[capture] Using closest fuel BEFORE ${plate}: ${closest.combined_fuel_volume_in_tank}L ` +
        `(${(closest.diffMs / 1000).toFixed(0)}s before, source: ${closest.source})`
      );
      return closest;
    }

    console.log(`[capture] No valid fuel data within 120s before status for ${plate}, will retry when more data arrives`);
    return null;
  }

  findFuelSnapshotAtOrAfter(plate, targetLocTime, currentMessage = null) {
    const closest = this.getClosestFuelSnapshot(plate, targetLocTime, 'after', currentMessage, 3 * 60 * 1000);
    if (closest) {
      console.log(
        `[capture] Using closest fuel AFTER ${plate}: ${closest.combined_fuel_volume_in_tank}L ` +
        `(${(closest.diffMs / 1000).toFixed(0)}s after, source: ${closest.source})`
      );
      return closest;
    }

    console.log(`[capture] No valid fuel data within 180s after status for ${plate}, will retry when more data arrives`);
    return null;
  }
  trackPreFillLowest(plate, fuelData, locTime) {
    try {
      const existing = pendingFuelDb.getPreFillWatcher(plate);
      
      if (!existing) {
        pendingFuelDb.setPreFillWatcher(plate, {
          lowestFuel: fuelData.combined_fuel_volume_in_tank,
          lowestPercentage: fuelData.combined_fuel_percentage,
          lowestFuelProbe1: fuelData.fuel_probe_1_volume_in_tank,
          lowestFuelProbe2: fuelData.fuel_probe_2_volume_in_tank,
          lowestPercentageProbe1: fuelData.fuel_probe_1_level_percentage,
          lowestPercentageProbe2: fuelData.fuel_probe_2_level_percentage,
          lowestLocTime: locTime
        });
      } else {
        const currentFuel = fuelData.combined_fuel_volume_in_tank;
        
        // Update if lower
        if (currentFuel < existing.lowest_fuel) {
          pendingFuelDb.setPreFillWatcher(plate, {
            lowestFuel: currentFuel,
            lowestPercentage: fuelData.combined_fuel_percentage,
            lowestFuelProbe1: fuelData.fuel_probe_1_volume_in_tank,
            lowestFuelProbe2: fuelData.fuel_probe_2_volume_in_tank,
            lowestPercentageProbe1: fuelData.fuel_probe_1_level_percentage,
            lowestPercentageProbe2: fuelData.fuel_probe_2_level_percentage,
            lowestLocTime: locTime
          });
        } else {
          // Just update last_update timestamp
          pendingFuelDb.setPreFillWatcher(plate, {
            lowestFuel: existing.lowest_fuel,
            lowestPercentage: existing.lowest_percentage,
            lowestFuelProbe1: existing.lowest_fuel_probe_1,
            lowestFuelProbe2: existing.lowest_fuel_probe_2,
            lowestPercentageProbe1: existing.lowest_percentage_probe_1,
            lowestPercentageProbe2: existing.lowest_percentage_probe_2,
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
   * Unified fuel change detection - handles both fills and thefts:
   * - Detects +10L increase (fill) or -50L decrease (theft) while engine OFF
   * - Message-to-message comparison within 2 minutes LocTime
   * - Watcher tracks highest (for fills) or lowest (for thefts)
   * - Completes when fuel stabilizes (2 min no change)
   */
  async detectWaterfordFuelChanges(plate, fuelData, locTime, speed) {
    try {
      const currentFuel = fuelData.combined_fuel_volume_in_tank;
      const currentLocTime = new Date(this.convertLocTime(locTime)).getTime();
      
      // Ignore if vehicle is moving (speed >= 10 km/h)
      if (speed >= 10) {
        return;
      }
      
      // Check if engine/ignition is OFF
      const { data: sessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('session_status')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .limit(1);
      
      const engineIsOff = !sessions || sessions.length === 0;
      if (!engineIsOff) {
        return; // Only detect changes when engine/ignition is OFF
      }
      
      // Check if we have an active watcher
      const watcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (watcher) {
        // Update highest (for fills) or lowest (for thefts)
        if (watcher.watcher_type === 'FILL' && currentFuel > watcher.highest_fuel) {
          pendingFuelDb.updateFuelFillWatcherHighest(
            plate,
            currentFuel,
            fuelData.combined_fuel_percentage,
            locTime,
            fuelData.fuel_probe_1_volume_in_tank,
            fuelData.fuel_probe_2_volume_in_tank,
            fuelData.fuel_probe_1_level_percentage,
            fuelData.fuel_probe_2_level_percentage
          );
          console.log(`📈 Fill watcher: ${plate} highest now ${currentFuel}L`);
        } else if (watcher.watcher_type === 'THEFT' && currentFuel < watcher.lowest_fuel) {
          pendingFuelDb.updateFuelFillWatcherLowest(
            plate,
            currentFuel,
            fuelData.combined_fuel_percentage,
            locTime,
            fuelData.fuel_probe_1_volume_in_tank,
            fuelData.fuel_probe_2_volume_in_tank,
            fuelData.fuel_probe_1_level_percentage,
            fuelData.fuel_probe_2_level_percentage
          );
          console.log(`📉 Theft watcher: ${plate} lowest now ${currentFuel}L`);
        }
        return;
      }
      
      // Look at the last 2 minutes of off-engine fuel history so fills/thefts can
      // be detected even when the change arrives across several packets.
      const fuelHistory = this.recentFuelData.get(plate);
      if (!fuelHistory || fuelHistory.length === 0) return;

      const twoMinutes = 2 * 60 * 1000;
      const recentWindow = fuelHistory.filter((entry) => entry.timestamp < currentLocTime && (currentLocTime - entry.timestamp) <= twoMinutes && (parseFloat(entry.speed) || 0) < 10);
      if (recentWindow.length === 0) return;

      let lowestEntry = recentWindow[0];
      let highestEntry = recentWindow[0];

      for (const entry of recentWindow) {
        if (entry.combined_fuel_volume_in_tank < lowestEntry.combined_fuel_volume_in_tank) {
          lowestEntry = entry;
        }
        if (entry.combined_fuel_volume_in_tank > highestEntry.combined_fuel_volume_in_tank) {
          highestEntry = entry;
        }
      }

      const theftDrop = highestEntry.combined_fuel_volume_in_tank - currentFuel;
      
      // THEFT DETECTION: -50L drop within the recent engine-off window
      if (theftDrop >= 50) {
        console.log(`[theft] POSSIBLE THEFT: ${plate} - -${theftDrop.toFixed(1)}L in recent off-engine window (waiting for stabilization)`);
        
        const openingFuel = highestEntry.combined_fuel_volume_in_tank;
        const openingPercentage = highestEntry.combined_fuel_percentage;
        const openingFuelProbe1 = highestEntry.fuel_probe_1_volume_in_tank;
        const openingFuelProbe2 = highestEntry.fuel_probe_2_volume_in_tank;
        const openingPercentageProbe1 = highestEntry.fuel_probe_1_level_percentage;
        const openingPercentageProbe2 = highestEntry.fuel_probe_2_level_percentage;
        const openingLocTime = highestEntry.locTime;
        
        pendingFuelDb.setFuelFillWatcher(plate, {
          startTime: this.convertLocTime(openingLocTime),
          startLocTime: openingLocTime,
          openingFuel: openingFuel,
          openingPercentage: openingPercentage,
          openingFuelProbe1,
          openingFuelProbe2,
          openingPercentageProbe1,
          openingPercentageProbe2,
          lowestFuel: currentFuel,
          lowestPercentage: fuelData.combined_fuel_percentage,
          lowestFuelProbe1: fuelData.fuel_probe_1_volume_in_tank,
          lowestFuelProbe2: fuelData.fuel_probe_2_volume_in_tank,
          lowestPercentageProbe1: fuelData.fuel_probe_1_level_percentage,
          lowestPercentageProbe2: fuelData.fuel_probe_2_level_percentage,
          lowestLocTime: locTime,
          watcherType: 'THEFT'
        });
        
        console.log(`[theft] THEFT WATCHER: ${plate} - Opening: ${openingFuel}L, Current: ${currentFuel}L`);
      }    } catch (error) {
      console.error(`❌ Error detecting fuel changes for ${plate}:`, error.message);
    }
  }

  /**
   * Passive fill detection - detects fills without "FUEL FILL" status message
   * Triggers when fuel increases by 10L+ within 2 minutes
   */
  async detectPassiveFill(plate, fuelData, locTime) {
    try {
      const currentFuel = fuelData.combined_fuel_volume_in_tank;
      
      // Check if we already have an active fill watcher - if so, just update highest fuel
      const existingWatcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (existingWatcher) {
        if (currentFuel > existingWatcher.highest_fuel) {
          pendingFuelDb.updateFuelFillWatcherHighest(
            plate,
            currentFuel,
            fuelData.combined_fuel_percentage,
            locTime,
            fuelData.fuel_probe_1_volume_in_tank,
            fuelData.fuel_probe_2_volume_in_tank,
            fuelData.fuel_probe_1_level_percentage,
            fuelData.fuel_probe_2_level_percentage
          );
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
      let lowestRecentFuelProbe1 = null;
      let lowestRecentFuelProbe2 = null;
      let lowestRecentPercentageProbe1 = null;
      let lowestRecentPercentageProbe2 = null;
      
      const fuelHistory = this.recentFuelData.get(plate);
      if (fuelHistory && fuelHistory.length > 0) {
        for (const entry of fuelHistory) {
          if (entry.timestamp >= twoMinutesAgo && entry.timestamp < currentTime) {
            if (lowestRecentFuel === null || entry.combined_fuel_volume_in_tank < lowestRecentFuel) {
              lowestRecentFuel = entry.combined_fuel_volume_in_tank;
              lowestRecentTime = entry.timestamp;
              lowestRecentLocTime = entry.locTime;
              lowestRecentPercentage = entry.combined_fuel_percentage;
              lowestRecentFuelProbe1 = entry.fuel_probe_1_volume_in_tank;
              lowestRecentFuelProbe2 = entry.fuel_probe_2_volume_in_tank;
              lowestRecentPercentageProbe1 = entry.fuel_probe_1_level_percentage;
              lowestRecentPercentageProbe2 = entry.fuel_probe_2_level_percentage;
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
        lowestRecentFuelProbe1 = dbLowest.fuel_volume_probe_1;
        lowestRecentFuelProbe2 = dbLowest.fuel_volume_probe_2;
        lowestRecentPercentageProbe1 = dbLowest.fuel_percentage_probe_1;
        lowestRecentPercentageProbe2 = dbLowest.fuel_percentage_probe_2;
      }
      
      if (lowestRecentFuel === null) return;
      
      // Check if fuel increased by 10L+ from the lowest reading
      const fuelIncrease = currentFuel - lowestRecentFuel;
      
      if (fuelIncrease >= 10) {
        console.log(`🔎 PASSIVE FILL DETECTED for ${plate}: +${fuelIncrease.toFixed(1)}L in ${((currentTime - lowestRecentTime) / 1000).toFixed(0)}s`);
        
        // Get the pre-fill lowest if available (might be even lower than 2-min window)
        const preFill = pendingFuelDb.getPreFillWatcher(plate);
        let openingFuel, openingPercentage, openingLocTime;
        let openingFuelProbe1, openingFuelProbe2, openingPercentageProbe1, openingPercentageProbe2;
        
        if (preFill && preFill.lowest_fuel < lowestRecentFuel) {
          openingFuel = preFill.lowest_fuel;
          openingPercentage = preFill.lowest_percentage;
          openingLocTime = preFill.lowest_loc_time;
          openingFuelProbe1 = preFill.lowest_fuel_probe_1;
          openingFuelProbe2 = preFill.lowest_fuel_probe_2;
          openingPercentageProbe1 = preFill.lowest_percentage_probe_1;
          openingPercentageProbe2 = preFill.lowest_percentage_probe_2;
          console.log(`🔎 Using pre-fill lowest: ${openingFuel}L`);
        } else {
          openingFuel = lowestRecentFuel;
          openingPercentage = lowestRecentPercentage;
          openingLocTime = lowestRecentLocTime;
          openingFuelProbe1 = lowestRecentFuelProbe1;
          openingFuelProbe2 = lowestRecentFuelProbe2;
          openingPercentageProbe1 = lowestRecentPercentageProbe1;
          openingPercentageProbe2 = lowestRecentPercentageProbe2;
          console.log(`🔎 Using 2-min lowest: ${openingFuel}L`);
        }
        
        // Start a watcher to track highest fuel (fill might still be ongoing)
        const startTime = this.convertLocTime(openingLocTime);
        pendingFuelDb.setFuelFillWatcher(plate, {
          startTime: startTime,
          startLocTime: openingLocTime,
          openingFuel: openingFuel,
          openingPercentage: openingPercentage,
          openingFuelProbe1,
          openingFuelProbe2,
          openingPercentageProbe1,
          openingPercentageProbe2,
          highestFuel: currentFuel,
          highestPercentage: fuelData.combined_fuel_percentage,
          highestFuelProbe1: fuelData.fuel_probe_1_volume_in_tank,
          highestFuelProbe2: fuelData.fuel_probe_2_volume_in_tank,
          highestPercentageProbe1: fuelData.fuel_probe_1_level_percentage,
          highestPercentageProbe2: fuelData.fuel_probe_2_level_percentage,
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
      const hasFuelData = this.hasFuelValue(vehicleData.fuel_probe_1_volume_in_tank) || this.hasFuelValue(vehicleData.fuel_probe_2_volume_in_tank);
      
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
      let openingFuelProbe1, openingFuelProbe2, openingPercentageProbe1, openingPercentageProbe2;
      const preFill = pendingFuelDb.getPreFillWatcher(plate);
      const closestFuel = this.findClosestFuelDataBefore(plate, vehicleData.LocTime);
      
      console.log(`🔍 FILL START DEBUG for ${plate}:`);
      console.log(`   Fill status LocTime: ${vehicleData.LocTime}`);
      console.log(`   Pre-fill lowest: ${preFill ? preFill.lowest_fuel + 'L' : 'NONE'}`);
      console.log(`   Closest fuel found: ${closestFuel ? closestFuel.combined_fuel_volume_in_tank + 'L' : 'NONE'}`);
      
      if (preFill && preFill.lowest_fuel > 0) {
        openingFuel = preFill.lowest_fuel;
        openingPercentage = preFill.lowest_percentage;
        openingFuelProbe1 = preFill.lowest_fuel_probe_1;
        openingFuelProbe2 = preFill.lowest_fuel_probe_2;
        openingPercentageProbe1 = preFill.lowest_percentage_probe_1;
        openingPercentageProbe2 = preFill.lowest_percentage_probe_2;
        console.log(`⛽ FUEL FILL START: ${plate} - Using pre-fill lowest: ${openingFuel}L (tracked since ${preFill.lowest_loc_time})`);
        // Delete watcher ONLY after successfully using it
        pendingFuelDb.deletePreFillWatcher(plate);
      } else if (closestFuel && closestFuel.combined_fuel_volume_in_tank > 0) {
        openingFuel = closestFuel.combined_fuel_volume_in_tank;
        openingPercentage = closestFuel.combined_fuel_percentage;
        openingFuelProbe1 = closestFuel.fuel_probe_1_volume_in_tank;
        openingFuelProbe2 = closestFuel.fuel_probe_2_volume_in_tank;
        openingPercentageProbe1 = closestFuel.fuel_probe_1_level_percentage;
        openingPercentageProbe2 = closestFuel.fuel_probe_2_level_percentage;
        console.log(`⛽ FUEL FILL START: ${plate} - Using closest fuel before status: ${openingFuel}L`);
      } else if (hasFuelData) {
        const currentFuelData = this.parseFuelData(vehicleData);
        openingFuel = currentFuelData.combined_fuel_volume_in_tank;
        openingPercentage = currentFuelData.combined_fuel_percentage;
        openingFuelProbe1 = currentFuelData.fuel_probe_1_volume_in_tank;
        openingFuelProbe2 = currentFuelData.fuel_probe_2_volume_in_tank;
        openingPercentageProbe1 = currentFuelData.fuel_probe_1_level_percentage;
        openingPercentageProbe2 = currentFuelData.fuel_probe_2_level_percentage;
        console.log(`⛽ FUEL FILL START: ${plate} - Using current fuel: ${openingFuel}L`);
      }
      
      if (openingFuel && openingFuel > 0) {
        const currentFuelData = hasFuelData ? this.parseFuelData(vehicleData) : null;
        const highestFuel = currentFuelData?.combined_fuel_volume_in_tank ?? openingFuel;
        const highestPercentage = currentFuelData?.combined_fuel_percentage ?? openingPercentage;
        const highestFuelProbe1 = currentFuelData?.fuel_probe_1_volume_in_tank ?? openingFuelProbe1;
        const highestFuelProbe2 = currentFuelData?.fuel_probe_2_volume_in_tank ?? openingFuelProbe2;
        const highestPercentageProbe1 = currentFuelData?.fuel_probe_1_level_percentage ?? openingPercentageProbe1;
        const highestPercentageProbe2 = currentFuelData?.fuel_probe_2_level_percentage ?? openingPercentageProbe2;
        const highestLocTime = currentFuelData ? vehicleData.LocTime : openingLocTime;

        pendingFuelDb.setFuelFillWatcher(plate, {
          startTime: currentTime,
          startLocTime: vehicleData.LocTime,
          openingFuel: openingFuel,
          openingPercentage: openingPercentage,
          openingFuelProbe1,
          openingFuelProbe2,
          openingPercentageProbe1,
          openingPercentageProbe2,
          highestFuel,
          highestPercentage,
          highestFuelProbe1,
          highestFuelProbe2,
          highestPercentageProbe1,
          highestPercentageProbe2,
          highestLocTime,
          watcherType: 'FILL'
        });
        
        console.log(`⛽ FUEL FILL START: ${plate} - Opening: ${openingFuel}L (status trigger)`);
      } else {
        console.log(`⚠️ FUEL FILL START: ${plate} - No valid opening fuel found, skipping session creation until fuel data available`);
        // Don't create pending fill without fuel data - wait for passive detection to handle it
        return;
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
        const currentFuel = fuelData.combined_fuel_volume_in_tank;
        
        if (currentFuel > watcher.highest_fuel) {
          pendingFuelDb.updateFuelFillWatcherHighest(
            plate,
            currentFuel,
            fuelData.combined_fuel_percentage,
            locTime,
            fuelData.fuel_probe_1_volume_in_tank,
            fuelData.fuel_probe_2_volume_in_tank,
            fuelData.fuel_probe_1_level_percentage,
            fuelData.fuel_probe_2_level_percentage
          );
          console.log(`📈 New highest fuel for ${plate}: ${currentFuel}L`);
        }
        return;
      }
      
      const pending = pendingFuelDb.getPendingFuelFill(plate);
      if (!pending) return;
      
      const currentTime = new Date(this.convertLocTime(locTime)).getTime();
      const startTime = new Date(pending.start_time).getTime();
      
      if (pending.waitingForOpeningFuel && currentTime > startTime) {
        pendingFuelDb.updatePendingFuelFillOpening(
          plate,
          fuelData.combined_fuel_volume_in_tank,
          fuelData.combined_fuel_percentage,
          fuelData.fuel_probe_1_volume_in_tank,
          fuelData.fuel_probe_2_volume_in_tank,
          fuelData.fuel_probe_1_level_percentage,
          fuelData.fuel_probe_2_level_percentage
        );
        console.log(`✅ Got opening fuel for fill: ${plate} - ${fuelData.combined_fuel_volume_in_tank}L`);
      }
    } catch (error) {
      console.error(`❌ Error checking fuel fill for ${plate}:`, error.message);
    }
  }

  async completeFuelFillWatcher(plate) {
    try {
      const watcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (!watcher) return;
      
      const watcherType = watcher.watcher_type || 'FILL';
      
      if (watcherType === 'FILL') {
        // Complete fuel fill
        const stabilizedFuel = watcher.highest_loc_time
          ? this.findConfirmedStableFuelAfter(
              plate,
              watcher.highest_loc_time,
              0,
              FUEL_STABILITY_CONFIG.fillSettleWindowMs
            ) || this.findStabilizedFuelAfter(plate, watcher.highest_loc_time, 0, FUEL_STABILITY_CONFIG.fillSettleWindowMs)
          : null;
        const closingFuel = stabilizedFuel?.combined_fuel_volume_in_tank ?? watcher.highest_fuel;
        const closingPercentage = stabilizedFuel?.combined_fuel_percentage ?? watcher.highest_percentage;
        const endTime = stabilizedFuel?.locTime
          ? this.convertLocTime(stabilizedFuel.locTime)
          : watcher.highest_loc_time
            ? this.convertLocTime(watcher.highest_loc_time)
            : new Date().toISOString();
        const fillAmount = Math.max(0, closingFuel - watcher.opening_fuel);
        if (fillAmount < 1) {
          console.log(`[fill] Skipping fill completion for ${plate} - change too small after stabilization (${fillAmount.toFixed(1)}L)`);
          pendingFuelDb.deleteFuelFillWatcher(plate);
          return;
        }
        const startTime = new Date(watcher.start_time).getTime();
        const endTimeMs = new Date(endTime).getTime();
        const duration = (endTimeMs - startTime) / 1000;
        
        const { error: fillInsertError } = await supabase.from('energy_rite_operating_sessions').insert({
          branch: plate,
          company: 'WATERFORD',
          session_date: watcher.start_time.split('T')[0],
          session_start_time: watcher.start_time,
          session_end_time: endTime,
          operating_hours: duration / 3600,
          ...this.buildSessionFuelFields('opening', {
            combined_fuel_volume_in_tank: watcher.opening_fuel,
            combined_fuel_percentage: watcher.opening_percentage,
            fuel_probe_1_volume_in_tank: watcher.opening_fuel_probe_1,
            fuel_probe_2_volume_in_tank: watcher.opening_fuel_probe_2,
            fuel_probe_1_level_percentage: watcher.opening_percentage_probe_1,
            fuel_probe_2_level_percentage: watcher.opening_percentage_probe_2
          }),
          ...this.buildSessionFuelFields('closing', stabilizedFuel || {
            combined_fuel_volume_in_tank: closingFuel,
            combined_fuel_percentage: closingPercentage,
            fuel_probe_1_volume_in_tank: watcher.highest_fuel_probe_1,
            fuel_probe_2_volume_in_tank: watcher.highest_fuel_probe_2,
            fuel_probe_1_level_percentage: watcher.highest_percentage_probe_1,
            fuel_probe_2_level_percentage: watcher.highest_percentage_probe_2
          }),
          total_fill: fillAmount,
          session_status: 'FUEL_FILL_COMPLETED',
          notes: `Fuel fill completed. Duration: ${duration.toFixed(1)}s, Opening: ${watcher.opening_fuel}L, Stable closing: ${closingFuel}L, Peak seen: ${watcher.highest_fuel}L, Filled: ${fillAmount.toFixed(1)}L`
        });

        if (fillInsertError) {
          throw new Error(`Failed to insert completed fill session: ${fillInsertError.message}`);
        }
        
        console.log(`⛽ FILL COMPLETE: ${plate} - ${watcher.opening_fuel}L → ${watcher.highest_fuel}L = +${fillAmount.toFixed(1)}L`);
        
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
          const usageBeforeFill = Math.max(0, session.opening_fuel - watcher.opening_fuel);
          
          await supabase
            .from('energy_rite_operating_sessions')
            .update({
              total_usage: (session.total_usage || 0) + usageBeforeFill,
              ...this.buildSessionFuelFields('opening', stabilizedFuel || {
                combined_fuel_volume_in_tank: closingFuel,
                combined_fuel_percentage: closingPercentage,
                fuel_probe_1_volume_in_tank: watcher.highest_fuel_probe_1,
                fuel_probe_2_volume_in_tank: watcher.highest_fuel_probe_2,
                fuel_probe_1_level_percentage: watcher.highest_percentage_probe_1,
                fuel_probe_2_level_percentage: watcher.highest_percentage_probe_2
              }),
              fill_events: (session.fill_events || 0) + 1,
              fill_amount_during_session: (session.fill_amount_during_session || 0) + fillAmount,
              total_fill: (session.total_fill || 0) + fillAmount,
              notes: `${session.notes || ''} | Fill: +${fillAmount.toFixed(1)}L`
            })
            .eq('id', session.id);
        }
      } else if (watcherType === 'THEFT') {
        // Complete fuel theft
        const stabilizedFuel = watcher.lowest_loc_time
          ? this.findConfirmedStableFuelAfter(
              plate,
              watcher.lowest_loc_time,
              0,
              FUEL_STABILITY_CONFIG.fillSettleWindowMs
            ) || this.findStabilizedFuelAfter(plate, watcher.lowest_loc_time, 0, FUEL_STABILITY_CONFIG.fillSettleWindowMs)
          : null;
        const closingFuel = stabilizedFuel?.combined_fuel_volume_in_tank ?? watcher.lowest_fuel;
        const closingPercentage = stabilizedFuel?.combined_fuel_percentage ?? watcher.lowest_percentage;
        const theftAmount = Math.max(0, watcher.opening_fuel - closingFuel);
        if (theftAmount < 1) {
          console.log(`[theft] Skipping theft completion for ${plate} - change too small after stabilization (${theftAmount.toFixed(1)}L)`);
          pendingFuelDb.deleteFuelFillWatcher(plate);
          return;
        }
        const endTime = stabilizedFuel?.locTime
          ? this.convertLocTime(stabilizedFuel.locTime)
          : watcher.lowest_loc_time ? this.convertLocTime(watcher.lowest_loc_time) : new Date().toISOString();
        const startTime = new Date(watcher.start_time).getTime();
        const endTimeMs = new Date(endTime).getTime();
        const duration = (endTimeMs - startTime) / 1000;
        
        const { error: theftInsertError } = await supabase.from('energy_rite_operating_sessions').insert({
          branch: plate,
          company: 'WATERFORD',
          session_date: watcher.start_time.split('T')[0],
          session_start_time: watcher.start_time,
          session_end_time: endTime,
          operating_hours: duration / 3600,
          ...this.buildSessionFuelFields('opening', {
            combined_fuel_volume_in_tank: watcher.opening_fuel,
            combined_fuel_percentage: watcher.opening_percentage,
            fuel_probe_1_volume_in_tank: watcher.opening_fuel_probe_1,
            fuel_probe_2_volume_in_tank: watcher.opening_fuel_probe_2,
            fuel_probe_1_level_percentage: watcher.opening_percentage_probe_1,
            fuel_probe_2_level_percentage: watcher.opening_percentage_probe_2
          }),
          ...this.buildSessionFuelFields('closing', stabilizedFuel || {
            combined_fuel_volume_in_tank: closingFuel,
            combined_fuel_percentage: closingPercentage,
            fuel_probe_1_volume_in_tank: watcher.lowest_fuel_probe_1,
            fuel_probe_2_volume_in_tank: watcher.lowest_fuel_probe_2,
            fuel_probe_1_level_percentage: watcher.lowest_percentage_probe_1,
            fuel_probe_2_level_percentage: watcher.lowest_percentage_probe_2
          }),
          total_fill: -theftAmount,
          session_status: 'FUEL_THEFT_COMPLETED',
          notes: `Fuel theft completed. Duration: ${duration.toFixed(1)}s, Opening: ${watcher.opening_fuel}L, Stable closing: ${closingFuel}L, Lowest seen: ${watcher.lowest_fuel}L, Lost: ${theftAmount.toFixed(1)}L`
        });

        if (theftInsertError) {
          throw new Error(`Failed to insert completed theft session: ${theftInsertError.message}`);
        }
        
        console.log(`🚨 THEFT COMPLETE: ${plate} - ${watcher.opening_fuel}L → ${watcher.lowest_fuel}L = -${theftAmount.toFixed(1)}L`);
        
        // Record in anomalies table
        await supabase.from('energy_rite_fuel_anomalies').insert({
          plate: plate,
          anomaly_type: 'FUEL_THEFT',
          anomaly_date: endTime,
          fuel_before: watcher.opening_fuel,
          fuel_after: closingFuel,
          difference: -theftAmount,
          severity: theftAmount >= 100 ? 'CRITICAL' : theftAmount >= 50 ? 'HIGH' : 'MEDIUM',
          status: 'confirmed',
          anomaly_data: {
            detection_method: 'AUTOMATIC',
            duration_seconds: duration
          }
        });
      }
      
      pendingFuelDb.deleteFuelFillWatcher(plate);
    } catch (error) {
      console.error(`❌ Error completing watcher for ${plate}:`, error.message);
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
        fuel_probe_2_level: parseFloat(vehicleData.fuel_probe_2_level) || 0,
        fuel_probe_2_level_percentage: parseFloat(vehicleData.fuel_probe_2_level_percentage) || 0,
        fuel_probe_2_volume_in_tank: parseFloat(vehicleData.fuel_probe_2_volume_in_tank) || 0,
        fuel_probe_2_temperature: parseFloat(vehicleData.fuel_probe_2_temperature) || 0,
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
        let currentFuelFields = {};
        
        if (vehicleData?.fuel_probe_1_volume_in_tank || vehicleData?.fuel_probe_2_volume_in_tank) {
          currentFuelFields = this.buildSessionFuelFields('closing', this.parseFuelData(vehicleData));
          currentFuel = currentFuelFields.closing_fuel || 0;
          currentPercentage = currentFuelFields.closing_percentage || 0;
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
            ...currentFuelFields,
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
    
    // Priority detection: ENGINE, IGNITION, and PTO status
    if (normalized.includes('ENGINE ON') || 
        normalized.includes('IGNITION ON') || 
        normalized.includes('PTO ON')) {
      console.log(`🟢 ENGINE/IGNITION ON DETECTED: ${driverName}`);
      return 'ON';
    }
    if (normalized.includes('ENGINE OFF') || 
        normalized.includes('IGNITION OFF') || 
        normalized.includes('PTO OFF')) {
      console.log(`🔴 ENGINE/IGNITION OFF DETECTED: ${driverName}`);
      return 'OFF';
    }
    
    console.log(`ℹ️ No engine/ignition status detected in: "${driverName}"`);
    return null;
  }

  parseFuelFillStatus(driverName) {
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    console.log(`🔍 Checking fuel fill status: "${driverName}" -> "${normalized}"`);
    
    // Only trigger fills when status explicitly says "Possible Fuel Fill"
    if (normalized.includes('POSSIBLE FUEL FILL')) {
      console.log(`⛽ POSSIBLE FUEL FILL DETECTED: ${driverName}`);
      return 'START';
    }
    
    console.log(`ℹ️ No fuel fill status detected in: "${driverName}"`);
    return null;
  }

  parseFuelTheftStatus(driverName) {
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    
    if (normalized.includes('POSSIBLE FUEL THEFT') ||
        normalized.includes('FUEL THEFT')) {
      console.log(`🚨 FUEL THEFT STATUS DETECTED: ${driverName}`);
      return 'START';
    }
    
    return null;
  }

  async handleFuelTheftStart(plate, vehicleData) {
    try {
      const currentTime = this.convertLocTime(vehicleData.LocTime);
      const currentFuel = parseFloat(vehicleData.fuel_probe_1_volume_in_tank) || 0;
      const currentPercentage = parseFloat(vehicleData.fuel_probe_1_level_percentage) || 0;
      
      // Find highest fuel before theft (last 30 minutes) - like finding lowest for fills
      const currentTimestamp = new Date(currentTime).getTime();
      const thirtyMinutesAgo = currentTimestamp - (30 * 60 * 1000);
      
      let highestFuel = currentFuel;
      let highestPercentage = currentPercentage;
      let highestLocTime = vehicleData.LocTime;
      
      const fuelHistory = this.recentFuelData.get(plate);
      if (fuelHistory && fuelHistory.length > 0) {
        for (const entry of fuelHistory) {
          if (entry.timestamp >= thirtyMinutesAgo && entry.timestamp < currentTimestamp) {
            if (entry.fuel_probe_1_volume_in_tank > highestFuel) {
              highestFuel = entry.fuel_probe_1_volume_in_tank;
              highestPercentage = entry.fuel_probe_1_level_percentage;
              highestLocTime = entry.locTime;
            }
          }
        }
      }
      
      const theftAmount = Math.max(0, highestFuel - currentFuel);
      const startTime = this.convertLocTime(highestLocTime);
      const endTime = currentTime;
      const startTimeMs = new Date(startTime).getTime();
      const endTimeMs = new Date(endTime).getTime();
      const duration = (endTimeMs - startTimeMs) / 1000;
      
      console.log(`🚨 FUEL THEFT: ${plate} - ${highestFuel}L → ${currentFuel}L = -${theftAmount.toFixed(1)}L`);
      
      // Record as session (exactly like fuel fill but measuring loss)
      await supabase.from('energy_rite_operating_sessions').insert({
        branch: plate,
        company: 'WATERFORD',
        session_date: startTime.split('T')[0],
        session_start_time: startTime,
        session_end_time: endTime,
        operating_hours: duration / 3600,
        ...this.buildSessionFuelFields('opening', {
          fuel_probe_1_volume_in_tank: highestFuel,
          fuel_probe_1_level_percentage: highestPercentage
        }),
        ...this.buildSessionFuelFields('closing', this.parseFuelData(vehicleData)),
        total_fill: -theftAmount, // Negative for theft (like fills but negative)
        session_status: 'FUEL_THEFT_COMPLETED',
        notes: `Fuel theft completed. Duration: ${duration.toFixed(1)}s, Opening: ${highestFuel}L, Closing: ${currentFuel}L, Lost: ${theftAmount.toFixed(1)}L`
      });
      
      console.log(`🚨 FUEL THEFT COMPLETE: ${plate} - ${highestFuel}L → ${currentFuel}L = -${theftAmount.toFixed(1)}L`);
      
      // Also record in anomalies table for alerts
      await supabase.from('energy_rite_fuel_anomalies').insert({
        plate: plate,
        anomaly_type: 'FUEL_THEFT',
        anomaly_date: currentTime,
        fuel_before: highestFuel,
        fuel_after: currentFuel,
        difference: -theftAmount,
        severity: theftAmount >= 100 ? 'CRITICAL' : theftAmount >= 50 ? 'HIGH' : 'MEDIUM',
        status: 'confirmed',
        anomaly_data: {
          detection_method: 'STATUS_MESSAGE',
          status_message: vehicleData.DriverName,
          duration_seconds: duration
        }
      });
      
    } catch (error) {
      console.error(`❌ Error handling fuel theft for ${plate}:`, error.message);
    }
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
      const openingFuelFields = this.buildSessionFuelFields('opening', this.parseFuelData(vehicleData));
      
      if (fuelFillStatus === 'START') {
        // Check if fuel fill session already exists
        const { data: existing } = await supabase
          .from('energy_rite_operating_sessions')
          .select('id')
          .eq('branch', plate)
          .eq('session_status', 'FUEL_FILL_ONGOING')
          .limit(1);
          
        if (!existing || existing.length === 0) {
          const openingFuel = openingFuelFields.opening_fuel || 0;
          const openingPercentage = openingFuelFields.opening_percentage || 0;
          
          await supabase.from('energy_rite_operating_sessions').insert({
            branch: plate,
            company: 'WATERFORD',
            session_date: currentTime.split('T')[0],
            session_start_time: currentTime,
            ...openingFuelFields,
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
    const maxWindow = 5 * 60 * 1000; // 5 minutes in LocTime
    
    let nextFuel = null;
    let minDiff = Infinity;
    
    // First check in-memory cache so fresh runtime data wins over older persisted rows.
    const fuelHistory = this.recentFuelData.get(plate);
    if (fuelHistory && fuelHistory.length > 0) {
      for (const fuelData of fuelHistory) {
        const diff = fuelData.timestamp - targetTime;
        if (diff > 0 && diff <= maxWindow && diff < minDiff) {
          minDiff = diff;
          nextFuel = fuelData;
        }
      }
    }
    
    if (!nextFuel) {
      const dbHistory = pendingFuelDb.getFuelHistoryAfter(plate, targetTime, 50);
      for (const entry of dbHistory) {
        const diff = entry.timestamp - targetTime;
        if (diff > 0 && diff <= maxWindow && diff < minDiff) {
          minDiff = diff;
          nextFuel = {
            fuel_probe_1_volume_in_tank: entry.fuel_volume_probe_1 ?? entry.fuel_volume,
            fuel_probe_2_volume_in_tank: entry.fuel_volume_probe_2 ?? 0,
            fuel_probe_1_level_percentage: entry.fuel_percentage_probe_1 ?? entry.fuel_percentage,
            fuel_probe_2_level_percentage: entry.fuel_percentage_probe_2 ?? 0,
            combined_fuel_volume_in_tank: entry.fuel_volume,
            combined_fuel_percentage: entry.fuel_percentage,
            locTime: entry.loc_time,
            timestamp: entry.timestamp
          };
        }
      }
    }
    
    if (nextFuel) {
      console.log(`[capture] Found next fuel data for ${plate}: ${(minDiff / 1000).toFixed(0)}s after status (LocTime-based)`);
      return nextFuel;
    }
    
    console.log(`[capture] No fuel data found within 30-min LocTime window after status for ${plate}, will retry when more data arrives`);
    return null;
  }

  findClosestFuelDataBefore(plate, targetLocTime) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const maxWindow = 5 * 60 * 1000; // 5 minutes in LocTime
    
    let closestFuel = null;
    let minDiff = Infinity;
    
    // First check in-memory cache so fresh runtime data wins over older persisted rows.
    const fuelHistory = this.recentFuelData.get(plate);
    if (fuelHistory && fuelHistory.length > 0) {
      for (const fuelData of fuelHistory) {
        const diff = targetTime - fuelData.timestamp;
        if (diff > 0 && diff <= maxWindow && diff < minDiff) {
          minDiff = diff;
          closestFuel = fuelData;
        }
      }
    }
    
    if (!closestFuel) {
      const dbHistory = pendingFuelDb.getFuelHistoryBefore(plate, targetTime, 50);
      for (const entry of dbHistory) {
        const diff = targetTime - entry.timestamp;
        if (diff > 0 && diff <= maxWindow && diff < minDiff) {
          minDiff = diff;
          closestFuel = {
            fuel_probe_1_volume_in_tank: entry.fuel_volume_probe_1 ?? entry.fuel_volume,
            fuel_probe_2_volume_in_tank: entry.fuel_volume_probe_2 ?? 0,
            fuel_probe_1_level_percentage: entry.fuel_percentage_probe_1 ?? entry.fuel_percentage,
            fuel_probe_2_level_percentage: entry.fuel_percentage_probe_2 ?? 0,
            combined_fuel_volume_in_tank: entry.fuel_volume,
            combined_fuel_percentage: entry.fuel_percentage,
            locTime: entry.loc_time,
            timestamp: entry.timestamp
          };
        }
      }
    }
    
    if (closestFuel) {
      console.log(`[capture] Found fuel data BEFORE ${plate}: ${closestFuel.fuel_probe_1_volume_in_tank}L at ${(minDiff / 1000).toFixed(0)}s before status (LocTime-based)`);
      return closestFuel;
    }
    
    console.log(`[capture] No fuel data found within 30-min LocTime window before status for ${plate}, will retry when more data arrives`);
    return null;
  }
  normalizeFuelHistoryEntry(entry) {
    if (!entry) return null;

    const probe1Volume = this.parseNumericFuelValue(entry.fuel_probe_1_volume_in_tank ?? entry.fuel_volume_probe_1 ?? entry.fuel_volume);
    const probe2Volume = this.parseNumericFuelValue(entry.fuel_probe_2_volume_in_tank ?? entry.fuel_volume_probe_2);
    const probe1Percentage = this.parseNumericFuelValue(entry.fuel_probe_1_level_percentage ?? entry.fuel_percentage_probe_1 ?? entry.fuel_percentage);
    const probe2Percentage = this.parseNumericFuelValue(entry.fuel_probe_2_level_percentage ?? entry.fuel_percentage_probe_2);
    const locTime = entry.locTime ?? entry.loc_time;
    const timestamp = entry.timestamp;

    if (timestamp === undefined || timestamp === null) {
      return null;
    }

    const percentageValues = [];
    if (this.hasFuelValue(entry.fuel_probe_1_level_percentage ?? entry.fuel_percentage_probe_1 ?? entry.fuel_percentage)) percentageValues.push(probe1Percentage);
    if (this.hasFuelValue(entry.fuel_probe_2_level_percentage ?? entry.fuel_percentage_probe_2)) percentageValues.push(probe2Percentage);

    const combinedFuel = this.hasFuelValue(entry.combined_fuel_volume_in_tank ?? entry.fuel_volume)
      ? this.parseNumericFuelValue(entry.combined_fuel_volume_in_tank ?? entry.fuel_volume)
      : probe1Volume + probe2Volume;

    const combinedPercentage = this.hasFuelValue(entry.combined_fuel_percentage ?? entry.fuel_percentage)
      ? this.parseNumericFuelValue(entry.combined_fuel_percentage ?? entry.fuel_percentage)
      : (percentageValues.length > 0
        ? percentageValues.reduce((sum, value) => sum + value, 0) / percentageValues.length
        : 0);

    return {
      fuel_probe_1_volume_in_tank: probe1Volume,
      fuel_probe_1_level_percentage: probe1Percentage,
      fuel_probe_2_volume_in_tank: probe2Volume,
      fuel_probe_2_level_percentage: probe2Percentage,
      combined_fuel_volume_in_tank: combinedFuel,
      combined_fuel_percentage: combinedPercentage,
      locTime,
      timestamp
    };
  }

  getFuelHistoryRange(plate, startTimestamp, endTimestamp) {
    const combined = [];
    const seen = new Set();

    const dbHistory = pendingFuelDb.getFuelHistoryInRange(plate, startTimestamp, endTimestamp) || [];
    for (const entry of dbHistory) {
      const normalized = this.normalizeFuelHistoryEntry(entry);
      if (!normalized) continue;

      const key = `${normalized.timestamp}:${normalized.locTime || ''}`;
      if (seen.has(key)) continue;

      seen.add(key);
      combined.push(normalized);
    }

    const memoryHistory = this.recentFuelData.get(plate) || [];
    for (const entry of memoryHistory) {
      if (entry.timestamp < startTimestamp || entry.timestamp > endTimestamp) continue;

      const normalized = this.normalizeFuelHistoryEntry(entry);
      if (!normalized) continue;

      const key = `${normalized.timestamp}:${normalized.locTime || ''}`;
      if (seen.has(key)) continue;

      seen.add(key);
      combined.push(normalized);
    }

    combined.sort((a, b) => a.timestamp - b.timestamp);
    return combined;
  }

  getMedianFuelReading(readings) {
    if (!readings || readings.length === 0) return null;

    const sample = readings.slice(-FUEL_STABILITY_CONFIG.medianSampleSize);
    const sortedByFuel = [...sample].sort(
      (a, b) => a.combined_fuel_volume_in_tank - b.combined_fuel_volume_in_tank
    );
    const median = sortedByFuel[Math.floor(sortedByFuel.length / 2)];

    return sample.reduce((closest, entry) => {
      if (!closest) return entry;

      const currentDiff = Math.abs(entry.combined_fuel_volume_in_tank - median.combined_fuel_volume_in_tank);
      const closestDiff = Math.abs(closest.combined_fuel_volume_in_tank - median.combined_fuel_volume_in_tank);

      if (currentDiff < closestDiff) return entry;
      if (currentDiff === closestDiff && entry.timestamp > closest.timestamp) return entry;
      return closest;
    }, null);
  }

  getFuelSpread(readings) {
    if (!readings || readings.length === 0) return Infinity;

    let minFuel = Infinity;
    let maxFuel = -Infinity;

    for (const entry of readings) {
      minFuel = Math.min(minFuel, entry.combined_fuel_volume_in_tank);
      maxFuel = Math.max(maxFuel, entry.combined_fuel_volume_in_tank);
    }

    return maxFuel - minFuel;
  }

  findConfirmedStableFuelAfter(
    plate,
    targetLocTime,
    settleDelayMs,
    windowMs,
    minSamples = FUEL_STABILITY_CONFIG.minStableSamples,
    maxSpreadLiters = FUEL_STABILITY_CONFIG.maxStableSpreadLiters
  ) {
    const readings = this.getFuelHistoryRange(
      plate,
      new Date(this.convertLocTime(targetLocTime)).getTime() + settleDelayMs,
      new Date(this.convertLocTime(targetLocTime)).getTime() + settleDelayMs + windowMs
    );

    if (readings.length < minSamples) {
      return null;
    }

    const sample = readings.slice(-Math.max(minSamples, FUEL_STABILITY_CONFIG.medianSampleSize));
    const spread = this.getFuelSpread(sample);
    if (spread > maxSpreadLiters) {
      console.log(`⏳ Stable fuel not confirmed for ${plate}: spread ${spread.toFixed(1)}L across ${sample.length} samples`);
      return null;
    }

    const stableReading = this.getMedianFuelReading(sample);
    if (stableReading) {
      console.log(`🎯 Confirmed stable fuel AFTER ${plate}: ${stableReading.combined_fuel_volume_in_tank}L from ${sample.length} samples (spread ${spread.toFixed(1)}L)`);
    }
    return stableReading;
  }

  findStabilizedFuelBefore(plate, targetLocTime, lookbackMs = FUEL_STABILITY_CONFIG.engineOnLookbackMs) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const readings = this.getFuelHistoryRange(plate, targetTime - lookbackMs, targetTime - 1);

    if (readings.length === 0) {
      return null;
    }

    const stabilized = this.getMedianFuelReading(readings);
    if (stabilized) {
      console.log(`ðŸŽ¯ Stabilized fuel BEFORE ${plate}: ${stabilized.combined_fuel_volume_in_tank}L from ${readings.length} samples`);
    }
    return stabilized;
  }

  findStabilizedFuelAfter(
    plate,
    targetLocTime,
    settleDelayMs = FUEL_STABILITY_CONFIG.engineOffSettleDelayMs,
    windowMs = FUEL_STABILITY_CONFIG.engineOffWindowMs
  ) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const startTime = targetTime + settleDelayMs;
    const endTime = startTime + windowMs;
    const readings = this.getFuelHistoryRange(plate, startTime, endTime);

    if (readings.length === 0) {
      return null;
    }

    const stabilized = this.getMedianFuelReading(readings);
    if (stabilized) {
      console.log(`ðŸŽ¯ Stabilized fuel AFTER ${plate}: ${stabilized.combined_fuel_volume_in_tank}L from ${readings.length} samples`);
    }
    return stabilized;
  }

  async handleSessionChange(plate, engineStatus, wsMessage = null) {
    try {
      const currentTime = this.convertLocTime(wsMessage?.LocTime);
      
      if (engineStatus === 'ON') {
        if (pendingFuelDb.getFuelFillWatcher(plate)) {
          console.log(`[session] Completing active off-engine watcher before ENGINE ON for ${plate}`);
          await this.completeFuelFillWatcher(plate);
        }
        const { data: existing } = await supabase
          .from('energy_rite_operating_sessions')
          .select('id')
          .eq('branch', plate)
          .eq('session_status', 'ONGOING')
          .limit(1);
          
        if (!existing || existing.length === 0) {
          const openingFuel = this.findFuelSnapshotAtOrBefore(plate, wsMessage.LocTime, wsMessage);

          if (openingFuel) {
            await supabase.from('energy_rite_operating_sessions').insert({
              branch: plate,
              company: 'WATERFORD',
              session_date: currentTime.split('T')[0],
              session_start_time: currentTime,
              ...this.buildSessionFuelFields('opening', openingFuel),
              session_status: 'ONGOING',
              notes: `Engine started. Opening: ${openingFuel.combined_fuel_volume_in_tank}L (${openingFuel.combined_fuel_percentage}%) [status-time capture]`
            });
            
            console.log(`[session] ENGINE ON: ${plate} - Opening: ${openingFuel.combined_fuel_volume_in_tank}L (${openingFuel.combined_fuel_percentage}%)`);
          } else {
            this.pendingFuelUpdates.set(plate, {
              sessionId: null,
              statusLocTime: wsMessage.LocTime,
              timestamp: Date.now(),
              needsSessionCreation: true
            });
            console.log(`[session] ENGINE ON: ${plate} - Waiting for earlier fuel data around status LocTime`);
          }
        } else {
          console.log(`[session] Engine session already exists for ${plate}`);
        }
      } else if (engineStatus === 'OFF') {
        const { data: sessions } = await supabase
          .from('energy_rite_operating_sessions')
          .select('*')
          .eq('branch', plate)
          .eq('session_status', 'ONGOING')
          .order('session_start_time', { ascending: false })
          .limit(1);
          
        if (sessions && Array.isArray(sessions) && sessions.length > 0) {
          const session = sessions[0];
          const closingFuel = this.findFuelSnapshotAtOrAfter(plate, wsMessage.LocTime, wsMessage);

          if (closingFuel) {
            await this.completeSession(session, currentTime, closingFuel);
            this.pendingClosures.delete(plate);
            this.pendingFuelUpdates.delete(plate);
            console.log(`[session] ENGINE OFF: ${plate} - Closing captured at status time or next LocTime packet`);
          } else {
            this.pendingClosures.set(plate, { 
              sessionId: session.id, 
              endTime: currentTime,
              statusLocTime: wsMessage.LocTime,
              timestamp: Date.now()
            });
            console.log(`[session] Engine OFF for ${plate} - Waiting for later fuel data around status LocTime`);
          }
        }
      }
      
      await supabase.from('energy_rite_activity_log').insert({
        branch: plate,
        activity_type: engineStatus === 'ON' ? 'ENGINE_ON' : 'ENGINE_OFF',
        activity_time: currentTime,
        notes: `Engine ${engineStatus} detected`
      });
      
    } catch (error) {
      console.error(`[session] Error handling session change for ${plate}:`, error.message);
    }
  }
  async handleFuelFillEvent(plate, vehicleData) {
    try {
      const currentTime = vehicleData.LocTime ? new Date(vehicleData.LocTime) : new Date();
      const parsedFuel = this.parseFuelData(vehicleData);
      const preFillLevel = parsedFuel.combined_fuel_level || 0;
      const preFillPercentage = parsedFuel.combined_fuel_percentage || 0;
      
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
      
      const currentFuelData = this.parseFuelData(vehicleData);
      const currentFuelLevel = currentFuelData.combined_fuel_level || 0;
      const currentFuelPercentage = currentFuelData.combined_fuel_percentage || 0;
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
          ...this.buildSessionFuelFields('opening', {
            combined_fuel_level: tracking.preFillLevel,
            combined_fuel_percentage: tracking.preFillPercentage
          }),
          ...this.buildSessionFuelFields('closing', currentFuelData),
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
      const closingFuelFields = this.buildSessionFuelFields('closing', vehicleData);
      const closingFuel = closingFuelFields.closing_fuel || 0;
      const closingPercentage = closingFuelFields.closing_percentage || 0;
      const closingVolume = closingFuel;
      const closingTemperature = this.parseNumericFuelValue(vehicleData.combined_fuel_temperature);
      
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
          ...closingFuelFields,
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
      const closestFuel = this.findFuelSnapshotAtOrAfter(plate, pending.statusLocTime, vehicleData);
      if (!closestFuel) {
        console.log(`[session] Waiting for fuel data closer to ENGINE OFF time for ${plate}`);
        return;
      }
      
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
      console.error(`[session] Error updating pending closure for ${plate}:`, error.message);
    }
  }

  async updatePendingSessionFuel(plate, vehicleData) {
    try {
      if (!this.pendingFuelUpdates.has(plate)) return;
      
      const pending = this.pendingFuelUpdates.get(plate);
      const closestFuel = this.findFuelSnapshotAtOrBefore(plate, pending.statusLocTime, vehicleData);
      
      if (!closestFuel) {
        console.log(`[session] Waiting for fuel data BEFORE ENGINE ON time for ${plate}`);
        return;
      }
      
      if (pending.needsSessionCreation) {
        const currentTime = this.convertLocTime(pending.statusLocTime);
        await supabase.from('energy_rite_operating_sessions').insert({
          branch: plate,
          company: 'WATERFORD',
          session_date: currentTime.split('T')[0],
          session_start_time: currentTime,
          ...this.buildSessionFuelFields('opening', closestFuel),
          session_status: 'ONGOING',
          notes: `Engine started. Opening: ${closestFuel.combined_fuel_volume_in_tank}L (${closestFuel.combined_fuel_percentage}%) [earlier LocTime fallback]`
        });
        console.log(`[session] Created session for ${plate} with fuel BEFORE ON: ${closestFuel.combined_fuel_volume_in_tank}L`);
      } else if (pending.sessionId) {
        await supabase.from('energy_rite_operating_sessions')
          .update({
            ...this.buildSessionFuelFields('opening', closestFuel),
            notes: `Engine started. Opening: ${closestFuel.combined_fuel_volume_in_tank}L (${closestFuel.combined_fuel_percentage}%) [earlier LocTime fallback]`
          })
          .eq('id', pending.sessionId);
        console.log(`[session] Updated pending session for ${plate} with fuel BEFORE ON: ${closestFuel.combined_fuel_volume_in_tank}L`);
      }
      
      this.pendingFuelUpdates.delete(plate);
      
    } catch (error) {
      console.error(`[session] Error updating pending session fuel for ${plate}:`, error.message);
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












