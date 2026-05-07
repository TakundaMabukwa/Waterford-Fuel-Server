const WebSocket = require('ws');
const { supabase } = require('./supabase-client');
const postgresMessageStore = require('./postgres-message-store');
const pendingFuelDb = require('./pending-fuel-db');

const FUEL_STABILITY_CONFIG = {
  engineOnLookbackMs: 2 * 60 * 1000,
  engineOnFallbackMs: 2 * 60 * 1000,
  engineOffSettleDelayMs: 30 * 1000,
  engineOffWindowMs: 3 * 60 * 1000,
  fillStartLookbackMs: 15 * 60 * 1000,
  fillSettleWindowMs: 2 * 60 * 1000,
  medianSampleSize: 5,
  minStableSamples: 2,
  maxStableSpreadLiters: 3
};

const ENGINE_STATUS_LOOKBACK_MS = (() => {
  const parsed = parseFloat(process.env.FUEL_ENGINE_STATUS_LOOKBACK_MS || '');
  return Number.isFinite(parsed) ? parsed : 30 * 60 * 1000;
})();

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
    this.lastEngineStatusByPlate = new Map();
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
      await postgresMessageStore.init();
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
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â½ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¯ ${watcherType} STABILIZED: ${watcher.plate} - No change for ${(timeSinceLastChange/1000).toFixed(0)}s`);
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
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â° Completing expired watcher for ${watcher.plate} on startup`);
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
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â½ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¯ Completing stabilized ${watcherType} watcher for ${watcher.plate} on startup`);
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
      console.log('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â§ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âª Test mode - skipping WebSocket connection');
      return;
    }
    
    // Initialize SQLite database first
    this.initDb().then(() => {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Connecting to WebSocket: ${this.wsUrl}`);
      this.ws = new WebSocket(this.wsUrl);

      this.ws.on('open', () => {
        console.log('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ WebSocket connected');
        this.reconnectAttempts = 0;
      });

      this.ws.on('message', async (data) => {
        try {
          const message = JSON.parse(data);
          console.log('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨ RAW MESSAGE:', JSON.stringify(message, null, 2));
          
          if (message.Plate && message.LocTime) {
            this.addToQueue(message);
          }
        } catch (error) {
          console.error('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error parsing message:', error);
        }
      });

      this.ws.on('close', () => {
        console.log('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ WebSocket disconnected');
        this.reconnect();
      });

      this.ws.on('error', (error) => {
        console.error('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ WebSocket error:', error);
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
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ Sorted ${batch.length} messages`);
    }
    
    // Process without awaiting each (parallel where safe)
    for (const message of batch) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¹ [${message.Plate}] ${message.LocTime}`);
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
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â Processing ${actualBranch}:`, {
        DriverName: vehicleData.DriverName,
        fuel_probe_1_level: vehicleData.fuel_probe_1_level,
        fuel_probe_1_volume_in_tank: vehicleData.fuel_probe_1_volume_in_tank,
        Speed: vehicleData.Speed,
        Temperature: vehicleData.Temperature
      });
      
      // Parse fuel data from Temperature field if present
      const fuelData = this.parseFuelData(vehicleData);
      const speed = parseFloat(vehicleData.Speed) || 0;
      const normalizedLocTime = this.convertLocTime(vehicleData.LocTime);

      // Persist every raw message to Postgres first. Session opening/closing values
      // are sourced from this table only.
      await postgresMessageStore.storeMessage(vehicleData, fuelData, normalizedLocTime);
      
      // Store fuel data if available
      if (fuelData.hasFuelData) {
        await this.storeFuelData({ ...vehicleData, actualBranch, ...fuelData });
        
        const fuelTimestamp = new Date(normalizedLocTime).getTime();
        
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
        this.updateLastEngineStatus(actualBranch, engineStatus, vehicleData.LocTime, 'stream', vehicleData.DriverName);
        console.log(`[engine] Processing ${engineStatus} for ${actualBranch}`);
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
      console.error('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error processing vehicle update:', error);
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

    if (this.hasNumericFuelValue(vehicleData.fuel_probe_1_level_percentage)) percentageValues.push(fuelProbe1Percentage);
    if (this.hasNumericFuelValue(vehicleData.fuel_probe_2_level_percentage)) percentageValues.push(fuelProbe2Percentage);
    if (this.hasNumericFuelValue(vehicleData.fuel_probe_1_temperature)) temperatureValues.push(fuelProbe1Temperature);
    if (this.hasNumericFuelValue(vehicleData.fuel_probe_2_temperature)) temperatureValues.push(fuelProbe2Temperature);

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

  hasNumericFuelValue(value) {
    if (!this.hasFuelValue(value)) {
      return false;
    }

    const parsed = parseFloat(value);
    return Number.isFinite(parsed);
  }

  hasFuelValue(value) {
    return value !== undefined && value !== null && String(value).trim() !== '';
  }

  hasExplicitFuelFields(vehicleData) {
    if (!vehicleData) return false;

    // Require at least one numeric level/volume reading before we treat a packet
    // as fuel-bearing for event capture. Percentage/temperature-only packets can
    // otherwise get coerced into misleading 0L values.
    const primaryFuelKeys = [
      'fuel_probe_1_level',
      'fuel_probe_1_volume_in_tank',
      'fuel_probe_2_level',
      'fuel_probe_2_volume_in_tank'
    ];

    return primaryFuelKeys.some((key) => this.hasNumericFuelValue(vehicleData[key]));
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
    if (this.hasNumericFuelValue(source.fuel_probe_1_level_percentage ?? source.fuel_percentage)) percentageValues.push(fuelProbe1Percentage);
    if (this.hasNumericFuelValue(source.fuel_probe_2_level_percentage)) percentageValues.push(fuelProbe2Percentage);
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

    if (this.hasNumericFuelValue(fuelData.fuel_probe_1_level_percentage)) percentageValues.push(percentageProbe1);
    if (this.hasNumericFuelValue(fuelData.fuel_probe_2_level_percentage)) percentageValues.push(percentageProbe2);

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

  decorateEventFuelSnapshot(snapshot, targetLocTime, direction, sourceLabel = null) {
    if (!snapshot) return null;

    const targetTimestamp = targetLocTime
      ? new Date(this.convertLocTime(targetLocTime)).getTime()
      : null;
    const snapshotTimestamp = Number.isFinite(snapshot.timestamp)
      ? snapshot.timestamp
      : snapshot.locTime
        ? new Date(this.convertLocTime(snapshot.locTime)).getTime()
        : null;

    let diffMs = snapshot.diffMs;
    if (Number.isFinite(targetTimestamp) && Number.isFinite(snapshotTimestamp)) {
      diffMs = direction === 'before'
        ? Math.max(0, targetTimestamp - snapshotTimestamp)
        : Math.max(0, snapshotTimestamp - targetTimestamp);
    }

    return {
      ...snapshot,
      source: sourceLabel || snapshot.source || 'unknown',
      diffMs
    };
  }

  buildCaptureDetails(snapshot, direction) {
    if (!snapshot) return 'unknown';
    const source = snapshot.source || 'unknown';

    if (Number.isFinite(snapshot.diffMs)) {
      return `${source}, ${(snapshot.diffMs / 1000).toFixed(0)}s ${direction}`;
    }

    return source;
  }

  async getClosestFuelSnapshot(plate, targetLocTime, direction, currentMessage = null, maxWindowMs = 5 * 60 * 1000) {
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

    const rangeStart = direction === 'before'
      ? new Date(targetTimestamp - maxWindowMs).toISOString()
      : new Date(targetTimestamp).toISOString();
    const rangeEnd = direction === 'before'
      ? new Date(targetTimestamp).toISOString()
      : new Date(targetTimestamp + maxWindowMs).toISOString();

    const postgresHistory = await postgresMessageStore.getFuelMessagesInRange(
      plate,
      rangeStart,
      rangeEnd,
      200
    );
    for (const entry of postgresHistory) {
      const normalized = this.buildFuelSnapshot(entry, entry.locTime, entry.timestamp);
      addCandidate(normalized, 'postgres');
    }

    if (direction === 'before') {
      candidates.sort((a, b) => {
        if (a.diffMs !== b.diffMs) return a.diffMs - b.diffMs;
        return b.timestamp - a.timestamp;
      });
    } else {
      candidates.sort((a, b) => {
        if (a.diffMs !== b.diffMs) return a.diffMs - b.diffMs;
        return a.timestamp - b.timestamp;
      });
    }

    if (candidates.length === 0) {
      return null;
    }

    return candidates[0];
  }

  async findFuelSnapshotAtOrBefore(plate, targetLocTime, currentMessage = null) {
    const closest = await this.getClosestFuelSnapshot(plate, targetLocTime, 'before', currentMessage, 2 * 60 * 1000);
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

  async findFuelSnapshotAtOrAfter(plate, targetLocTime, currentMessage = null) {
    const closest = await this.getClosestFuelSnapshot(plate, targetLocTime, 'after', currentMessage, 3 * 60 * 1000);
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

  async getFuelExtremaFromLastMessages(plate, targetLocTime, count = 10) {
    const targetTimestamp = new Date(this.convertLocTime(targetLocTime)).getTime();
    const beforeTimeIso = new Date(targetTimestamp + 1).toISOString();
    const messages = await postgresMessageStore.getRecentFuelMessages(plate, count, { beforeTimeIso });

    let lowest = null;
    let highest = null;

    for (const entry of messages) {
      const snapshot = this.buildFuelSnapshot(entry, entry.locTime, entry.timestamp);
      if (!snapshot) continue;
      if ((snapshot.combined_fuel_volume_in_tank || 0) <= 0) continue;

      if (!lowest || snapshot.combined_fuel_volume_in_tank < lowest.combined_fuel_volume_in_tank) {
        lowest = snapshot;
      } else if (
        lowest &&
        snapshot.combined_fuel_volume_in_tank === lowest.combined_fuel_volume_in_tank &&
        snapshot.timestamp > lowest.timestamp
      ) {
        lowest = snapshot;
      }

      if (!highest || snapshot.combined_fuel_volume_in_tank > highest.combined_fuel_volume_in_tank) {
        highest = snapshot;
      } else if (
        highest &&
        snapshot.combined_fuel_volume_in_tank === highest.combined_fuel_volume_in_tank &&
        snapshot.timestamp > highest.timestamp
      ) {
        highest = snapshot;
      }
    }

    return {
      lowest: this.decorateEventFuelSnapshot(lowest, targetLocTime, 'before', `postgres_last${count}_lowest`),
      highest: this.decorateEventFuelSnapshot(highest, targetLocTime, 'before', `postgres_last${count}_highest`)
    };
  }

  async findLowestFuelFromLastMessages(plate, targetLocTime, count = 10) {
    const extrema = await this.getFuelExtremaFromLastMessages(plate, targetLocTime, count);
    if (extrema.lowest) {
      console.log(
        `[capture] Using last-${count} LOWEST BEFORE ${plate}: ${extrema.lowest.combined_fuel_volume_in_tank}L ` +
        `(${(extrema.lowest.diffMs / 1000).toFixed(0)}s before, source: ${extrema.lowest.source})`
      );
    }
    return extrema.lowest;
  }

  async findHighestFuelFromLastMessages(plate, targetLocTime, count = 10) {
    const extrema = await this.getFuelExtremaFromLastMessages(plate, targetLocTime, count);
    if (extrema.highest) {
      console.log(
        `[capture] Using last-${count} HIGHEST BEFORE ${plate}: ${extrema.highest.combined_fuel_volume_in_tank}L ` +
        `(${(extrema.highest.diffMs / 1000).toFixed(0)}s before, source: ${extrema.highest.source})`
      );
    }
    return extrema.highest;
  }

  async findSessionClosingFuelFromLastMessages(plate, targetLocTime, count = 10) {
    const extrema = await this.getFuelExtremaFromLastMessages(plate, targetLocTime, count);
    if (!extrema.lowest && !extrema.highest) {
      return null;
    }

    // For operating session closure, use the lowest recent reading before ENGINE OFF
    // so closing reflects fuel burned by the running engine instead of a transient peak.
    const selected = extrema.lowest || extrema.highest;
    const spread = extrema.lowest && extrema.highest
      ? Math.max(0, extrema.highest.combined_fuel_volume_in_tank - extrema.lowest.combined_fuel_volume_in_tank)
      : 0;

    console.log(
      `[capture] Session close using last-${count} LOWEST BEFORE ${plate}: ${selected.combined_fuel_volume_in_tank}L ` +
      `(${(selected.diffMs / 1000).toFixed(0)}s before, spread ${spread.toFixed(1)}L, source: ${selected.source})`
    );

    return selected;
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
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error tracking pre-fill lowest for ${plate}:`, error.message);
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
        if (pendingFuelDb.getPreFillWatcher(plate)) {
          pendingFuelDb.deletePreFillWatcher(plate);
          console.log(`[prefill] Cleared pre-fill watcher for ${plate} because vehicle is moving (${speed} km/h)`);
        }
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
        if (pendingFuelDb.getPreFillWatcher(plate)) {
          pendingFuelDb.deletePreFillWatcher(plate);
          console.log(`[prefill] Cleared pre-fill watcher for ${plate} because engine is ON`);
        }
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
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â  Fill watcher: ${plate} highest now ${currentFuel}L`);
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
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â° Theft watcher: ${plate} lowest now ${currentFuel}L`);
        }
        return;
      }

      if (currentFuel > 0) {
        this.trackPreFillLowest(plate, fuelData, locTime);
      }
      
      // Source theft/fill baselines from Postgres: last 10 messages for this plate.
      const recentMessages = await postgresMessageStore.getRecentFuelMessages(plate, 10);
      if (!recentMessages || recentMessages.length === 0) return;

      const comparisonWindow = recentMessages
        .map((entry) => ({
          ...entry,
          speed: this.parseNumericFuelValue(entry.speed),
          combined_fuel_volume_in_tank: this.parseNumericFuelValue(entry.combined_fuel_volume_in_tank),
          combined_fuel_percentage: this.parseNumericFuelValue(entry.combined_fuel_percentage),
          fuel_probe_1_volume_in_tank: this.parseNumericFuelValue(entry.fuel_probe_1_volume_in_tank),
          fuel_probe_2_volume_in_tank: this.parseNumericFuelValue(entry.fuel_probe_2_volume_in_tank),
          fuel_probe_1_level_percentage: this.parseNumericFuelValue(entry.fuel_probe_1_level_percentage),
          fuel_probe_2_level_percentage: this.parseNumericFuelValue(entry.fuel_probe_2_level_percentage)
        }))
        .filter((entry) => {
          if (entry.timestamp > currentLocTime) return false;
          if (entry.speed >= 10) return false;
          return entry.combined_fuel_volume_in_tank > 0;
        });
      if (comparisonWindow.length === 0) return;

      let highestEntry = comparisonWindow[0];
      for (const entry of comparisonWindow) {
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
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error detecting fuel changes for ${plate}:`, error.message);
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
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â  Fill watcher update: ${plate} highest fuel now ${currentFuel}L`);
        }
        return; // Already tracking this fill
      }
      
      // Skip if we have a pending fill (status-based detection in progress)
      const existingPending = pendingFuelDb.getPendingFuelFill(plate);
      if (existingPending) {
        return; // Don't interfere with status-based fill detection
      }
      const engineIsOff = await this.isVehicleEngineOffForFill(plate, locTime);
      if (!engineIsOff) return;
      
      const currentTime = new Date(this.convertLocTime(locTime)).getTime();
      
      // Find the lowest fuel reading from the last 10 Postgres messages.
      let lowestRecentFuel = null;
      let lowestRecentTime = null;
      let lowestRecentLocTime = null;
      let lowestRecentPercentage = null;
      let lowestRecentFuelProbe1 = null;
      let lowestRecentFuelProbe2 = null;
      let lowestRecentPercentageProbe1 = null;
      let lowestRecentPercentageProbe2 = null;

      const recentMessages = await postgresMessageStore.getRecentFuelMessages(plate, 10);
      for (const entry of recentMessages) {
        if (entry.timestamp >= currentTime) continue;
        const entryFuel = this.parseNumericFuelValue(entry.combined_fuel_volume_in_tank);
        if (entryFuel <= 0) continue;
        const entryPercentage = this.parseNumericFuelValue(entry.combined_fuel_percentage);
        const entryFuelProbe1 = this.parseNumericFuelValue(entry.fuel_probe_1_volume_in_tank);
        const entryFuelProbe2 = this.parseNumericFuelValue(entry.fuel_probe_2_volume_in_tank);
        const entryPctProbe1 = this.parseNumericFuelValue(entry.fuel_probe_1_level_percentage);
        const entryPctProbe2 = this.parseNumericFuelValue(entry.fuel_probe_2_level_percentage);

        if (lowestRecentFuel === null || entryFuel < lowestRecentFuel) {
          lowestRecentFuel = entryFuel;
          lowestRecentTime = entry.timestamp;
          lowestRecentLocTime = entry.locTime;
          lowestRecentPercentage = entryPercentage;
          lowestRecentFuelProbe1 = entryFuelProbe1;
          lowestRecentFuelProbe2 = entryFuelProbe2;
          lowestRecentPercentageProbe1 = entryPctProbe1;
          lowestRecentPercentageProbe2 = entryPctProbe2;
        }
      }
      
      if (lowestRecentFuel === null) return;
      
      // Check if fuel increased by 10L+ from the lowest reading
      const fuelIncrease = currentFuel - lowestRecentFuel;
      
      if (fuelIncrease >= 10) {
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â½ PASSIVE FILL DETECTED for ${plate}: +${fuelIncrease.toFixed(1)}L in ${((currentTime - lowestRecentTime) / 1000).toFixed(0)}s`);
        
        // Use the lowest reading from the last 10 Postgres messages.
        const openingFuel = lowestRecentFuel;
        const openingPercentage = lowestRecentPercentage;
        const openingLocTime = lowestRecentLocTime;
        const openingFuelProbe1 = lowestRecentFuelProbe1;
        const openingFuelProbe2 = lowestRecentFuelProbe2;
        const openingPercentageProbe1 = lowestRecentPercentageProbe1;
        const openingPercentageProbe2 = lowestRecentPercentageProbe2;
        console.log(`[fill] Using Postgres last-10 lowest: ${openingFuel}L`);

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
        
        // Clean up pre-fill watcher so it cannot override the last-10 baseline
        pendingFuelDb.deletePreFillWatcher(plate);
        
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â½ PASSIVE FILL WATCHER started for ${plate} - Opening: ${openingFuel}L, Current: ${currentFuel}L`);
        
        // Note: No fixed timeout - fill will complete when fuel stabilizes (stops increasing for 2 min)
        // The stabilization checker runs every 30 seconds
      }
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error in passive fill detection for ${plate}:`, error.message);
    }
  }

  async handleFuelFillStart(plate, vehicleData) {
    try {
      const hasFuelData = this.hasFuelValue(vehicleData.fuel_probe_1_volume_in_tank) || this.hasFuelValue(vehicleData.fuel_probe_2_volume_in_tank);
      
      // Check if passive detection already started a watcher for this fill
      const existingWatcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (existingWatcher) {
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¯ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â FUEL FILL status for ${plate} - Watcher already active (passive detected earlier)`);
        return; // Let the existing watcher handle it
      }
      
      // Check if we already have a pending fill (status already received)
      const existingPending = pendingFuelDb.getPendingFuelFill(plate);
      if (existingPending) {
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¯ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â FUEL FILL status for ${plate} - Already tracking this fill`);
        return;
      }
      const engineIsOff = await this.isVehicleEngineOffForFill(plate, vehicleData.LocTime);
      if (!engineIsOff) {
        const statusInfo = await this.getLastEngineStatus(plate, vehicleData.LocTime);
        console.log(
          `[fill] Ignoring POSSIBLE FUEL FILL for ${plate} - latest engine status is ${
            statusInfo?.status || 'UNKNOWN'
          }`
        );
        return;
      }
      // Get opening fuel from Postgres only: lowest value from the last 10 messages.
      let openingFuel, openingPercentage;
      let openingFuelProbe1, openingFuelProbe2, openingPercentageProbe1, openingPercentageProbe2;
      let openingLocTime;
      const statusTimestamp = new Date(this.convertLocTime(vehicleData.LocTime)).getTime();
      const recentMessages = await postgresMessageStore.getRecentFuelMessages(plate, 10);
      let lowestMessage = null;

      for (const entry of recentMessages) {
        if (entry.timestamp > statusTimestamp) continue;
        const entryFuel = this.parseNumericFuelValue(entry.combined_fuel_volume_in_tank);
        if (entryFuel <= 0) continue;

        if (
          !lowestMessage ||
          entryFuel < this.parseNumericFuelValue(lowestMessage.combined_fuel_volume_in_tank) ||
          (
            entryFuel === this.parseNumericFuelValue(lowestMessage.combined_fuel_volume_in_tank) &&
            entry.timestamp > lowestMessage.timestamp
          )
        ) {
          lowestMessage = entry;
        }
      }
      
      console.log(`[fill] FILL START DEBUG for ${plate}:`);
      console.log(`   Fill status LocTime: ${vehicleData.LocTime}`);
      console.log(`   Postgres last-10 count: ${recentMessages.length}`);
      console.log(`   Lowest from Postgres: ${lowestMessage ? lowestMessage.combined_fuel_volume_in_tank + 'L' : 'NONE'}`);
      
      if (lowestMessage) {
        openingFuel = this.parseNumericFuelValue(lowestMessage.combined_fuel_volume_in_tank);
        openingPercentage = this.parseNumericFuelValue(lowestMessage.combined_fuel_percentage);
        openingFuelProbe1 = this.parseNumericFuelValue(lowestMessage.fuel_probe_1_volume_in_tank);
        openingFuelProbe2 = this.parseNumericFuelValue(lowestMessage.fuel_probe_2_volume_in_tank);
        openingPercentageProbe1 = this.parseNumericFuelValue(lowestMessage.fuel_probe_1_level_percentage);
        openingPercentageProbe2 = this.parseNumericFuelValue(lowestMessage.fuel_probe_2_level_percentage);
        openingLocTime = lowestMessage.locTime;
        console.log(`[fill] FUEL FILL START: ${plate} - Using Postgres lowest from last 10: ${openingFuel}L`);
      }
      
      if (openingFuel && openingFuel > 0) {
        const currentFuelData = hasFuelData ? this.parseFuelData(vehicleData) : null;
        const highestFuel = this.parseNumericFuelValue(currentFuelData?.combined_fuel_volume_in_tank ?? openingFuel);
        const highestPercentage = this.parseNumericFuelValue(currentFuelData?.combined_fuel_percentage ?? openingPercentage);
        const highestFuelProbe1 = this.parseNumericFuelValue(currentFuelData?.fuel_probe_1_volume_in_tank ?? openingFuelProbe1);
        const highestFuelProbe2 = this.parseNumericFuelValue(currentFuelData?.fuel_probe_2_volume_in_tank ?? openingFuelProbe2);
        const highestPercentageProbe1 = this.parseNumericFuelValue(currentFuelData?.fuel_probe_1_level_percentage ?? openingPercentageProbe1);
        const highestPercentageProbe2 = this.parseNumericFuelValue(currentFuelData?.fuel_probe_2_level_percentage ?? openingPercentageProbe2);
        const highestLocTime = currentFuelData ? vehicleData.LocTime : openingLocTime;
        const watcherStartLocTime = openingLocTime || vehicleData.LocTime;
        const watcherStartTime = this.convertLocTime(watcherStartLocTime);

        pendingFuelDb.setFuelFillWatcher(plate, {
          startTime: watcherStartTime,
          startLocTime: watcherStartLocTime,
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
        
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ FUEL FILL START: ${plate} - Opening: ${openingFuel}L at ${watcherStartLocTime} (status at ${vehicleData.LocTime})`);
      } else {
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¯ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â FUEL FILL START: ${plate} - No Postgres baseline in last 10 messages, skipping watcher creation`);
        return;
      }
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error starting fuel fill for ${plate}:`, error.message);
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
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â  New highest fuel for ${plate}: ${currentFuel}L`);
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
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ Got opening fuel for fill: ${plate} - ${fuelData.combined_fuel_volume_in_tank}L`);
      }
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error checking fuel fill for ${plate}:`, error.message);
    }
  }

  async completeFuelFillWatcher(plate) {
    try {
      const watcher = pendingFuelDb.getFuelFillWatcher(plate);
      if (!watcher) return;
      
      const watcherType = watcher.watcher_type || 'FILL';
      
      if (watcherType === 'FILL') {
        // Complete fuel fill using peak-based closing value.
        // Requirement: ending value is highest peak reached during the fill.
        const closingFuel = this.parseNumericFuelValue(watcher.highest_fuel);
        const closingPercentage = this.parseNumericFuelValue(watcher.highest_percentage);
        const openingFuel = this.parseNumericFuelValue(watcher.opening_fuel);
        const endTime = watcher.highest_loc_time
          ? this.convertLocTime(watcher.highest_loc_time)
          : new Date().toISOString();
        const fillAmount = Math.max(0, closingFuel - openingFuel);
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
            combined_fuel_volume_in_tank: openingFuel,
            combined_fuel_percentage: this.parseNumericFuelValue(watcher.opening_percentage),
            fuel_probe_1_volume_in_tank: this.parseNumericFuelValue(watcher.opening_fuel_probe_1),
            fuel_probe_2_volume_in_tank: this.parseNumericFuelValue(watcher.opening_fuel_probe_2),
            fuel_probe_1_level_percentage: this.parseNumericFuelValue(watcher.opening_percentage_probe_1),
            fuel_probe_2_level_percentage: this.parseNumericFuelValue(watcher.opening_percentage_probe_2)
          }),
          ...this.buildSessionFuelFields('closing', {
            combined_fuel_volume_in_tank: closingFuel,
            combined_fuel_percentage: closingPercentage,
            fuel_probe_1_volume_in_tank: this.parseNumericFuelValue(watcher.highest_fuel_probe_1),
            fuel_probe_2_volume_in_tank: this.parseNumericFuelValue(watcher.highest_fuel_probe_2),
            fuel_probe_1_level_percentage: this.parseNumericFuelValue(watcher.highest_percentage_probe_1),
            fuel_probe_2_level_percentage: this.parseNumericFuelValue(watcher.highest_percentage_probe_2)
          }),
          total_fill: fillAmount,
          session_status: 'FUEL_FILL_COMPLETED',
          notes: `Fuel fill completed. Duration: ${duration.toFixed(1)}s, Opening: ${openingFuel}L, Peak closing: ${closingFuel}L, Filled: ${fillAmount.toFixed(1)}L`
        });

        if (fillInsertError) {
          throw new Error(`Failed to insert completed fill session: ${fillInsertError.message}`);
        }
        
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ FILL COMPLETE: ${plate} - ${watcher.opening_fuel}L ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ ${watcher.highest_fuel}L = +${fillAmount.toFixed(1)}L`);
        
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
          const usageBeforeFill = Math.max(
            0,
            this.parseNumericFuelValue(session.opening_fuel) - openingFuel
          );
          
          await supabase
            .from('energy_rite_operating_sessions')
            .update({
              total_usage: (session.total_usage || 0) + usageBeforeFill,
              ...this.buildSessionFuelFields('opening', {
                combined_fuel_volume_in_tank: closingFuel,
                combined_fuel_percentage: closingPercentage,
                fuel_probe_1_volume_in_tank: this.parseNumericFuelValue(watcher.highest_fuel_probe_1),
                fuel_probe_2_volume_in_tank: this.parseNumericFuelValue(watcher.highest_fuel_probe_2),
                fuel_probe_1_level_percentage: this.parseNumericFuelValue(watcher.highest_percentage_probe_1),
                fuel_probe_2_level_percentage: this.parseNumericFuelValue(watcher.highest_percentage_probe_2)
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
        let stabilizedFuel = null;
        if (watcher.lowest_loc_time) {
          const confirmedStable = await this.findConfirmedStableFuelAfter(
            plate,
            watcher.lowest_loc_time,
            0,
            FUEL_STABILITY_CONFIG.fillSettleWindowMs
          );
          stabilizedFuel = confirmedStable || await this.findStabilizedFuelAfter(
            plate,
            watcher.lowest_loc_time,
            0,
            FUEL_STABILITY_CONFIG.fillSettleWindowMs
          );
        }
        const openingFuel = this.parseNumericFuelValue(watcher.opening_fuel);
        const closingFuel = this.parseNumericFuelValue(stabilizedFuel?.combined_fuel_volume_in_tank ?? watcher.lowest_fuel);
        const closingPercentage = this.parseNumericFuelValue(stabilizedFuel?.combined_fuel_percentage ?? watcher.lowest_percentage);
        const theftAmount = Math.max(0, openingFuel - closingFuel);
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
            combined_fuel_volume_in_tank: openingFuel,
            combined_fuel_percentage: this.parseNumericFuelValue(watcher.opening_percentage),
            fuel_probe_1_volume_in_tank: this.parseNumericFuelValue(watcher.opening_fuel_probe_1),
            fuel_probe_2_volume_in_tank: this.parseNumericFuelValue(watcher.opening_fuel_probe_2),
            fuel_probe_1_level_percentage: this.parseNumericFuelValue(watcher.opening_percentage_probe_1),
            fuel_probe_2_level_percentage: this.parseNumericFuelValue(watcher.opening_percentage_probe_2)
          }),
          ...this.buildSessionFuelFields('closing', stabilizedFuel || {
            combined_fuel_volume_in_tank: closingFuel,
            combined_fuel_percentage: closingPercentage,
            fuel_probe_1_volume_in_tank: this.parseNumericFuelValue(watcher.lowest_fuel_probe_1),
            fuel_probe_2_volume_in_tank: this.parseNumericFuelValue(watcher.lowest_fuel_probe_2),
            fuel_probe_1_level_percentage: this.parseNumericFuelValue(watcher.lowest_percentage_probe_1),
            fuel_probe_2_level_percentage: this.parseNumericFuelValue(watcher.lowest_percentage_probe_2)
          }),
          total_fill: -theftAmount,
          session_status: 'FUEL_THEFT_COMPLETED',
          notes: `Fuel theft completed. Duration: ${duration.toFixed(1)}s, Opening: ${openingFuel}L, Stable closing: ${closingFuel}L, Lowest seen: ${this.parseNumericFuelValue(watcher.lowest_fuel)}L, Lost: ${theftAmount.toFixed(1)}L`
        });

        if (theftInsertError) {
          throw new Error(`Failed to insert completed theft session: ${theftInsertError.message}`);
        }
        
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨ THEFT COMPLETE: ${plate} - ${watcher.opening_fuel}L ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ ${watcher.lowest_fuel}L = -${theftAmount.toFixed(1)}L`);
        
        // Record in anomalies table
        await supabase.from('energy_rite_fuel_anomalies').insert({
          plate: plate,
          anomaly_type: 'FUEL_THEFT',
          anomaly_date: endTime,
          fuel_before: openingFuel,
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
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error completing watcher for ${plate}:`, error.message);
    }
  }

  reconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
      
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
      
      setTimeout(() => {
        this.connect();
      }, delay);
    } else {
      console.error('ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Max reconnection attempts reached');
    }
  }



  async storeFuelData(vehicleData) {
    try {
      const fuelLevel = this.parseNumericFuelValue(vehicleData.fuel_probe_1_level);
      const fuelPercentage = this.parseNumericFuelValue(vehicleData.fuel_probe_1_level_percentage);
      const fuelProbe1Volume = this.parseNumericFuelValue(vehicleData.fuel_probe_1_volume_in_tank);
      const fuelProbe2Volume = this.parseNumericFuelValue(vehicleData.fuel_probe_2_volume_in_tank);
      const hasVolumeSignal = fuelProbe1Volume > 0 || fuelProbe2Volume > 0;

      if (fuelLevel <= 0 && !hasVolumeSignal) return;
      
      const timestamp = this.convertLocTime(vehicleData.LocTime);
      
      await supabase.from('energy_rite_fuel_data').insert({
        plate: vehicleData.actualBranch || vehicleData.Plate,
        fuel_probe_1_level: fuelLevel,
        fuel_probe_1_level_percentage: fuelPercentage,
        fuel_probe_1_volume_in_tank: fuelProbe1Volume,
        fuel_probe_2_level: this.parseNumericFuelValue(vehicleData.fuel_probe_2_level),
        fuel_probe_2_level_percentage: this.parseNumericFuelValue(vehicleData.fuel_probe_2_level_percentage),
        fuel_probe_2_volume_in_tank: fuelProbe2Volume,
        fuel_probe_2_temperature: this.parseNumericFuelValue(vehicleData.fuel_probe_2_temperature),
        created_at: timestamp
      });
      
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ Stored fuel data for ${vehicleData.actualBranch || vehicleData.Plate}: ${fuelLevel}L (${fuelPercentage}%)`);
      
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error storing fuel data for ${vehicleData.actualBranch || vehicleData.Plate}:`, error.message);
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
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ No fuel data in WebSocket for ${plate}, waiting for next message`);
          return; // Don't complete session without fuel data
        }
        
        const fillAmount = Math.max(0, currentFuel - startingFuel);
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ FUEL FILL COMPLETE: ${plate} - ${startingFuel}L ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ ${currentFuel}L = +${fillAmount.toFixed(1)}L`);
        
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
          
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ FUEL FILL COMPLETE: ${plate} - Duration: ${duration.toFixed(0)}s, Filled: ${fillAmount.toFixed(1)}L`);
        
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
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error completing fuel fill session for ${plate}:`, error.message);
    }
  }

  updateLastEngineStatus(plate, status, locTime = null, source = 'stream', driverName = null) {
    if (!plate || !status) return;

    const normalizedStatus = status === 'ON' ? 'ON' : status === 'OFF' ? 'OFF' : null;
    if (!normalizedStatus) return;

    const timestamp = locTime
      ? new Date(this.convertLocTime(locTime)).getTime()
      : Date.now();

    this.lastEngineStatusByPlate.set(plate, {
      status: normalizedStatus,
      locTime: locTime || null,
      timestamp,
      source,
      driverName: driverName || null
    });
  }

  async getLastEngineStatus(plate, targetLocTime = null) {
    const fromMemory = this.lastEngineStatusByPlate.get(plate);
    const targetTimestamp = targetLocTime
      ? new Date(this.convertLocTime(targetLocTime)).getTime()
      : Date.now();

    if (
      fromMemory &&
      Number.isFinite(fromMemory.timestamp) &&
      Math.abs(targetTimestamp - fromMemory.timestamp) <= ENGINE_STATUS_LOOKBACK_MS
    ) {
      return fromMemory;
    }

    try {
      const beforeTimeIso = targetLocTime ? this.convertLocTime(targetLocTime) : undefined;
      const recentStatusMessages = await postgresMessageStore.getRecentStatusMessages(plate, 25, { beforeTimeIso });

      for (const entry of recentStatusMessages) {
        const status = this.parseEngineStatus(entry.driver_name, { silent: true });
        if (!status) continue;

        this.updateLastEngineStatus(plate, status, entry.locTime, 'postgres_history', entry.driver_name);
        return this.lastEngineStatusByPlate.get(plate) || null;
      }
    } catch (error) {
      console.error(`[engine] Failed to load last engine status for ${plate}:`, error.message);
    }

    return fromMemory || null;
  }

  async isVehicleEngineOffForFill(plate, targetLocTime = null) {
    const statusInfo = await this.getLastEngineStatus(plate, targetLocTime);
    if (!statusInfo || statusInfo.status !== 'OFF') {
      return false;
    }

    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id')
      .eq('branch', plate)
      .eq('session_status', 'ONGOING')
      .limit(1);

    if (error) {
      console.error(`[fill] Failed to verify engine session state for ${plate}:`, error.message);
      return false;
    }

    return !sessions || sessions.length === 0;
  }

  parseEngineStatus(driverName, options = {}) {
    const silent = options.silent === true;
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    if (!silent) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â Checking engine status: "${driverName}" -> "${normalized}"`);
    }
    
    // Priority detection: ENGINE, IGNITION, and PTO status
    if (normalized.includes('ENGINE ON') || 
        normalized.includes('IGNITION ON') || 
        normalized.includes('PTO ON')) {
      if (!silent) {
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ ENGINE/IGNITION ON DETECTED: ${driverName}`);
      }
      return 'ON';
    }
    if (normalized.includes('ENGINE OFF') || 
        normalized.includes('IGNITION OFF') || 
        normalized.includes('PTO OFF')) {
      if (!silent) {
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â´ ENGINE/IGNITION OFF DETECTED: ${driverName}`);
      }
      return 'OFF';
    }
    
    if (!silent) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¯ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â No engine/ignition status detected in: "${driverName}"`);
    }
    return null;
  }

  parseFuelFillStatus(driverName) {
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â Checking fuel fill status: "${driverName}" -> "${normalized}"`);
    
    // Only trigger fills when status explicitly says "Possible Fuel Fill"
    if (normalized.includes('POSSIBLE FUEL FILL')) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ POSSIBLE FUEL FILL DETECTED: ${driverName}`);
      return 'START';
    }
    
    console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¯ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â No fuel fill status detected in: "${driverName}"`);
    return null;
  }

  parseFuelTheftStatus(driverName) {
    if (!driverName || driverName.trim() === '') {
      return null;
    }
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    
    if (normalized.includes('POSSIBLE FUEL THEFT') ||
        normalized.includes('FUEL THEFT')) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨ FUEL THEFT STATUS DETECTED: ${driverName}`);
      return 'START';
    }
    
    return null;
  }

  async handleFuelTheftStart(plate, vehicleData) {
    try {
      const currentTime = this.convertLocTime(vehicleData.LocTime);
      const currentFuel = this.parseNumericFuelValue(vehicleData.fuel_probe_1_volume_in_tank);
      const currentPercentage = this.parseNumericFuelValue(vehicleData.fuel_probe_1_level_percentage);
      
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
      
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨ FUEL THEFT: ${plate} - ${highestFuel}L ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ ${currentFuel}L = -${theftAmount.toFixed(1)}L`);
      
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
      
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨ FUEL THEFT COMPLETE: ${plate} - ${highestFuel}L ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ ${currentFuel}L = -${theftAmount.toFixed(1)}L`);
      
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
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error handling fuel theft for ${plate}:`, error.message);
    }
  }

  async handleFuelFill(plate, fillResult) {
    try {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ Handling fuel fill for ${plate}:`, fillResult.fillDetails);
      
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
          
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¹ Updated session ${session.id} with fill: +${fillResult.fillDetails.fillAmount.toFixed(1)}L`);
      }
      
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error handling fuel fill for ${plate}:`, error.message);
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
          
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ FUEL FILL START: ${plate} - Opening: ${openingFuel}L (${openingPercentage}%)`);
        } else {
          console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ FUEL FILL ONGOING: ${plate} - Session already exists`);
        }
      }
      
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error handling fuel fill session for ${plate}:`, error.message);
    }
  }

  async findClosestFuelData(plate, targetLocTime) {
    return this.findFuelSnapshotAtOrAfter(plate, targetLocTime);
  }

  async findClosestFuelDataBefore(plate, targetLocTime) {
    return this.findFuelSnapshotAtOrBefore(plate, targetLocTime);
  }

  async findLowestFuelDataBefore(plate, targetLocTime, lookbackMs = FUEL_STABILITY_CONFIG.fillStartLookbackMs) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const readings = await this.getFuelHistoryRange(plate, targetTime - lookbackMs, targetTime - 1);

    if (!readings || readings.length === 0) {
      return null;
    }

    let lowestFuel = null;
    for (const entry of readings) {
      const fuelVolume = this.parseNumericFuelValue(entry.combined_fuel_volume_in_tank);
      if (fuelVolume <= 0) continue;

      if (!lowestFuel || fuelVolume < lowestFuel.combined_fuel_volume_in_tank) {
        lowestFuel = entry;
      } else if (fuelVolume === lowestFuel.combined_fuel_volume_in_tank && entry.timestamp > lowestFuel.timestamp) {
        // If equal lows exist, prefer the most recent so opening aligns with fill onset.
        lowestFuel = entry;
      }
    }

    if (!lowestFuel) {
      return null;
    }

    const diffMs = Math.max(0, targetTime - lowestFuel.timestamp);
    console.log(
      `[capture] Found LOWEST fuel before status for ${plate}: ` +
      `${lowestFuel.combined_fuel_volume_in_tank}L (${(diffMs / 1000).toFixed(0)}s before, window ${(lookbackMs / 60000).toFixed(0)}m)`
    );

    return {
      ...lowestFuel,
      source: 'lowest_before_window',
      diffMs
    };
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

  async getFuelHistoryRange(plate, startTimestamp, endTimestamp, limit = 500) {
    if (!Number.isFinite(startTimestamp) || !Number.isFinite(endTimestamp) || startTimestamp > endTimestamp) {
      return [];
    }

    const startIso = new Date(startTimestamp).toISOString();
    const endIso = new Date(endTimestamp).toISOString();
    const combined = [];
    const seen = new Set();

    const dbHistory = await postgresMessageStore.getFuelMessagesInRange(
      plate,
      startIso,
      endIso,
      limit
    );

    for (const entry of dbHistory) {
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

  async findConfirmedStableFuelAfter(
    plate,
    targetLocTime,
    settleDelayMs,
    windowMs,
    minSamples = FUEL_STABILITY_CONFIG.minStableSamples,
    maxSpreadLiters = FUEL_STABILITY_CONFIG.maxStableSpreadLiters
  ) {
    const readings = await this.getFuelHistoryRange(
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
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ Stable fuel not confirmed for ${plate}: spread ${spread.toFixed(1)}L across ${sample.length} samples`);
      return null;
    }

    const stableReading = this.getMedianFuelReading(sample);
    if (stableReading) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â½ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¯ Confirmed stable fuel AFTER ${plate}: ${stableReading.combined_fuel_volume_in_tank}L from ${sample.length} samples (spread ${spread.toFixed(1)}L)`);
    }
    return stableReading;
  }

  async findStabilizedFuelBefore(plate, targetLocTime, lookbackMs = FUEL_STABILITY_CONFIG.engineOnLookbackMs) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const readings = await this.getFuelHistoryRange(plate, targetTime - lookbackMs, targetTime - 1);

    if (readings.length === 0) {
      return null;
    }

    const stabilized = this.getMedianFuelReading(readings);
    if (stabilized) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¯ Stabilized fuel BEFORE ${plate}: ${stabilized.combined_fuel_volume_in_tank}L from ${readings.length} samples`);
    }
    return stabilized;
  }

  async findStabilizedFuelAfter(
    plate,
    targetLocTime,
    settleDelayMs = FUEL_STABILITY_CONFIG.engineOffSettleDelayMs,
    windowMs = FUEL_STABILITY_CONFIG.engineOffWindowMs
  ) {
    const targetTime = new Date(this.convertLocTime(targetLocTime)).getTime();
    const startTime = targetTime + settleDelayMs;
    const endTime = startTime + windowMs;
    const readings = await this.getFuelHistoryRange(plate, startTime, endTime);

    if (readings.length === 0) {
      return null;
    }

    const stabilized = this.getMedianFuelReading(readings);
    if (stabilized) {
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¯ Stabilized fuel AFTER ${plate}: ${stabilized.combined_fuel_volume_in_tank}L from ${readings.length} samples`);
    }
    return stabilized;
  }

  async handleSessionChange(plate, engineStatus, wsMessage = null) {
    try {
      const currentTime = this.convertLocTime(wsMessage?.LocTime);
      
      if (engineStatus === 'ON') {
        if (pendingFuelDb.getPreFillWatcher(plate)) {
          pendingFuelDb.deletePreFillWatcher(plate);
          console.log(`[prefill] Cleared pre-fill watcher for ${plate} on ENGINE ON`);
        }
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
          const openingFuel = await this.findLowestFuelFromLastMessages(plate, wsMessage.LocTime, 10);

          if (openingFuel) {
            const captureDetails = this.buildCaptureDetails(openingFuel, 'before');
            await supabase.from('energy_rite_operating_sessions').insert({
              branch: plate,
              company: 'WATERFORD',
              session_date: currentTime.split('T')[0],
              session_start_time: currentTime,
              ...this.buildSessionFuelFields('opening', openingFuel),
              session_status: 'ONGOING',
              notes: `Engine started. Opening: ${openingFuel.combined_fuel_volume_in_tank}L (${openingFuel.combined_fuel_percentage}%) [capture: ${captureDetails}]`
            });
            
            console.log(`[session] ENGINE ON: ${plate} - Opening: ${openingFuel.combined_fuel_volume_in_tank}L (${openingFuel.combined_fuel_percentage}%) [${captureDetails}]`);
          } else {
            this.pendingFuelUpdates.set(plate, {
              sessionId: null,
              statusLocTime: wsMessage.LocTime,
              timestamp: Date.now(),
              needsSessionCreation: true
            });
            console.log(`[session] ENGINE ON: ${plate} - Waiting for last-10 history to include valid fuel data`);
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
          const closingFuel = await this.findSessionClosingFuelFromLastMessages(plate, wsMessage.LocTime, 10);

          if (closingFuel) {
            const captureDetails = this.buildCaptureDetails(closingFuel, 'after');
            await this.completeSession(session, currentTime, closingFuel, captureDetails);
            this.pendingClosures.delete(plate);
            this.pendingFuelUpdates.delete(plate);
            console.log(`[session] ENGINE OFF: ${plate} - Closing captured [${captureDetails}]`);
          } else {
            this.pendingClosures.set(plate, { 
              sessionId: session.id, 
              endTime: currentTime,
              statusLocTime: wsMessage.LocTime,
              timestamp: Date.now()
            });
            console.log(`[session] Engine OFF for ${plate} - Waiting for last-10 history to include valid fuel data`);
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
      
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ Fuel fill detected for ${plate} - Pre-fill: ${preFillLevel}L (${preFillPercentage}%)`);
      
      if (!this.fuelFillTracking) this.fuelFillTracking = new Map();
      
      this.fuelFillTracking.set(plate, {
        preFillLevel,
        preFillPercentage,
        fillStartTime: currentTime,
        isTracking: true
      });
      
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error handling fuel fill start for ${plate}:`, error.message);
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
      
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ Fuel fill ended for ${plate}: +${fillAmount.toFixed(1)}L`);
      
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
          notes: `Fuel Fill: +${fillAmount.toFixed(1)}L (${tracking.preFillLevel}L ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ ${currentFuelLevel}L) Duration: ${fillDuration.toFixed(0)}s`
        });
        
        console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½ Fuel fill session created: ${plate} +${fillAmount.toFixed(1)}L`);
        
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
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error handling fuel fill end for ${plate}:`, error.message);
    }
  }

  async checkFuelFillCompletion(plate, vehicleData) {
    // This method is now mainly for backup/timeout scenarios
    // Primary detection is through status change
  }

  async completeSession(session, endTime, vehicleData, captureDetails = null) {
    try {
      const closingFuelFields = this.buildSessionFuelFields('closing', vehicleData);
      const closingFuel = closingFuelFields.closing_fuel || 0;
      const closingFuelProbe1 = closingFuelFields.closing_fuel_probe_1 || 0;
      const closingFuelProbe2 = closingFuelFields.closing_fuel_probe_2 || 0;
      const closingVolume = closingFuel;
      const closingTemperature = this.parseNumericFuelValue(vehicleData.combined_fuel_temperature);
      
      const startTime = new Date(session.session_start_time);
      const endTimeDate = new Date(endTime);
      const durationMs = endTimeDate.getTime() - startTime.getTime();
      const operatingHours = Math.max(0, durationMs / (1000 * 60 * 60));
      const startingFuel = this.parseNumericFuelValue(session.opening_fuel);
      const startingFuelProbe1 = this.parseNumericFuelValue(session.opening_fuel_probe_1);
      const startingFuelProbe2 = this.parseNumericFuelValue(session.opening_fuel_probe_2);
      
      // Add any accumulated usage from fills during session
      const accumulatedUsage = this.parseNumericFuelValue(session.total_usage);
      
      // Calculate session segment usage from both combined and probe-level deltas.
      // Using the best positive delta avoids false zero-usage sessions when one signal is noisy.
      const combinedSegmentUsage = Math.max(0, startingFuel - closingFuel);
      const probe1SegmentUsage = Math.max(0, startingFuelProbe1 - closingFuelProbe1);
      const probe2SegmentUsage = Math.max(0, startingFuelProbe2 - closingFuelProbe2);
      const probeSegmentUsage = probe1SegmentUsage + probe2SegmentUsage;
      const segmentUsage = Math.max(combinedSegmentUsage, probeSegmentUsage);
      const finalUsage = accumulatedUsage + segmentUsage;
      const fuelCost = finalUsage * 20;
      const literUsagePerHour = operatingHours > 0 ? finalUsage / operatingHours : 0;
      const captureSuffix = captureDetails ? `, Capture: ${captureDetails}` : '';
      const usageDebug = `, SegmentUsed: ${segmentUsage.toFixed(1)}L (combined ${combinedSegmentUsage.toFixed(1)}L, probe1 ${probe1SegmentUsage.toFixed(1)}L, probe2 ${probe2SegmentUsage.toFixed(1)}L, accumulated ${accumulatedUsage.toFixed(1)}L)`;
      
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
          notes: `Engine stopped. Duration: ${operatingHours.toFixed(2)}h, Opening: ${startingFuel}L, Closing: ${closingFuel}L, Used: ${finalUsage.toFixed(1)}L${usageDebug}${captureSuffix}`
        })
        .eq('id', session.id);
        
      console.log(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â´ Engine OFF: ${session.branch} - Used: ${finalUsage.toFixed(1)}L in ${operatingHours.toFixed(2)}h`);
    } catch (error) {
      console.error(`ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Error completing session:`, error.message);
    }
  }

  async updatePendingClosure(plate, vehicleData) {
    try {
      if (!this.pendingClosures.has(plate)) return;
      
      const pending = this.pendingClosures.get(plate);
      const closestFuel = await this.findSessionClosingFuelFromLastMessages(plate, pending.statusLocTime, 10);
      if (!closestFuel) {
        console.log(`[session] Waiting for last-10 history to include valid ENGINE OFF fuel for ${plate}`);
        return;
      }
      
      const { data: session } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('id', pending.sessionId)
        .single();
        
      if (session) {
        const captureDetails = this.buildCaptureDetails(closestFuel, 'after');
        await this.completeSession(session, pending.endTime, closestFuel, captureDetails);
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
      const closestFuel = await this.findLowestFuelFromLastMessages(plate, pending.statusLocTime, 10);
      
      if (!closestFuel) {
        console.log(`[session] Waiting for last-10 history to include valid ENGINE ON fuel for ${plate}`);
        return;
      }
      
      if (pending.needsSessionCreation) {
        const currentTime = this.convertLocTime(pending.statusLocTime);
        const captureDetails = this.buildCaptureDetails(closestFuel, 'before');
        await supabase.from('energy_rite_operating_sessions').insert({
          branch: plate,
          company: 'WATERFORD',
          session_date: currentTime.split('T')[0],
          session_start_time: currentTime,
          ...this.buildSessionFuelFields('opening', closestFuel),
          session_status: 'ONGOING',
          notes: `Engine started. Opening: ${closestFuel.combined_fuel_volume_in_tank}L (${closestFuel.combined_fuel_percentage}%) [capture: ${captureDetails}]`
        });
        console.log(`[session] Created session for ${plate} with fuel BEFORE ON: ${closestFuel.combined_fuel_volume_in_tank}L`);
      } else if (pending.sessionId) {
        const captureDetails = this.buildCaptureDetails(closestFuel, 'before');
        await supabase.from('energy_rite_operating_sessions')
          .update({
            ...this.buildSessionFuelFields('opening', closestFuel),
            notes: `Engine started. Opening: ${closestFuel.combined_fuel_volume_in_tank}L (${closestFuel.combined_fuel_percentage}%) [capture: ${captureDetails}]`
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












