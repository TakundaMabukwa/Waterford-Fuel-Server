const { supabase } = require('../supabase-client');

/**
 * Fuel Fill Detection System
 * Detects fuel fills when there's a significant increase in fuel level
 */

const FUEL_FILL_CONFIG = {
    // Minimum fuel increase to consider as a fill (in liters)
    MIN_FILL_AMOUNT: 5.0,
    
    // Maximum time window for fill detection (in minutes)
    MAX_TIME_WINDOW: 180,
    
    // Minimum percentage increase to consider
    MIN_PERCENTAGE_INCREASE: 2.0
};

/**
 * Detect fuel fill based on fuel level increase
 * @param {string} plate - Vehicle plate number
 * @param {number} currentFuelLevel - Current fuel level
 * @param {string} driverName - Driver name status
 * @param {Object} fuelData - Complete fuel data from WebSocket message
 * @returns {Object} Fill detection result
 */
async function detectFuelFill(plate, currentFuelLevel, driverName, fuelData = {}) {
    try {
        // Parse and validate current fuel level
        const parsedCurrentFuel = parseFloat(currentFuelLevel);
        if (isNaN(parsedCurrentFuel) || parsedCurrentFuel < 0) {
            return { isFill: false, reason: 'Invalid fuel level' };
        }

        // Check if "Possible Fuel Fill" or "Fuel Fill" is in driver name
        const hasFillStatus = driverName && (
            driverName.toLowerCase().includes('possible fuel fill') ||
            driverName.toLowerCase().includes('fuel fill') ||
            driverName.toLowerCase().includes('refuel') ||
            driverName.toLowerCase().includes('filling') ||
            driverName.toLowerCase().includes('fill')
        );

        // If driver status indicates fuel fill, handle session management
        if (hasFillStatus) {
            console.log(`🚨 FUEL FILL STATUS DETECTED: ${plate} - ${driverName}`);
            
            // Get current active session to update
            const { data: activeSession } = await supabase
                .from('energy_rite_operating_sessions')
                .select('*')
                .eq('branch', plate)
                .eq('session_status', 'ONGOING')
                .order('session_start_time', { ascending: false })
                .limit(1)
                .single();

            let fuelBefore = 0;
            let fillAmount = 50; // Default

            if (activeSession) {
                // Update session to mark fuel fill event
                fuelBefore = activeSession.opening_fuel || 0;
                fillAmount = Math.max(parsedCurrentFuel - fuelBefore, 0);

                await supabase
                    .from('energy_rite_operating_sessions')
                    .update({
                        fill_events: (activeSession.fill_events || 0) + 1,
                        fill_amount_during_session: (activeSession.fill_amount_during_session || 0) + fillAmount,
                        // Reset opening fuel to current level to prevent usage miscalculation
                        opening_fuel: parsedCurrentFuel
                    })
                    .eq('id', activeSession.id);
            }

            // Get vehicle info
            let vehicleInfo = null;
            try {
                const { data, error: vehicleError } = await supabase
                    .from('energyrite_vehicle_lookup')
                    .select('cost_code, company')
                    .eq('plate', plate)
                    .single();
                
                if (!vehicleError) {
                    vehicleInfo = data;
                }
            } catch (vehicleError) {
                console.log(`Vehicle lookup failed for ${plate}, using defaults`);
            }

            // Use the complete fuel data for session creation
            const currentFuelData = fuelData;
            
            // Create fuel fill session with all available data
            const fillSession = await supabase
                .from('energy_rite_operating_sessions')
                .insert({
                    branch: plate,
                    company: vehicleInfo?.company || 'KFC',
                    cost_code: vehicleInfo?.cost_code || null,
                    session_date: new Date().toISOString().split('T')[0],
                    session_start_time: new Date().toISOString(),
                    session_end_time: new Date().toISOString(),
                    operating_hours: 0,
                    opening_percentage: currentFuelData.fuel_probe_1_level_percentage || 0,
                    opening_fuel: fuelBefore,
                    opening_volume: currentFuelData.fuel_probe_1_volume_in_tank || 0,
                    opening_temperature: currentFuelData.fuel_probe_1_temperature || 0,
                    closing_percentage: currentFuelData.fuel_probe_1_level_percentage || 0,
                    closing_fuel: parsedCurrentFuel,
                    closing_volume: currentFuelData.fuel_probe_1_volume_in_tank || 0,
                    closing_temperature: currentFuelData.fuel_probe_1_temperature || 0,
                    total_fill: fillAmount,
                    total_usage: 0,
                    liter_usage_per_hour: 0,
                    cost_per_liter: 20.00,
                    cost_for_usage: fillAmount * 20.00,
                    session_status: 'FUEL_FILL',
                    notes: `Fuel fill detected via status: ${driverName}`,
                    fill_events: 1,
                    fill_amount_during_session: fillAmount
                });

            if (fillSession.error) {
                console.error('Error creating fuel fill session:', fillSession.error.message);
            } else {
                console.log(`⛽ FUEL FILL SESSION CREATED: ${plate} - ${fillAmount}L`);
            }

            return {
                isFill: true,
                vehicle: { plate: plate },
                fillDetails: {
                    fuelBefore: fuelBefore,
                    currentFuel: parsedCurrentFuel,
                    fillAmount: fillAmount,
                    detectionMethod: 'STATUS_INDICATOR',
                    fillTime: new Date(),
                    sessionUpdated: !!activeSession
                }
            };
        }

        // Get recent fuel data for comparison (filter out null values)
        const { data: recentData, error } = await supabase
            .from('energy_rite_fuel_data')
            .select('*')
            .eq('plate', plate)
            .not('fuel_probe_1_level', 'is', null)
            .order('created_at', { ascending: false })
            .limit(10);

        if (error) {
            console.error('Database error in fuel fill detection:', error.message);
            return { isFill: false, reason: 'Database connection error' };
        }

        if (!recentData || !Array.isArray(recentData)) {
            return { isFill: false, reason: 'No data available' };
        }

        // Filter out entries with null or invalid fuel levels
        const validData = recentData.filter(entry => {
            const fuel = parseFloat(entry?.fuel_probe_1_level);
            return !isNaN(fuel) && fuel >= 0;
        });

        if (validData.length < 1) {
            return { isFill: false, reason: 'Insufficient valid data for comparison' };
        }

        const current = validData[0];
        const previous = validData.length > 1 ? validData[1] : null;
        const currentTime = new Date();
        
        if (!previous) {
            return { isFill: false, reason: 'Need at least 2 data points for comparison' };
        }
        
        const previousTime = new Date(previous.created_at);

        // Calculate time difference
        const timeDiffMinutes = (currentTime - previousTime) / (1000 * 60);

        // Skip if time difference is too large
        if (timeDiffMinutes > FUEL_FILL_CONFIG.MAX_TIME_WINDOW) {
            return { isFill: false, reason: 'Time gap too large' };
        }

        const previousFuel = parseFloat(previous?.fuel_probe_1_level || 0);
        const currentFuel = parseFloat(current?.fuel_probe_1_level || parsedCurrentFuel);
        const fuelIncrease = currentFuel - previousFuel;
        const fuelIncreasePercentage = previousFuel > 0 ? (fuelIncrease / previousFuel) * 100 : 0;

        // Check fill conditions
        const fillConditions = {
            significantIncrease: fuelIncrease >= FUEL_FILL_CONFIG.MIN_FILL_AMOUNT,
            percentageIncrease: fuelIncreasePercentage >= FUEL_FILL_CONFIG.MIN_PERCENTAGE_INCREASE,
            withinTimeWindow: timeDiffMinutes <= FUEL_FILL_CONFIG.MAX_TIME_WINDOW,
            statusIndicatesFill: hasFillStatus,
            positiveIncrease: fuelIncrease > 0
        };

        // Detect fuel increase (lowered threshold for better detection)
        const isFill = fillConditions.positiveIncrease && 
                      (fuelIncrease >= 5 || fuelIncreasePercentage >= 5) && 
                      fillConditions.withinTimeWindow;

        if (isFill) {
            // Get cost code and company for this vehicle
            let vehicleInfo = null;
            try {
                const { data, error: vehicleError } = await supabase
                    .from('energyrite_vehicle_lookup')
                    .select('cost_code, company')
                    .eq('plate', plate)
                    .single();
                
                if (!vehicleError) {
                    vehicleInfo = data;
                }
            } catch (vehicleError) {
                console.log(`Vehicle lookup failed for ${plate}, using defaults`);
            }

            // Log fuel fill
            const fillRecord = await supabase
                .from('energy_rite_fuel_fills')
                .insert({
                    plate: plate,
                    cost_code: vehicleInfo?.cost_code || null,
                    company: vehicleInfo?.company || 'KFC',
                    fill_date: currentTime.toISOString(),
                    fuel_before: previousFuel,
                    fuel_after: currentFuel,
                    fill_amount: fuelIncrease,
                    fill_percentage: fuelIncreasePercentage,
                    detection_method: hasFillStatus ? 'STATUS_INDICATOR' : 'LEVEL_INCREASE',
                    status: 'detected',
                    fill_data: {
                        timeDiffMinutes: timeDiffMinutes.toFixed(2),
                        conditions: fillConditions,
                        driver_status: driverName,
                        cost_code: vehicleInfo?.cost_code,
                        company: vehicleInfo?.company
                    }
                });

            if (fillRecord.error) {
                console.error('Error logging fuel fill:', fillRecord.error.message || fillRecord.error);
                // Continue execution even if logging fails
            }

            // Log activity (with error handling)
            try {
                await supabase.from('energy_rite_activity_log').insert({
                    activity_type: 'FUEL_FILL',
                    description: `Fuel fill detected for ${plate}: +${fuelIncrease.toFixed(1)}L`,
                    branch: plate,
                    activity_data: {
                        previous_fuel: previousFuel,
                        current_fuel: currentFuel,
                        fill_amount: fuelIncrease,
                        fill_percentage: fuelIncreasePercentage.toFixed(2),
                        detection_method: hasFillStatus ? 'STATUS_INDICATOR' : 'LEVEL_INCREASE',
                        timestamp: currentTime.toISOString()
                    }
                });
            } catch (activityError) {
                console.error('Error logging activity:', activityError.message);
            }

            const fillInfo = {
                isFill: true,
                vehicle: {
                    plate: plate
                },
                fillDetails: {
                    previousFuel: previousFuel,
                    currentFuel: currentFuel,
                    fillAmount: fuelIncrease,
                    fillPercentage: fuelIncreasePercentage.toFixed(2),
                    timeDiffMinutes: timeDiffMinutes.toFixed(2),
                    fillTime: currentTime,
                    detectionMethod: hasFillStatus ? 'STATUS_INDICATOR' : 'LEVEL_INCREASE'
                },
                conditions: fillConditions
            };

            console.log(`⛽ FUEL FILL DETECTED! Vehicle: ${plate} - Fill: +${fuelIncrease.toFixed(1)}L (${fuelIncreasePercentage.toFixed(1)}%) - Method: ${fillInfo.fillDetails.detectionMethod}`);

            return fillInfo;
        } else {
            return { 
                isFill: false, 
                reason: 'No significant fuel increase detected',
                details: {
                    fuelIncrease: fuelIncrease,
                    fuelIncreasePercentage: fuelIncreasePercentage.toFixed(2),
                    timeDiffMinutes: timeDiffMinutes.toFixed(2),
                    conditions: fillConditions
                }
            };
        }

    } catch (error) {
        console.error('Error detecting fuel fill:', error.message);
        return { isFill: false, reason: 'Detection error' };
    }
}

/**
 * Get fuel fill statistics
 * @returns {Object} Fill statistics
 */
async function getFuelFillStatistics() {
    try {
        const { data: fillData, error: fillError } = await supabase
            .from('energy_rite_fuel_fills')
            .select('*')
            .order('fill_date', { ascending: false });

        if (fillError) throw new Error(`Database error: ${fillError.message}`);

        // Get recent fills (last 24 hours)
        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        const recentFills = fillData.filter(fill => fill.fill_date >= yesterday);

        // Get unique vehicles
        const uniqueVehicles = [...new Set(fillData.map(f => f.plate))];

        return {
            statistics: {
                total_vehicles_with_fills: uniqueVehicles.length,
                total_fill_events: fillData.length,
                recent_fills: recentFills.length,
                total_fuel_filled: fillData.reduce((sum, f) => sum + parseFloat(f.fill_amount || 0), 0)
            },
            recentFills: recentFills.slice(0, 10).map(fill => ({
                plate: fill.plate,
                fill_date: fill.fill_date,
                fuel_before: fill.fuel_before,
                fuel_after: fill.fuel_after,
                fill_amount: fill.fill_amount,
                detection_method: fill.detection_method
            }))
        };
    } catch (error) {
        console.error('Error getting fuel fill statistics:', error.message);
        return { error: error.message };
    }
}

/**
 * Get fuel fills for a specific vehicle
 * @param {string} plate - Vehicle plate number
 * @param {number} days - Number of days to look back
 * @returns {Object} Vehicle fill history
 */
async function getVehicleFillHistory(plate, days = 30) {
    try {
        const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

        const { data: fillData, error } = await supabase
            .from('energy_rite_fuel_fills')
            .select('*')
            .eq('plate', plate)
            .gte('fill_date', startDate)
            .order('fill_date', { ascending: false });

        if (error) throw new Error(`Database error: ${error.message}`);

        return {
            success: true,
            vehicle: plate,
            period_days: days,
            total_fills: fillData.length,
            total_fuel_filled: fillData.reduce((sum, f) => sum + parseFloat(f.fill_amount || 0), 0),
            fills: fillData
        };
    } catch (error) {
        console.error('Error getting vehicle fill history:', error.message);
        return { success: false, error: error.message };
    }
}

module.exports = {
    detectFuelFill,
    getFuelFillStatistics,
    getVehicleFillHistory,
    FUEL_FILL_CONFIG
};