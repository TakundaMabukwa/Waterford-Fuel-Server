const { supabase } = require('../supabase-client');

/**
 * Fuel Theft Detection System
 * Detects rapid fuel level drops that may indicate theft
 */

// Configuration for theft detection
const THEFT_CONFIG = {
    // Minimum fuel drop percentage to consider as potential theft
    MIN_DROP_PERCENTAGE: 20, // 20% drop
    
    // Maximum time window for theft detection (in minutes)
    MAX_TIME_WINDOW: 30, // 30 minutes
    
    // Minimum fuel volume drop to consider (in liters)
    MIN_VOLUME_DROP: 50, // 50 liters
    
    // Maximum fuel drop rate (liters per minute)
    MAX_DROP_RATE: 5 // 5 liters per minute
};

/**
 * Detect fuel theft based on rapid fuel level drops
 * @param {string} plate - Vehicle plate number
 * @param {number} currentFuelLevel - Current fuel level from feed
 * @param {string} pocsagstr - Vehicle identifier
 * @returns {Object} Theft detection result
 */
async function detectFuelTheft(plate, currentFuelLevel, pocsagstr) {
    try {
        if (!currentFuelLevel || currentFuelLevel <= 0) {
            return { isTheft: false, reason: 'Invalid fuel level' };
        }

        // Get recent fuel data for this vehicle
        const { data: recentData, error } = await supabase
            .from('energy_rite_fuel_data')
            .select('*')
            .eq('plate', plate)
            .order('created_at', { ascending: false })
            .limit(2);

        if (error) throw new Error(`Database error: ${error.message}`);

        if (recentData.length < 2) {
            return { isTheft: false, reason: 'Insufficient data for comparison' };
        }

        const current = recentData[0];
        const previous = recentData[1];
        const currentTime = new Date();
        const previousTime = new Date(previous.created_at);

        // Calculate time difference
        const timeDiffMinutes = (currentTime - previousTime) / (1000 * 60);

        // Skip if time difference is too large (more than 2 hours)
        if (timeDiffMinutes > 120) {
            return { isTheft: false, reason: 'Time gap too large' };
        }

        // Calculate fuel drop
        const previousFuel = parseFloat(previous.fuel_probe_1_level);
        const currentFuel = parseFloat(current.fuel_probe_1_level);
        const fuelDrop = previousFuel - currentFuel;
        const fuelDropPercentage = (fuelDrop / previousFuel) * 100;
        const fuelDropRate = fuelDrop / timeDiffMinutes;

        // Check theft conditions
        const theftConditions = {
            significantDrop: fuelDrop >= THEFT_CONFIG.MIN_VOLUME_DROP,
            highPercentage: fuelDropPercentage >= THEFT_CONFIG.MIN_DROP_PERCENTAGE,
            rapidDrop: fuelDropRate >= THEFT_CONFIG.MAX_DROP_RATE,
            withinTimeWindow: timeDiffMinutes <= THEFT_CONFIG.MAX_TIME_WINDOW
        };

        const isTheft = theftConditions.significantDrop && 
                       theftConditions.highPercentage && 
                       theftConditions.rapidDrop && 
                       theftConditions.withinTimeWindow;

        if (isTheft) {
            // Log theft anomaly
            await supabase
                .from('energy_rite_fuel_anomalies')
                .insert({
                    plate: plate,
                    anomaly_type: 'FUEL_THEFT',
                    anomaly_date: currentTime.toISOString(),
                    fuel_before: previousFuel,
                    fuel_after: currentFuel,
                    difference: -fuelDrop,
                    severity: 'HIGH',
                    status: 'pending',
                    anomaly_data: {
                        fuelDropPercentage: fuelDropPercentage.toFixed(2),
                        timeDiffMinutes: timeDiffMinutes.toFixed(2),
                        fuelDropRate: fuelDropRate.toFixed(2),
                        conditions: theftConditions
                    }
                });

            const theftInfo = {
                isTheft: true,
                vehicle: {
                    plate: plate,
                    pocsagstr: pocsagstr
                },
                theftDetails: {
                    previousFuel: previousFuel,
                    currentFuel: currentFuel,
                    fuelDrop: fuelDrop,
                    fuelDropPercentage: fuelDropPercentage.toFixed(2),
                    timeDiffMinutes: timeDiffMinutes.toFixed(2),
                    fuelDropRate: fuelDropRate.toFixed(2),
                    theftTime: currentTime
                },
                conditions: theftConditions
            };

            console.log(`🚨 FUEL THEFT DETECTED! Vehicle: ${plate} - Drop: ${fuelDrop.toFixed(1)}L (${fuelDropPercentage.toFixed(1)}%) in ${timeDiffMinutes.toFixed(1)} minutes`);

            return theftInfo;
        } else {
            return { 
                isTheft: false, 
                reason: 'Normal fuel consumption',
                details: {
                    fuelDrop: fuelDrop,
                    fuelDropPercentage: fuelDropPercentage.toFixed(2),
                    timeDiffMinutes: timeDiffMinutes.toFixed(2),
                    conditions: theftConditions
                }
            };
        }

    } catch (error) {
        console.error('Error detecting fuel theft:', error.message);
        return { isTheft: false, reason: 'Detection error', error: error.message };
    }
}

/**
 * Get theft statistics
 * @returns {Object} Theft statistics
 */
async function getTheftStatistics() {
    try {
        // Get total theft incidents
        const { data: theftData, error: theftError } = await supabase
            .from('energy_rite_fuel_anomalies')
            .select('*')
            .eq('anomaly_type', 'FUEL_THEFT');

        if (theftError) throw new Error(`Database error: ${theftError.message}`);

        // Get recent thefts (last 24 hours)
        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        const recentThefts = theftData.filter(theft => theft.anomaly_date >= yesterday);

        // Get unique vehicles with fuel data
        const { data: vehicleData, error: vehicleError } = await supabase
            .from('energy_rite_fuel_data')
            .select('plate')
            .not('plate', 'is', null);

        if (vehicleError) throw new Error(`Database error: ${vehicleError.message}`);

        const uniqueVehicles = [...new Set(vehicleData.map(v => v.plate))];

        return {
            statistics: {
                total_vehicles: uniqueVehicles.length,
                theft_incidents: theftData.length,
                recent_thefts: recentThefts.length
            },
            recentThefts: recentThefts.slice(0, 10).map(theft => ({
                plate: theft.plate,
                anomaly_date: theft.anomaly_date,
                fuel_before: theft.fuel_before,
                fuel_after: theft.fuel_after,
                difference: theft.difference
            }))
        };
    } catch (error) {
        console.error('Error getting theft statistics:', error.message);
        return { error: error.message };
    }
}

/**
 * Reset theft flag for a vehicle
 * @param {string} plate - Vehicle plate number
 */
async function resetTheftFlag(plate) {
    try {
        const { data, error } = await supabase
            .from('energy_rite_fuel_anomalies')
            .update({ status: 'resolved' })
            .eq('plate', plate)
            .eq('anomaly_type', 'FUEL_THEFT')
            .eq('status', 'pending');

        if (error) throw new Error(`Database error: ${error.message}`);

        if (data && data.length > 0) {
            console.log(`Theft flags reset for vehicle: ${plate}`);
            return { success: true, message: 'Theft flags reset successfully' };
        } else {
            return { success: false, message: 'No pending theft incidents found for vehicle' };
        }
    } catch (error) {
        console.error('Error resetting theft flag:', error.message);
        return { success: false, error: error.message };
    }
}

module.exports = {
    detectFuelTheft,
    getTheftStatistics,
    resetTheftFlag,
    THEFT_CONFIG
};