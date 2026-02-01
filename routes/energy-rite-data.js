const express = require('express');
const router = express.Router();
const energyRiteDataController = require('../controllers/energy-rite/energyRiteDataController');

// Get all vehicles with optional filters (no limit by default)
// GET /api/energy-rite/vehicles?plate=CARLTONVIL&hasFuel=true&isActive=true
// GET /api/energy-rite/vehicles?limit=100 (to limit results)
router.get('/vehicles', energyRiteDataController.getAllVehicles);

// Get a specific vehicle by plate
// GET /api/energy-rite/vehicles/CARLTONVIL
router.get('/vehicles/:plate', energyRiteDataController.getVehicleByPlate);

// Get vehicles with fuel data
// GET /api/energy-rite/fuel?limit=50&minLevel=0&maxLevel=1000
router.get('/fuel', energyRiteDataController.getVehiclesWithFuel);

// Get dashboard statistics
// GET /api/energy-rite/stats
router.get('/stats', energyRiteDataController.getDashboardStats);

// Get real-time updates via Server-Sent Events
// GET /api/energy-rite/realtime
router.get('/realtime', energyRiteDataController.getRealTimeUpdates);

// Get recent activity
// GET /api/energy-rite/activity?hours=24&limit=50
router.get('/activity', energyRiteDataController.getRecentActivity);

// Get fuel consumption analysis
// GET /api/energy-rite/fuel-analysis?days=7
router.get('/fuel-analysis', energyRiteDataController.getFuelAnalysis);

module.exports = router;