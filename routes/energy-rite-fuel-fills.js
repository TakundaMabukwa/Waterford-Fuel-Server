const express = require('express');
const router = express.Router();
const energyRiteFuelFillController = require('../controllers/energy-rite/energyRiteFuelFillController');

// Get all fuel fills with filters
router.get('/', energyRiteFuelFillController.getAllFuelFills);

// Get fuel fill statistics
router.get('/statistics', energyRiteFuelFillController.getFuelFillStats);

// Get daily fuel fill summary
router.get('/daily-summary', energyRiteFuelFillController.getDailyFillSummary);

// Get fills for reports integration
router.get('/for-reports', energyRiteFuelFillController.getFillsForReports);

// Get fuel fill history for a specific vehicle
router.get('/vehicle/:plate', energyRiteFuelFillController.getVehicleFillHistory);

module.exports = router;