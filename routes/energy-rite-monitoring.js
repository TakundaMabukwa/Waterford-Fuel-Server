const express = require('express');
const router = express.Router();
const energyRiteMonitoringController = require('../controllers/energy-rite/energyRiteMonitoringController');

// Long running sites monitoring
router.get('/long-running', energyRiteMonitoringController.getLongRunningSites);

// Top usage sites
router.get('/top-usage', energyRiteMonitoringController.getTopUsageSites);

// Monitoring dashboard
router.get('/dashboard', energyRiteMonitoringController.getMonitoringDashboard);

module.exports = router;