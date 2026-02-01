const express = require('express');
const router = express.Router();
const energyRiteFuelAnalysisController = require('../controllers/energy-rite/energyRiteFuelAnalysisController');

// Detect fuel anomalies
router.post('/anomalies/detect', energyRiteFuelAnalysisController.detectFuelAnomalies);

// Get fuel anomalies
router.get('/anomalies', energyRiteFuelAnalysisController.getFuelAnomalies);

// Resolve anomaly
router.put('/anomalies/:id/resolve', energyRiteFuelAnalysisController.resolveAnomaly);

// Get fuel consumption analysis
router.get('/consumption', energyRiteFuelAnalysisController.getFuelConsumptionAnalysis);

module.exports = router;