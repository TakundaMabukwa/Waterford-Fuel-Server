const express = require('express');
const router = express.Router();
const energyRiteActivityReportController = require('../controllers/energy-rite/energyRiteActivityReportController');

// Get activity report for all sites or specific site
router.get('/', energyRiteActivityReportController.getActivityReport);

// Get peak usage analysis
router.get('/peak-usage', energyRiteActivityReportController.getPeakUsageAnalysis);

// Get site comparison report
router.get('/site-comparison', energyRiteActivityReportController.getSiteComparison);

// Get comprehensive activity dashboard
router.get('/dashboard', energyRiteActivityReportController.getActivityDashboard);

// Take manual activity snapshot
router.post('/snapshot', energyRiteActivityReportController.takeSnapshot);

module.exports = router;