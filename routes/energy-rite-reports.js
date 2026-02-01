const express = require('express');
const router = express.Router();
const energyRiteReportsController = require('../controllers/energy-rite/energyRiteReportsController');

// Get today's sessions
router.get('/today', energyRiteReportsController.getTodaysSessions);

// Daily Report
router.get('/daily', energyRiteReportsController.getDailyReport);

// Weekly Report
router.get('/weekly', energyRiteReportsController.getWeeklyReport);

// Monthly Report
router.get('/monthly', energyRiteReportsController.getMonthlyReport);

// Activity Report
router.get('/activity', energyRiteReportsController.getActivityReport);

// Download Activity Report
router.get('/activity/download', energyRiteReportsController.downloadActivityReport);

// Snapshot Data with Cost Code Filtering
router.get('/snapshots', energyRiteReportsController.getSnapshotData);



// Generate daily report data
router.post('/daily/generate', energyRiteReportsController.generateDailyReportData);

// Generate monthly report data
router.post('/monthly/generate', energyRiteReportsController.generateMonthlyReportData);

// Generate reports by cost code
router.get('/generate/daily', energyRiteReportsController.generateDailyReportByCostCode);
router.get('/generate/weekly', energyRiteReportsController.generateWeeklyReportByCostCode);
router.get('/generate/monthly', energyRiteReportsController.generateMonthlyReportByCostCode);

module.exports = router;