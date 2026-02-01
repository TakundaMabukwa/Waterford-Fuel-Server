const express = require('express');
const router = express.Router();
const reportStorageController = require('../controllers/energy-rite/energyRiteReportStorageController');

// Generate and store reports
router.post('/generate/daily', reportStorageController.generateAndStoreDailyReport);
router.post('/generate/weekly', reportStorageController.generateAndStoreWeeklyReport);
router.post('/generate/monthly', reportStorageController.generateAndStoreMonthlyReport);

// Get stored reports list
router.get('/stored', reportStorageController.getStoredReports);

// Serve report files
router.get('/files/:fileName', reportStorageController.serveReportFile);

module.exports = router;