const express = require('express');
const router = express.Router();
const reportDistributionController = require('../controllers/energy-rite/energyRiteReportDistributionController');

// Distribute reports by cost code
router.post('/daily', reportDistributionController.distributeDailyReports);
router.post('/weekly', reportDistributionController.distributeWeeklyReports);
router.post('/monthly', reportDistributionController.distributeMonthlyReports);
router.post('/activity', reportDistributionController.distributeActivityReports);

// Get email recipients grouped by cost code
router.get('/recipients', reportDistributionController.getEmailRecipientsByCostCode);

// Test distribution for specific cost code
router.post('/test', reportDistributionController.testDistribution);

module.exports = router;