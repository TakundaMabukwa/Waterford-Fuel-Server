const express = require('express');
const router = express.Router();
const energyRiteActivityExcelReportController = require('../controllers/energy-rite/energyRiteActivityExcelReportController');

// Generate activity Excel report
router.get('/generate', energyRiteActivityExcelReportController.generateActivityExcelReport);

module.exports = router;