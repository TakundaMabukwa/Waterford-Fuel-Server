const express = require('express');
const router = express.Router();
const energyRiteReportDocumentsController = require('../controllers/energy-rite/energyRiteReportDocumentsController');
const energyRiteCostCenterReportController = require('../controllers/energy-rite/energyRiteCostCenterReportController');
const energyRiteReportDocsController = require('../controllers/energy-rite/energyRiteReportDocsController');

// Generate Excel report
router.post('/generate', energyRiteReportDocumentsController.generateExcelReport);

// Get report documents
router.get('/documents', energyRiteReportDocumentsController.getReportDocuments);

// Get specific report document
router.get('/documents/:id', energyRiteReportDocumentsController.getReportDocument);

// Delete report document
router.delete('/documents/:id', energyRiteReportDocumentsController.deleteReportDocument);

// Get report statistics
router.get('/statistics', energyRiteReportDocumentsController.getReportStatistics);

// Generate scheduled reports
router.post('/generate-scheduled', energyRiteReportDocumentsController.generateScheduledReports);

// Cost center reports
router.get('/cost-centers', energyRiteCostCenterReportController.getAllCostCenters);
router.get('/cost-centers/daily', energyRiteCostCenterReportController.generateDailyReport);
router.get('/cost-centers/weekly', energyRiteCostCenterReportController.generateWeeklyReport);
router.get('/cost-centers/monthly', energyRiteCostCenterReportController.generateMonthlyReport);
router.get('/cost-centers/all', energyRiteCostCenterReportController.generateAllReports);
router.get('/cost-centers/stored', energyRiteCostCenterReportController.getStoredReports);

// Report docs management
router.get('/docs', energyRiteReportDocsController.getReportDocuments);
router.get('/docs/download/:id', energyRiteReportDocsController.downloadReportDocument);
router.post('/docs/generate', energyRiteReportDocsController.generateManualReport);
router.get('/docs/statistics', energyRiteReportDocsController.getReportStatistics);
router.delete('/docs/:id', energyRiteReportDocsController.deleteReportDocument);

module.exports = router;