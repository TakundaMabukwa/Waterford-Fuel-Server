const express = require('express');
const router = express.Router();
const enhancedExecutiveDashboardController = require('../controllers/energy-rite/enhancedExecutiveDashboardController');

// Get enhanced executive dashboard with daily metrics and continuous operations
router.get('/', enhancedExecutiveDashboardController.getExecutiveDashboard);

module.exports = router;