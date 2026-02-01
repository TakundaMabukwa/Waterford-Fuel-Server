const express = require('express');
const router = express.Router();
const energyRiteExecutiveDashboardController = require('../controllers/energy-rite/energyRiteExecutiveDashboardController');

// Get executive dashboard with high-level KPIs
router.get('/', energyRiteExecutiveDashboardController.getExecutiveDashboard);

module.exports = router;