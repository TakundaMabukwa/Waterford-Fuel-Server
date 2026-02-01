const express = require('express');
const router = express.Router();
const costCenterController = require('../controllers/energy-rite/energyRiteCostCenterController');

// Get accessible cost centers for user
router.get('/accessible', costCenterController.getAccessibleCostCenters);

// Get cost center dropdown options
router.get('/dropdown', costCenterController.getCostCenterDropdown);

// Validate cost center access
router.get('/validate', costCenterController.validateAccess);

module.exports = router;