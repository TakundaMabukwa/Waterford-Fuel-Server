const express = require('express');
const router = express.Router();
const cumulativeSnapshotsController = require('../controllers/energy-rite/energyRiteCumulativeSnapshotsController');

// Get cumulative snapshots for a specific month
// GET /api/energy-rite/cumulative-snapshots/2025/11
router.get('/:year/:month', cumulativeSnapshotsController.getCumulativeMonthlySnapshots);

module.exports = router;