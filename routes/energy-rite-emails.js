const express = require('express');
const router = express.Router();
const energyRiteEmailController = require('../controllers/energy-rite/energyRiteEmailController');

// Get all emails
router.get('/', energyRiteEmailController.getAllEmails);

// Add new email
router.post('/', energyRiteEmailController.addEmail);

// Update email
router.put('/:id', energyRiteEmailController.updateEmail);

// Delete email
router.delete('/:id', energyRiteEmailController.deleteEmail);

// Get emails by cost code
router.get('/cost-code/:cost_code', energyRiteEmailController.getEmailsByCostCode);

module.exports = router;