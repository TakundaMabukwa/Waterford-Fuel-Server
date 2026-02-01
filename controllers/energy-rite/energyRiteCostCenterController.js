const costCenterAccess = require('../../helpers/cost-center-access');
const { supabase } = require('../../supabase-client');

class EnergyRiteCostCenterController {
  
  /**
   * Get accessible cost centers for user
   */
  async getAccessibleCostCenters(req, res) {
    try {
      const { userCostCode } = req.query;
      
      if (!userCostCode) {
        return res.status(400).json({ 
          success: false, 
          message: 'User cost code required' 
        });
      }
      
      const accessibleCostCenters = await costCenterAccess.getAccessibleCostCenters(userCostCode);
      
      res.json({
        success: true,
        userCostCode,
        accessibleCostCenters,
        count: accessibleCostCenters.length
      });
      
    } catch (error) {
      console.error('❌ Error getting accessible cost centers:', error);
      res.status(500).json({ 
        success: false, 
        message: 'Failed to get accessible cost centers' 
      });
    }
  }
  
  /**
   * Get cost center dropdown options
   */
  async getCostCenterDropdown(req, res) {
    try {
      const { userCostCode } = req.query;
      
      if (!userCostCode) {
        return res.status(400).json({ 
          success: false, 
          message: 'User cost code required' 
        });
      }
      
      const dropdownOptions = await costCenterAccess.getCostCenterDropdownOptions(userCostCode);
      
      res.json({
        success: true,
        userCostCode,
        options: dropdownOptions
      });
      
    } catch (error) {
      console.error('❌ Error getting cost center dropdown:', error);
      res.status(500).json({ 
        success: false, 
        message: 'Failed to get cost center dropdown' 
      });
    }
  }
  
  /**
   * Validate cost center access
   */
  async validateAccess(req, res) {
    try {
      const { userCostCode, targetCostCode } = req.query;
      
      if (!userCostCode || !targetCostCode) {
        return res.status(400).json({ 
          success: false, 
          message: 'Both user and target cost codes required' 
        });
      }
      
      const hasAccess = await costCenterAccess.hasAccessToCostCenter(userCostCode, targetCostCode);
      
      res.json({
        success: true,
        userCostCode,
        targetCostCode,
        hasAccess
      });
      
    } catch (error) {
      console.error('❌ Error validating cost center access:', error);
      res.status(500).json({ 
        success: false, 
        message: 'Failed to validate access' 
      });
    }
  }
}

module.exports = new EnergyRiteCostCenterController();