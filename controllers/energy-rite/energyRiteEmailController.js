const { supabase } = require('../../supabase-client');

class EnergyRiteEmailController {
  /**
   * Get all configured emails
   */
  async getAllEmails(req, res) {
    try {
      const { data, error } = await supabase
        .from('energyrite_emails')
        .select('id, email, branch, cost_code, site_id, status, email_type, recipient_name, created_at, updated_at')
        .order('id', { ascending: true });
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.status(200).json({
        success: true,
        data: data
      });
    } catch (error) {
      console.error('Error getting emails:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
  
  /**
   * Add new email
   */
  async addEmail(req, res) {
    try {
      const { email, cost_code, site_id } = req.body;
      
      if (!email) {
        return res.status(400).json({
          success: false,
          error: 'Email is required'
        });
      }
      
      // Validate email format
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email)) {
        return res.status(400).json({
          success: false,
          error: 'Invalid email format'
        });
      }
      
      const { data, error } = await supabase
        .from('energyrite_emails')
        .upsert({
          email: email,
          cost_code: cost_code || null,
          site_id: site_id || null,
          branch: cost_code || null,
          status: 'active',
          email_type: 'report'
        }, {
          onConflict: 'email,cost_code'
        })
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.status(201).json({
        success: true,
        data: data[0],
        message: 'Email added successfully'
      });
    } catch (error) {
      console.error('Error adding email:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
  
  /**
   * Update email
   */
  async updateEmail(req, res) {
    try {
      const { id } = req.params;
      const { email, cost_code, site_id } = req.body;
      
      if (!email) {
        return res.status(400).json({
          success: false,
          error: 'Email is required'
        });
      }
      
      // Validate email format
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email)) {
        return res.status(400).json({
          success: false,
          error: 'Invalid email format'
        });
      }
      
      const { data, error } = await supabase
        .from('energyrite_emails')
        .update({
          email: email,
          cost_code: cost_code || null,
          site_id: site_id || null,
          branch: cost_code || null,
          updated_at: new Date().toISOString()
        })
        .eq('id', id)
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      if (data.length === 0) {
        return res.status(404).json({
          success: false,
          error: 'Email not found'
        });
      }
      
      res.status(200).json({
        success: true,
        data: data[0],
        message: 'Email updated successfully'
      });
    } catch (error) {
      console.error('Error updating email:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
  
  /**
   * Delete email
   */
  async deleteEmail(req, res) {
    try {
      const { id } = req.params;
      
      const { data, error } = await supabase
        .from('energyrite_emails')
        .delete()
        .eq('id', id)
        .select('id');
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      if (data.length === 0) {
        return res.status(404).json({
          success: false,
          error: 'Email not found'
        });
      }
      
      res.status(200).json({
        success: true,
        message: 'Email deleted successfully'
      });
    } catch (error) {
      console.error('Error deleting email:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
  
  /**
   * Get emails by cost code
   */
  async getEmailsByCostCode(req, res) {
    try {
      const { cost_code } = req.params;
      
      const { data, error } = await supabase
        .from('energyrite_emails')
        .select('id, email, branch, cost_code, site_id, status, email_type, recipient_name, created_at, updated_at')
        .eq('cost_code', cost_code)
        .order('id', { ascending: true });
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.status(200).json({
        success: true,
        data: data
      });
    } catch (error) {
      console.error('Error getting emails by cost code:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
}

module.exports = new EnergyRiteEmailController();