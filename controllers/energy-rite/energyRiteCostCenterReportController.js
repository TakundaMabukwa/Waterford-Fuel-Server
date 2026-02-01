const { supabase } = require('../../supabase-client');
const ExcelReportGenerator = require('./energyRiteExcelReportGenerator');

/**
 * Energy Rite Cost Center Report Controller
 */

class EnergyRiteCostCenterReportController {
  
  /**
   * Get all cost centers
   */
  async getAllCostCenters(req, res) {
    try {
      // Get unique cost codes from fuel data
      const { data, error } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate')
        .not('plate', 'is', null);
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      // Extract unique plates as cost centers
      const uniquePlates = [...new Set(data.map(item => item.plate))];
      const costCenters = uniquePlates.map(plate => ({
        cost_code: plate,
        company_name: 'EnergyRite'
      }));
      
      return res.status(200).json({
        success: true,
        message: `Retrieved ${costCenters.length} cost centers`,
        data: costCenters
      });
      
    } catch (error) {
      console.error('❌ Error fetching cost centers:', error);
      return res.status(500).json({
        success: false,
        error: 'Failed to fetch cost centers',
        message: error.message
      });
    }
  }

  /**
   * Generate daily report for specified cost center or all
   */
  async generateDailyReport(req, res) {
    try {
      const { date, cost_code } = req.query;
      
      console.log(`📊 Generating daily report for ${cost_code || 'all cost centers'} on ${date || 'today'}`);
      
      const targetDate = date ? new Date(date) : new Date();
      
      const result = await ExcelReportGenerator.generateDailyReport(
        targetDate, 
        cost_code || null
      );
      
      return res.status(200).json({
        success: true,
        message: `Daily report generated successfully for ${cost_code || 'all cost centers'}`,
        data: result
      });
      
    } catch (error) {
      console.error('❌ Error generating daily report:', error);
      return res.status(500).json({
        success: false,
        error: 'Failed to generate daily report',
        message: error.message
      });
    }
  }
  
  /**
   * Generate weekly report for specified cost center or all
   */
  async generateWeeklyReport(req, res) {
    try {
      const { date, cost_code } = req.query;
      
      console.log(`📊 Generating weekly report for ${cost_code || 'all cost centers'} ending on ${date || 'today'}`);
      
      const targetDate = date ? new Date(date) : new Date();
      
      const result = await ExcelReportGenerator.generateWeeklyReport(
        targetDate, 
        cost_code || null
      );
      
      return res.status(200).json({
        success: true,
        message: `Weekly report generated successfully for ${cost_code || 'all cost centers'}`,
        data: result
      });
      
    } catch (error) {
      console.error('❌ Error generating weekly report:', error);
      return res.status(500).json({
        success: false,
        error: 'Failed to generate weekly report',
        message: error.message
      });
    }
  }
  
  /**
   * Generate monthly report for specified cost center or all
   */
  async generateMonthlyReport(req, res) {
    try {
      const { date, cost_code } = req.query;
      
      console.log(`📊 Generating monthly report for ${cost_code || 'all cost centers'} for month ending ${date || 'today'}`);
      
      const targetDate = date ? new Date(date) : new Date();
      
      const result = await ExcelReportGenerator.generateMonthlyReport(
        targetDate, 
        cost_code || null
      );
      
      return res.status(200).json({
        success: true,
        message: `Monthly report generated successfully for ${cost_code || 'all cost centers'}`,
        data: result
      });
      
    } catch (error) {
      console.error('❌ Error generating monthly report:', error);
      return res.status(500).json({
        success: false,
        error: 'Failed to generate monthly report',
        message: error.message
      });
    }
  }
  
  /**
   * Generate all report types for a specific cost center or all centers
   */
  async generateAllReports(req, res) {
    try {
      const { date, cost_code } = req.query;
      
      console.log(`📊 Generating all reports for ${cost_code || 'all cost centers'}`);
      
      const targetDate = date ? new Date(date) : new Date();
      
      // Generate all reports in parallel
      const [dailyReport, weeklyReport, monthlyReport] = await Promise.all([
        ExcelReportGenerator.generateDailyReport(targetDate, cost_code || null),
        ExcelReportGenerator.generateWeeklyReport(targetDate, cost_code || null),
        ExcelReportGenerator.generateMonthlyReport(targetDate, cost_code || null)
      ]);
      
      return res.status(200).json({
        success: true,
        message: `All reports generated successfully for ${cost_code || 'all cost centers'}`,
        data: {
          dailyReport,
          weeklyReport,
          monthlyReport
        }
      });
      
    } catch (error) {
      console.error('❌ Error generating all reports:', error);
      return res.status(500).json({
        success: false,
        error: 'Failed to generate reports',
        message: error.message
      });
    }
  }
  
  /**
   * Get stored reports with filters
   */
  async getStoredReports(req, res) {
    try {
      const { cost_code, report_type, limit = 50, days = 30 } = req.query;
      
      console.log(`📋 Fetching stored reports for ${cost_code || 'all cost centers'}`);
      
      let query = supabase
        .from('energy_rite_generated_reports')
        .select('*')
        .eq('status', 'generated')
        .gte('created_at', new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000).toISOString())
        .order('created_at', { ascending: false })
        .limit(parseInt(limit));
      
      if (cost_code) {
        query = query.eq('cost_code', cost_code);
      }
      
      if (report_type) {
        query = query.eq('report_type', report_type);
      }
      
      const { data, error } = await query;
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      return res.status(200).json({
        success: true,
        message: `Retrieved ${data.length} stored reports`,
        data: data,
        filters: {
          cost_code: cost_code || 'all',
          report_type: report_type || 'all',
          days: parseInt(days),
          limit: parseInt(limit)
        }
      });
      
    } catch (error) {
      console.error('❌ Error fetching stored reports:', error);
      return res.status(500).json({
        success: false,
        error: 'Failed to fetch stored reports',
        message: error.message
      });
    }
  }
}

module.exports = new EnergyRiteCostCenterReportController();