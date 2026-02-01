const { supabase } = require('../../supabase-client');
const ExcelReportGenerator = require('./energyRiteExcelReportGenerator');
const emailService = require('../../services/energy-rite/emailService');

class EnergyRiteReportDocumentsController {
  
  /**
   * Generate a new Excel report
   */
  async generateExcelReport(req, res) {
    try {
      const { 
        report_type = 'daily', 
        target_date = null,
        date = null,
        start_date = null,
        end_date = null,
        cost_code = null,
        site_id = null,
        month_type = 'previous'
      } = { ...req.body, ...req.query };
      
      const reportDate = date || target_date;
      
      console.log(`📅 Request params: type=${report_type}, date=${reportDate}, cost_code=${cost_code}, site_id=${site_id}`);
      
      console.log(`📊 Generating ${report_type} Excel report for ${reportDate || 'today'}...`);
      
      if (!['daily', 'weekly', 'monthly'].includes(report_type)) {
        return res.status(400).json({
          success: false,
          error: 'Invalid report type. Must be daily, weekly, or monthly.'
        });
      }
      
      const result = await ExcelReportGenerator.generateExcelReport(
        report_type, 
        reportDate, 
        cost_code,
        site_id,
        month_type,
        start_date,
        end_date
      );
      
      // Send email with the report
      try {
        const emailResult = await emailService.sendReportEmail({
          reportType: report_type,
          period: result.period,
          fileName: result.file_name,
          downloadUrl: result.download_url,
          costCode: cost_code,
          siteId: site_id,
          stats: result.stats
        });
        console.log(`📧 Report email sent to ${emailResult.recipients} recipients`);
        result.email = emailResult;
      } catch (emailError) {
        console.error(`❌ Failed to send report email: ${emailError.message}`);
        result.email = { 
          success: false, 
          error: emailError.message 
        };
      }
      
      res.status(200).json({
        success: true,
        message: `${report_type} Excel report generated successfully and email sent`,
        data: result
      });
      
    } catch (error) {
      console.error('❌ Error generating Excel report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate Excel report',
        message: error.message
      });
    }
  }
  
  /**
   * Get list of all generated reports
   */
  async getReportDocuments(req, res) {
    try {
      const { 
        report_type, 
        cost_code, 
        limit = 50, 
        offset = 0 
      } = req.query;
      
      let query = supabase
        .from('energy_rite_report_documents')
        .select('*');
      
      if (report_type) {
        query = query.eq('report_type', report_type);
      }
      
      if (cost_code) {
        query = query.eq('branch', cost_code);
      }
      
      const { data, error } = await query
        .order('created_at', { ascending: false })
        .range(parseInt(offset), parseInt(offset) + parseInt(limit) - 1);
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      // Get total count
      const { count, error: countError } = await supabase
        .from('energy_rite_report_documents')
        .select('*', { count: 'exact', head: true });
      
      if (countError) throw new Error(`Database error: ${countError.message}`);
      
      res.status(200).json({
        success: true,
        data: data,
        pagination: {
          total: count,
          limit: parseInt(limit),
          offset: parseInt(offset),
          has_more: parseInt(offset) + parseInt(limit) < count
        }
      });
      
    } catch (error) {
      console.error('❌ Error fetching report documents:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to fetch report documents',
        message: error.message
      });
    }
  }
  
  /**
   * Get specific report document
   */
  async getReportDocument(req, res) {
    try {
      const { id } = req.params;
      
      const { data, error } = await supabase
        .from('energy_rite_report_documents')
        .select('*')
        .eq('id', id)
        .single();
      
      if (error || !data) {
        return res.status(404).json({
          success: false,
          error: 'Report document not found'
        });
      }
      
      res.status(200).json({
        success: true,
        data: data
      });
      
    } catch (error) {
      console.error('❌ Error fetching report document:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to fetch report document',
        message: error.message
      });
    }
  }
  
  /**
   * Delete report document
   */
  async deleteReportDocument(req, res) {
    try {
      const { id } = req.params;
      
      const { data, error } = await supabase
        .from('energy_rite_report_documents')
        .delete()
        .eq('id', id)
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      if (data.length === 0) {
        return res.status(404).json({
          success: false,
          error: 'Report document not found'
        });
      }
      
      res.status(200).json({
        success: true,
        message: 'Report document deleted successfully',
        deleted_file: data[0].report_name
      });
      
    } catch (error) {
      console.error('❌ Error deleting report document:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to delete report document',
        message: error.message
      });
    }
  }
  
  /**
   * Get report generation statistics
   */
  async getReportStatistics(req, res) {
    try {
      const { data: allReports, error } = await supabase
        .from('energy_rite_report_documents')
        .select('report_type, created_at, branch');
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      // Calculate statistics
      const now = new Date();
      const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
      
      const byType = ['daily', 'weekly', 'monthly'].map(type => {
        const typeReports = allReports.filter(r => r.report_type === type);
        return {
          report_type: type,
          total_reports: typeReports.length,
          reports_last_7_days: typeReports.filter(r => new Date(r.created_at) > sevenDaysAgo).length,
          reports_last_30_days: typeReports.filter(r => new Date(r.created_at) > thirtyDaysAgo).length,
          latest_report_date: typeReports.length > 0 ? 
            Math.max(...typeReports.map(r => new Date(r.created_at).getTime())) : null
        };
      });
      
      const overall = {
        total_all_reports: allReports.length,
        unique_cost_codes: [...new Set(allReports.map(r => r.branch).filter(Boolean))].length,
        first_report_date: allReports.length > 0 ? 
          Math.min(...allReports.map(r => new Date(r.created_at).getTime())) : null,
        latest_report_date: allReports.length > 0 ? 
          Math.max(...allReports.map(r => new Date(r.created_at).getTime())) : null
      };
      
      res.status(200).json({
        success: true,
        data: {
          by_type: byType,
          overall: overall
        }
      });
      
    } catch (error) {
      console.error('❌ Error fetching report statistics:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to fetch report statistics',
        message: error.message
      });
    }
  }
  
  /**
   * Generate scheduled reports
   */
  async generateScheduledReports(req, res) {
    try {
      const results = [];
      const today = new Date();
      
      console.log('🕐 Starting scheduled report generation...');
      
      // Generate daily report for yesterday
      const yesterday = new Date(today);
      yesterday.setDate(today.getDate() - 1);
      
      try {
        const dailyReport = await ExcelReportGenerator.generateDailyReport(yesterday);
        results.push({ type: 'daily', ...dailyReport });
        console.log('✅ Daily report generated');
        
        try {
          const emailResult = await emailService.sendReportEmail({
            reportType: 'daily',
            period: dailyReport.period,
            fileName: dailyReport.file_name,
            downloadUrl: dailyReport.download_url,
            stats: dailyReport.stats
          });
          console.log(`📧 Daily report email sent to ${emailResult.recipients} recipients`);
          results[results.length - 1].email = emailResult;
        } catch (emailError) {
          console.error(`❌ Failed to send daily report email: ${emailError.message}`);
          results[results.length - 1].email = { 
            success: false, 
            error: emailError.message 
          };
        }
      } catch (error) {
        console.error('❌ Daily report failed:', error.message);
        results.push({ type: 'daily', success: false, error: error.message });
      }
      
      // Generate weekly report on Mondays
      if (today.getDay() === 1) {
        try {
          const weeklyReport = await ExcelReportGenerator.generateWeeklyReport(yesterday);
          results.push({ type: 'weekly', ...weeklyReport });
          console.log('✅ Weekly report generated');
        } catch (error) {
          console.error('❌ Weekly report failed:', error.message);
          results.push({ type: 'weekly', success: false, error: error.message });
        }
      }
      
      // Generate monthly report on 1st of month
      if (today.getDate() === 1) {
        const lastMonth = new Date(today);
        lastMonth.setMonth(today.getMonth() - 1);
        
        try {
          const monthlyReport = await ExcelReportGenerator.generateMonthlyReport(lastMonth);
          results.push({ type: 'monthly', ...monthlyReport });
          console.log('✅ Monthly report generated');
        } catch (error) {
          console.error('❌ Monthly report failed:', error.message);
          results.push({ type: 'monthly', success: false, error: error.message });
        }
      }
      
      res.status(200).json({
        success: true,
        message: 'Scheduled report generation completed',
        results: results
      });
      
    } catch (error) {
      console.error('❌ Error in scheduled report generation:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate scheduled reports',
        message: error.message
      });
    }
  }
}

module.exports = new EnergyRiteReportDocumentsController();