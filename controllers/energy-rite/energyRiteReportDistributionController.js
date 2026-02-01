const reportDistributionService = require('../../services/energy-rite/reportDistributionService');

class EnergyRiteReportDistributionController {

  /**
   * Distribute daily reports to all cost codes
   */
  async distributeDailyReports(req, res) {
    try {
      const { targetDate } = req.query;
      const result = await reportDistributionService.distributeReportsByCostCode('daily', targetDate);
      
      res.status(200).json({
        success: result.success,
        message: result.message,
        data: result.results,
        stats: {
          total_cost_codes: result.total_cost_codes,
          successful_distributions: result.successful_distributions
        }
      });
    } catch (error) {
      console.error('Error distributing daily reports:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  /**
   * Distribute weekly reports to all cost codes
   */
  async distributeWeeklyReports(req, res) {
    try {
      const { targetDate } = req.query;
      const result = await reportDistributionService.distributeReportsByCostCode('weekly', targetDate);
      
      res.status(200).json({
        success: result.success,
        message: result.message,
        data: result.results,
        stats: {
          total_cost_codes: result.total_cost_codes,
          successful_distributions: result.successful_distributions
        }
      });
    } catch (error) {
      console.error('Error distributing weekly reports:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  /**
   * Distribute monthly reports to all cost codes
   */
  async distributeMonthlyReports(req, res) {
    try {
      const { targetDate } = req.query;
      const result = await reportDistributionService.distributeReportsByCostCode('monthly', targetDate);
      
      res.status(200).json({
        success: result.success,
        message: result.message,
        data: result.results,
        stats: {
          total_cost_codes: result.total_cost_codes,
          successful_distributions: result.successful_distributions
        }
      });
    } catch (error) {
      console.error('Error distributing monthly reports:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  /**
   * Distribute activity reports to all cost codes
   */
  async distributeActivityReports(req, res) {
    try {
      const { startDate, endDate } = req.query;
      const result = await reportDistributionService.distributeActivityReportsByCostCode(startDate, endDate);
      
      res.status(200).json({
        success: result.success,
        message: result.message,
        data: result.results,
        stats: {
          total_cost_codes: result.total_cost_codes,
          successful_distributions: result.successful_distributions
        }
      });
    } catch (error) {
      console.error('Error distributing activity reports:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  /**
   * Get email recipients grouped by cost code
   */
  async getEmailRecipientsByCostCode(req, res) {
    try {
      const recipients = await reportDistributionService.getEmailRecipients();
      
      // Group by cost code
      const groupedRecipients = {};
      recipients.forEach(recipient => {
        const costCode = recipient.cost_code || 'ALL';
        if (!groupedRecipients[costCode]) {
          groupedRecipients[costCode] = [];
        }
        groupedRecipients[costCode].push({
          id: recipient.id,
          email: recipient.email,
          name: recipient.recipient_name,
          email_type: recipient.email_type
        });
      });

      res.status(200).json({
        success: true,
        data: groupedRecipients,
        total_cost_codes: Object.keys(groupedRecipients).length,
        total_recipients: recipients.length
      });
    } catch (error) {
      console.error('Error getting email recipients by cost code:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  /**
   * Test report distribution for a specific cost code
   */
  async testDistribution(req, res) {
    try {
      const { costCode, reportType = 'daily' } = req.query;
      
      if (!costCode) {
        return res.status(400).json({
          success: false,
          error: 'Cost code is required for testing'
        });
      }

      // Get recipients for this cost code only
      const allRecipients = await reportDistributionService.getEmailRecipients();
      const testRecipients = allRecipients.filter(r => r.cost_code === costCode);
      
      if (testRecipients.length === 0) {
        return res.status(404).json({
          success: false,
          error: `No recipients found for cost code: ${costCode}`
        });
      }

      console.log(`🧪 Testing ${reportType} report distribution for cost code: ${costCode}`);
      
      // Generate single report for testing
      const excelReportGenerator = require('../../controllers/energy-rite/energyRiteExcelReportGenerator');
      const reportResult = await excelReportGenerator.generateExcelReport(reportType, null, costCode);

      if (reportResult.success) {
        const emailService = require('../../services/energy-rite/emailService');
        const emailResult = await emailService.sendReportEmail({
          reportType: reportType,
          period: reportResult.period,
          fileName: reportResult.file_name,
          downloadUrl: reportResult.download_url,
          costCode: costCode,
          stats: reportResult.stats
        });

        res.status(200).json({
          success: true,
          message: `Test ${reportType} report sent successfully`,
          data: {
            cost_code: costCode,
            recipients: testRecipients.map(r => r.email),
            report_generated: true,
            email_sent: emailResult.success,
            file_name: reportResult.file_name,
            download_url: reportResult.download_url
          }
        });
      } else {
        res.status(500).json({
          success: false,
          error: 'Failed to generate test report'
        });
      }

    } catch (error) {
      console.error('Error in test distribution:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
}

module.exports = new EnergyRiteReportDistributionController();