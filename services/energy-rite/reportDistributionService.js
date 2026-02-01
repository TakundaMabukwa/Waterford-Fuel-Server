const { supabase } = require('../../supabase-client');
const emailService = require('./emailService');
const excelReportGenerator = require('../../controllers/energy-rite/energyRiteExcelReportGenerator');
const activityExcelController = require('../../controllers/energy-rite/energyRiteActivityExcelReportController');

class ReportDistributionService {
  
  /**
   * Get all email recipients with their cost codes
   */
  async getEmailRecipients() {
    try {
      const { data, error } = await supabase
        .from('energyrite_emails')
        .select('id, email, recipient_name, branch, cost_code, email_type, status')
        .eq('status', 'active')
        .order('branch');
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      return data || [];
    } catch (error) {
      console.error('Error getting email recipients:', error);
      return [];
    }
  }

  /**
   * Generate and distribute reports by cost code
   */
  async distributeReportsByCostCode(reportType = 'daily', targetDate = null) {
    try {
      console.log(`🚀 Starting ${reportType} report distribution...`);
      
      // Get all email recipients
      const recipients = await this.getEmailRecipients();
      if (recipients.length === 0) {
        console.log('⚠️ No email recipients found');
        return { success: false, message: 'No recipients found' };
      }

      // Group recipients by cost code
      const recipientsByCostCode = {};
      recipients.forEach(recipient => {
        const costCode = recipient.cost_code || recipient.branch || 'ALL';
        if (!recipientsByCostCode[costCode]) {
          recipientsByCostCode[costCode] = [];
        }
        recipientsByCostCode[costCode].push(recipient);
      });

      console.log(`📊 Found ${Object.keys(recipientsByCostCode).length} cost code groups`);

      const results = [];

      // Generate and send reports for each cost code
      for (const [costCode, costCodeRecipients] of Object.entries(recipientsByCostCode)) {
        try {
          console.log(`📈 Generating ${reportType} report for cost code: ${costCode}`);
          
          // Generate Excel report for this cost code
          const reportResult = await excelReportGenerator.generateExcelReport(
            reportType, 
            targetDate, 
            costCode === 'ALL' ? null : costCode
          );

          if (reportResult.success) {
            // Send email to all recipients for this cost code
            const emailAddresses = costCodeRecipients.map(r => r.email);
            
            const emailResult = await emailService.sendReportEmail({
              reportType: reportType,
              period: reportResult.period,
              fileName: reportResult.file_name,
              downloadUrl: reportResult.download_url,
              costCode: costCode === 'ALL' ? null : costCode,
              stats: reportResult.stats
            });

            results.push({
              cost_code: costCode,
              recipients: emailAddresses,
              report_generated: true,
              email_sent: emailResult.success,
              file_name: reportResult.file_name,
              download_url: reportResult.download_url
            });

            console.log(`✅ ${reportType} report sent to ${emailAddresses.length} recipients for ${costCode}`);
          } else {
            results.push({
              cost_code: costCode,
              recipients: costCodeRecipients.map(r => r.email),
              report_generated: false,
              email_sent: false,
              error: 'Report generation failed'
            });
          }

        } catch (error) {
          console.error(`❌ Error processing cost code ${costCode}:`, error.message);
          results.push({
            cost_code: costCode,
            recipients: costCodeRecipients.map(r => r.email),
            report_generated: false,
            email_sent: false,
            error: error.message
          });
        }
      }

      const successCount = results.filter(r => r.email_sent).length;
      console.log(`🎉 Distribution complete: ${successCount}/${results.length} cost codes processed successfully`);

      return {
        success: true,
        message: `Reports distributed to ${successCount} cost code groups`,
        results: results,
        total_cost_codes: results.length,
        successful_distributions: successCount
      };

    } catch (error) {
      console.error('❌ Error in report distribution:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Generate and distribute activity reports by cost code
   */
  async distributeActivityReportsByCostCode(startDate = null, endDate = null) {
    try {
      console.log('🚀 Starting activity report distribution...');
      
      // Get all email recipients
      const recipients = await this.getEmailRecipients();
      if (recipients.length === 0) {
        console.log('⚠️ No email recipients found');
        return { success: false, message: 'No recipients found' };
      }

      // Group recipients by cost code
      const recipientsByCostCode = {};
      recipients.forEach(recipient => {
        const costCode = recipient.cost_code || 'ALL';
        if (!recipientsByCostCode[costCode]) {
          recipientsByCostCode[costCode] = [];
        }
        recipientsByCostCode[costCode].push(recipient);
      });

      const results = [];

      // Generate and send activity reports for each cost code
      for (const [costCode, costCodeRecipients] of Object.entries(recipientsByCostCode)) {
        try {
          console.log(`📊 Generating activity report for cost code: ${costCode}`);
          
          // Create mock request object for activity report generation
          const mockReq = {
            query: {
              costCode: costCode === 'ALL' ? null : costCode,
              startDate: startDate,
              endDate: endDate
            }
          };

          // Generate activity Excel report
          const reportResult = await new Promise((resolve, reject) => {
            const mockRes = {
              status: (code) => ({
                json: (data) => {
                  if (data.success) {
                    resolve(data.data);
                  } else {
                    reject(new Error(data.error || 'Report generation failed'));
                  }
                }
              })
            };

            activityExcelController.generateActivityExcelReport(mockReq, mockRes);
          });

          // Send email to all recipients for this cost code
          const emailAddresses = costCodeRecipients.map(r => r.email);
          
          const emailResult = await emailService.sendReportEmail({
            reportType: 'activity',
            period: `${reportResult.period.start_date} to ${reportResult.period.end_date}`,
            fileName: reportResult.file_name,
            downloadUrl: reportResult.download_url,
            costCode: costCode === 'ALL' ? null : costCode,
            stats: {
              total_sites: reportResult.total_sites,
              total_sessions: 0,
              total_operating_hours: 0
            }
          });

          results.push({
            cost_code: costCode,
            recipients: emailAddresses,
            report_generated: true,
            email_sent: emailResult.success,
            file_name: reportResult.file_name,
            download_url: reportResult.download_url
          });

          console.log(`✅ Activity report sent to ${emailAddresses.length} recipients for ${costCode}`);

        } catch (error) {
          console.error(`❌ Error processing activity report for ${costCode}:`, error.message);
          results.push({
            cost_code: costCode,
            recipients: costCodeRecipients.map(r => r.email),
            report_generated: false,
            email_sent: false,
            error: error.message
          });
        }
      }

      const successCount = results.filter(r => r.email_sent).length;
      console.log(`🎉 Activity report distribution complete: ${successCount}/${results.length} cost codes processed`);

      return {
        success: true,
        message: `Activity reports distributed to ${successCount} cost code groups`,
        results: results,
        total_cost_codes: results.length,
        successful_distributions: successCount
      };

    } catch (error) {
      console.error('❌ Error in activity report distribution:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Schedule daily report distribution
   */
  async scheduleDailyReports() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    
    return await this.distributeReportsByCostCode('daily', yesterday);
  }

  /**
   * Schedule weekly report distribution
   */
  async scheduleWeeklyReports() {
    const lastWeek = new Date();
    lastWeek.setDate(lastWeek.getDate() - 7);
    
    return await this.distributeReportsByCostCode('weekly', lastWeek);
  }

  /**
   * Schedule monthly report distribution
   */
  async scheduleMonthlyReports() {
    const lastMonth = new Date();
    lastMonth.setMonth(lastMonth.getMonth() - 1);
    
    return await this.distributeReportsByCostCode('monthly', lastMonth);
  }
}

module.exports = new ReportDistributionService();