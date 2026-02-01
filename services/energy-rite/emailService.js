/**
 * Energy Rite Email Service
 * Handles sending report emails to configured recipients
 */
const notificationapi = require('notificationapi-node-server-sdk').default;
const { supabase } = require('../../supabase-client');

class EnergyRiteEmailService {
  constructor() {
    this.setupNotificationAPI();
  }

  /**
   * Setup NotificationAPI
   */
  setupNotificationAPI() {
    try {
      notificationapi.init(
        process.env.NOTIFICATIONAPI_CLIENT_ID,
        process.env.NOTIFICATIONAPI_CLIENT_SECRET
      );
      console.log('✅ NotificationAPI initialized');
    } catch (error) {
      console.error('❌ NotificationAPI initialization failed:', error.message);
    }
  }

  /**
   * Get email recipients based on cost code
   */
  async getEmailRecipients(costCode = null) {
    try {
      let query = supabase
        .from('energyrite_emails')
        .select('email, branch')
        .eq('status', 'active');

      if (costCode) {
        query = query.or(`cost_code.is.null,cost_code.eq.${costCode}`);
      }

      const { data, error } = await query.order('email');
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      return data;
    } catch (error) {
      console.error('Error getting email recipients:', error.message);
      return [];
    }
  }

  /**
   * Send email with report attachment
   */
  async sendReportEmail(options) {
    const { 
      reportType, 
      period, 
      fileName, 
      downloadUrl, 
      costCode, 
      stats 
    } = options;

    try {
      const recipients = await this.getEmailRecipients(costCode);
      if (recipients.length === 0) {
        console.log('⚠️ No email recipients found for reports');
        return { success: false, message: 'No recipients found' };
      }

      const emails = recipients.map(r => r.email);
      const reportTypeName = reportType.charAt(0).toUpperCase() + reportType.slice(1);
      
      const emailHTML = `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <div style="background-color: #1e3a5f; color: white; padding: 20px; text-align: center;">
            <img src="https://energyrite.co.za/wp-content/uploads/energyrite_logo_lite.png" alt="EnergyRite Logo" style="max-width: 200px; height: auto; margin-bottom: 10px;">
            <h1>EnergyRite ${reportTypeName} Report</h1>
            <p>${period}</p>
          </div>
          <div style="padding: 20px; background-color: #f8f9fa; color: #333;">
            <p><strong>Hello,</strong></p>
            <p>Your EnergyRite ${reportTypeName} report for ${period} is ready for download.</p>
            
            <div style="background: white; padding: 15px; margin: 15px 0; border-radius: 5px; border-left: 4px solid #1e3a5f;">
              <p><strong>Sites:</strong> ${stats?.total_sites || 0} | <strong>Sessions:</strong> ${stats?.total_sessions || 0}</p>
              ${costCode ? `<p><strong>Cost Code:</strong> ${costCode}</p>` : ''}
              <p><strong>Generated:</strong> ${new Date().toLocaleString()}</p>
            </div>
            
            <div style="text-align: center; margin: 20px 0;">
              <a href="${downloadUrl}" style="display: inline-block; background-color: #1e3a5f; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">
                Download Report
              </a>
            </div>
          </div>
        </div>
      `;

      // Send via NotificationAPI
      for (const email of emails) {
        try {
          const result = await notificationapi.send({
            type: 'Energy_rite_report',
            to: {
              id: email.replace('@', '_').replace('.', '_'),
              email: email
            },
            email: {
              subject: `Energy Rite ${reportTypeName} Report - ${period} ${costCode ? `(${costCode})` : ''}`,
              html: emailHTML,
              senderName: 'Energy Rite Reports',
              senderEmail: process.env.EMAIL_FROM || 'noreply@energyrite.com'
            }
          });
          console.log(`✅ Email sent to ${email}:`, result);
        } catch (emailError) {
          console.error(`❌ Failed to send to ${email}:`, emailError.message);
          throw emailError;
        }
      }
      
      console.log(`✅ Report email sent successfully to ${emails.length} recipients`);
      
      return { 
        success: true, 
        recipients: emails.length
      };
      
    } catch (error) {
      console.error('❌ Failed to send report email:', error.message);
      
      return { 
        success: false, 
        error: error.message 
      };
    }
  }
}

module.exports = new EnergyRiteEmailService();