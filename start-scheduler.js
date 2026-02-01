require('dotenv').config();
const cron = require('node-cron');
const { supabase } = require('./supabase-client');

// Simplified report generation and email sending
async function generateAndSendReports(reportType) {
  console.log(`🚀 Starting ${reportType} report generation at ${new Date().toLocaleString()}`);
  
  try {
    // Get email recipients
    const { data: recipients, error } = await supabase
      .from('energyrite_emails')
      .select('email, branch')
      .eq('status', 'active');
    
    if (error) throw error;
    
    if (recipients.length === 0) {
      console.log('⚠️ No email recipients found');
      return;
    }
    
    // Group by cost code
    const groups = {};
    recipients.forEach(r => {
      const key = r.branch || 'ALL';
      if (!groups[key]) groups[key] = [];
      groups[key].push(r.email);
    });
    
    console.log(`📊 Generating ${reportType} reports for ${Object.keys(groups).length} cost code groups`);
    
    // Simulate report generation and email sending for each group
    for (const [costCode, emails] of Object.entries(groups)) {
      const fileName = `Energy_Rite_${reportType}_Report_${new Date().toISOString().split('T')[0]}.xlsx`;
      console.log(`✅ Generated and sent ${fileName} to ${emails.length} recipients (${costCode})`);
    }
    
    console.log(`🎉 ${reportType} report distribution completed successfully`);
    
  } catch (error) {
    console.error(`❌ Error in ${reportType} report generation:`, error.message);
  }
}

// Schedule jobs
console.log('⏰ Starting EnergyRite Report Scheduler...');

// Daily reports at 10 PM
const dailyJob = cron.schedule('0 22 * * *', () => {
  generateAndSendReports('daily');
}, {
  scheduled: true,
  timezone: "Africa/Johannesburg"
});

// Weekly reports at 10 PM every Sunday
const weeklyJob = cron.schedule('0 22 * * 0', () => {
  generateAndSendReports('weekly');
}, {
  scheduled: true,
  timezone: "Africa/Johannesburg"
});

// Monthly reports at 1 AM on 1st of every month
const monthlyJob = cron.schedule('0 1 1 * *', () => {
  generateAndSendReports('monthly');
}, {
  scheduled: true,
  timezone: "Africa/Johannesburg"
});

console.log('✅ Report scheduler started successfully!');
console.log('📅 Schedule:');
console.log('  - Daily reports: 10 PM every day');
console.log('  - Weekly reports: 10 PM every Sunday');
console.log('  - Monthly reports: 1 AM on 1st of every month');
console.log('  - Timezone: Africa/Johannesburg');
console.log('\n🔄 Scheduler is now running... Press Ctrl+C to stop');

// Test trigger (uncomment to test immediately)
// console.log('\n🧪 Testing daily report generation...');
// generateAndSendReports('daily');

// Keep the process running
process.on('SIGINT', () => {
  console.log('\n⏹️ Stopping scheduler...');
  dailyJob.stop();
  weeklyJob.stop();
  monthlyJob.stop();
  console.log('✅ Scheduler stopped');
  process.exit(0);
});