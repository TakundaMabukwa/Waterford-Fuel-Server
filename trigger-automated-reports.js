/**
 * Trigger Automated Reports - Production Simulation
 */
require('dotenv').config();
const reportDistributionService = require('./services/energy-rite/reportDistributionService');
const reportScheduler = require('./helpers/report-scheduler');

async function triggerDailyReports() {
  console.log('🌅 Triggering Daily Reports (10 PM Schedule)...');
  
  try {
    const result = await reportDistributionService.scheduleDailyReports();
    
    if (result.success) {
      console.log(`✅ Daily reports sent to ${result.successful_distributions} cost code groups`);
      result.results.forEach(r => {
        console.log(`  📧 ${r.cost_code}: ${r.recipients.length} recipients - ${r.email_sent ? 'SENT' : 'FAILED'}`);
      });
    } else {
      console.log('❌ Daily report distribution failed:', result.error);
    }
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

async function triggerWeeklyReports() {
  console.log('📅 Triggering Weekly Reports (Sunday 10 PM)...');
  
  try {
    const result = await reportDistributionService.scheduleWeeklyReports();
    
    if (result.success) {
      console.log(`✅ Weekly reports sent to ${result.successful_distributions} cost code groups`);
    } else {
      console.log('❌ Weekly report distribution failed:', result.error);
    }
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

async function triggerMonthlyReports() {
  console.log('📊 Triggering Monthly Reports (1st of month 1 AM)...');
  
  try {
    const result = await reportDistributionService.scheduleMonthlyReports();
    
    if (result.success) {
      console.log(`✅ Monthly reports sent to ${result.successful_distributions} cost code groups`);
    } else {
      console.log('❌ Monthly report distribution failed:', result.error);
    }
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

async function startScheduler() {
  console.log('⏰ Starting Production Scheduler...');
  reportScheduler.startScheduledReports();
  console.log('✅ Scheduler running - reports will be sent automatically');
}

// Command line interface
const command = process.argv[2];

switch (command) {
  case 'daily':
    triggerDailyReports();
    break;
  case 'weekly':
    triggerWeeklyReports();
    break;
  case 'monthly':
    triggerMonthlyReports();
    break;
  case 'start':
    startScheduler();
    break;
  default:
    console.log('Usage: node trigger-automated-reports.js [daily|weekly|monthly|start]');
    console.log('  daily   - Trigger daily reports now');
    console.log('  weekly  - Trigger weekly reports now');
    console.log('  monthly - Trigger monthly reports now');
    console.log('  start   - Start automatic scheduler');
}