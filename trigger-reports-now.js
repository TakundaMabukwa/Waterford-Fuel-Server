#!/usr/bin/env node
/**
 * Trigger EnergyRite Automated Reports Immediately
 */
require('dotenv').config();
const reportDistributionService = require('./services/energy-rite/reportDistributionService');

async function triggerReportsNow() {
  console.log('🚀 Triggering EnergyRite Automated Reports...\n');
  
  try {
    // Trigger daily reports
    console.log('📅 Generating and sending daily reports...');
    const dailyResult = await reportDistributionService.scheduleDailyReports();
    
    if (dailyResult.success) {
      console.log(`✅ Daily reports sent to ${dailyResult.successful_distributions} cost code groups`);
      dailyResult.results.forEach(r => {
        console.log(`  📧 ${r.cost_code}: ${r.recipients.length} recipients - ${r.email_sent ? 'SENT' : 'FAILED'}`);
      });
    } else {
      console.log('❌ Daily report distribution failed:', dailyResult.error);
    }
    
    console.log('\n🎉 Automated report trigger completed!');
    
  } catch (error) {
    console.error('❌ Error triggering reports:', error.message);
  }
}

// Run immediately
triggerReportsNow();