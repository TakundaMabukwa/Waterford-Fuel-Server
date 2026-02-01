#!/usr/bin/env node
/**
 * Trigger EnergyRite Automated Reports with New Styled Email Template
 */
require('dotenv').config();
const reportDistributionService = require('./services/energy-rite/reportDistributionService');

async function triggerStyledReports() {
  console.log('🎨 Triggering EnergyRite Reports with Brand Styling...\n');
  
  try {
    console.log('📅 Generating daily reports with new email template...');
    const result = await reportDistributionService.scheduleDailyReports();
    
    if (result.success) {
      console.log(`✅ Styled reports sent to ${result.successful_distributions} cost code groups`);
      console.log('\n📧 Email Details:');
      result.results.forEach(r => {
        const status = r.email_sent ? '✅ SENT' : '❌ FAILED';
        console.log(`  • ${r.cost_code}: ${r.recipients.length} recipients - ${status}`);
        if (r.file_name) {
          console.log(`    📄 File: ${r.file_name}`);
        }
      });
      
      console.log('\n🎉 All reports sent with new brand styling!');
      console.log('📧 Recipients will receive emails with:');
      console.log('  • Primary Blue (#1e3a5f) header with gradient');
      console.log('  • Success Green (#10B981) download button');
      console.log('  • Professional card-based layout');
      console.log('  • Responsive design for mobile devices');
      console.log('  • EnergyRite branding and logo placeholder');
      
    } else {
      console.log('❌ Report distribution failed:', result.error);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

triggerStyledReports();