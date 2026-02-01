/**
 * Test Automated Report Generation and Email Distribution
 */
const { supabase } = require('./supabase-client');
const reportDistributionService = require('./services/energy-rite/reportDistributionService');
const emailService = require('./services/energy-rite/emailService');
const reportScheduler = require('./helpers/report-scheduler');

async function testEmailRecipients() {
  console.log('\n🔍 Testing Email Recipients...');
  
  try {
    // Get all email recipients
    const recipients = await reportDistributionService.getEmailRecipients();
    console.log(`📧 Found ${recipients.length} email recipients:`);
    
    recipients.forEach(recipient => {
      console.log(`  - ${recipient.email} (Branch: ${recipient.branch || 'ALL'})`);
    });
    
    return recipients.length > 0;
  } catch (error) {
    console.error('❌ Error testing email recipients:', error.message);
    return false;
  }
}

async function testReportGeneration() {
  console.log('\n📊 Testing Report Generation...');
  
  try {
    // Test manual daily report generation
    const testDate = new Date();
    testDate.setDate(testDate.getDate() - 1); // Yesterday
    
    console.log(`📅 Generating test daily report for ${testDate.toDateString()}`);
    
    const result = await reportScheduler.triggerManualReport('daily', testDate, {
      cost_code: null // Generate for all cost codes
    });
    
    if (result && result.success) {
      console.log('✅ Daily report generated successfully');
      console.log(`📄 File: ${result.file_name}`);
      console.log(`🔗 Download URL: ${result.download_url}`);
      return true;
    } else {
      console.log('❌ Daily report generation failed');
      return false;
    }
  } catch (error) {
    console.error('❌ Error testing report generation:', error.message);
    return false;
  }
}

async function testEmailDistribution() {
  console.log('\n📨 Testing Email Distribution...');
  
  try {
    // Test daily report distribution
    const result = await reportDistributionService.distributeReportsByCostCode('daily');
    
    if (result.success) {
      console.log('✅ Email distribution completed successfully');
      console.log(`📊 Results: ${result.successful_distributions}/${result.total_cost_codes} cost codes processed`);
      
      // Show detailed results
      result.results.forEach(r => {
        const status = r.email_sent ? '✅' : '❌';
        console.log(`  ${status} ${r.cost_code}: ${r.recipients.length} recipients`);
        if (r.error) {
          console.log(`    Error: ${r.error}`);
        }
      });
      
      return result.successful_distributions > 0;
    } else {
      console.log('❌ Email distribution failed:', result.error);
      return false;
    }
  } catch (error) {
    console.error('❌ Error testing email distribution:', error.message);
    return false;
  }
}

async function testSchedulerStatus() {
  console.log('\n⏰ Testing Scheduler Status...');
  
  try {
    const status = reportScheduler.getSchedulerStatus();
    console.log('📅 Scheduler Status:');
    console.log(`  Daily Reports: ${status.daily ? '🟢 Running' : '🔴 Stopped'}`);
    console.log(`  Weekly Reports: ${status.weekly ? '🟢 Running' : '🔴 Stopped'}`);
    console.log(`  Monthly Reports: ${status.monthly ? '🟢 Running' : '🔴 Stopped'}`);
    console.log(`  Timezone: ${status.timezone}`);
    
    return true;
  } catch (error) {
    console.error('❌ Error testing scheduler status:', error.message);
    return false;
  }
}

async function setupTestEmailRecipients() {
  console.log('\n🔧 Setting up test email recipients...');
  
  try {
    // Add test email recipients if none exist
    const existingRecipients = await reportDistributionService.getEmailRecipients();
    
    if (existingRecipients.length === 0) {
      console.log('📧 No email recipients found. Adding test recipients...');
      
      const testEmails = [
        { email: 'test@energyrite.com', branch: null }, // Receives all reports
        { email: 'manager@energyrite.com', branch: 'SITE001' }, // Specific cost code
        { email: 'admin@energyrite.com', branch: 'SITE002' } // Another cost code
      ];
      
      for (const testEmail of testEmails) {
        const { data, error } = await supabase
          .from('energyrite_emails')
          .upsert({
            email: testEmail.email,
            recipient_name: `Test User (${testEmail.email})`,
            branch: testEmail.branch,
            email_type: 'report',
            status: 'active'
          }, {
            onConflict: 'email,branch'
          });
        
        if (error) {
          console.error(`❌ Error adding test email ${testEmail.email}:`, error.message);
        } else {
          console.log(`✅ Added test email: ${testEmail.email} (Branch: ${testEmail.branch || 'ALL'})`);
        }
      }
    } else {
      console.log(`✅ Found ${existingRecipients.length} existing email recipients`);
    }
    
    return true;
  } catch (error) {
    console.error('❌ Error setting up test email recipients:', error.message);
    return false;
  }
}

async function testEmailService() {
  console.log('\n📧 Testing Email Service...');
  
  try {
    // Test email service configuration
    const testEmailOptions = {
      reportType: 'daily',
      period: 'Test Period',
      fileName: 'test_report.xlsx',
      downloadUrl: 'https://example.com/test_report.xlsx',
      costCode: null,
      stats: {
        total_sites: 5,
        total_sessions: 100,
        total_operating_hours: 250
      }
    };
    
    console.log('📨 Sending test email...');
    const result = await emailService.sendReportEmail(testEmailOptions);
    
    if (result.success) {
      console.log(`✅ Test email sent successfully to ${result.recipients} recipients`);
      return true;
    } else {
      console.log('❌ Test email failed:', result.error);
      return false;
    }
  } catch (error) {
    console.error('❌ Error testing email service:', error.message);
    return false;
  }
}

async function startScheduler() {
  console.log('\n🚀 Starting Report Scheduler...');
  
  try {
    reportScheduler.startScheduledReports();
    console.log('✅ Report scheduler started successfully');
    console.log('📅 Scheduled times:');
    console.log('  - Daily reports: 10 PM every day');
    console.log('  - Weekly reports: 10 PM every Sunday');
    console.log('  - Monthly reports: 1 AM on 1st of every month');
    console.log('  - Timezone: Africa/Johannesburg');
    
    return true;
  } catch (error) {
    console.error('❌ Error starting scheduler:', error.message);
    return false;
  }
}

async function runFullTest() {
  console.log('🧪 Starting Automated Report Generation and Email Distribution Test\n');
  
  const results = {
    emailRecipientsSetup: false,
    emailRecipients: false,
    reportGeneration: false,
    emailService: false,
    emailDistribution: false,
    schedulerStatus: false,
    schedulerStart: false
  };
  
  // Run all tests
  results.emailRecipientsSetup = await setupTestEmailRecipients();
  results.emailRecipients = await testEmailRecipients();
  results.reportGeneration = await testReportGeneration();
  results.emailService = await testEmailService();
  results.emailDistribution = await testEmailDistribution();
  results.schedulerStatus = await testSchedulerStatus();
  results.schedulerStart = await startScheduler();
  
  // Summary
  console.log('\n📋 Test Results Summary:');
  console.log('========================');
  
  Object.entries(results).forEach(([test, passed]) => {
    const status = passed ? '✅ PASS' : '❌ FAIL';
    const testName = test.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase());
    console.log(`${status} ${testName}`);
  });
  
  const passedTests = Object.values(results).filter(Boolean).length;
  const totalTests = Object.keys(results).length;
  
  console.log(`\n🎯 Overall Result: ${passedTests}/${totalTests} tests passed`);
  
  if (passedTests === totalTests) {
    console.log('🎉 All tests passed! Automated reporting system is ready.');
  } else {
    console.log('⚠️ Some tests failed. Please check the configuration and try again.');
  }
}

// Run the test
if (require.main === module) {
  runFullTest()
    .then(() => {
      console.log('\n✅ Test completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('\n❌ Test failed:', error.message);
      process.exit(1);
    });
}

module.exports = {
  testEmailRecipients,
  testReportGeneration,
  testEmailDistribution,
  testSchedulerStatus,
  setupTestEmailRecipients,
  testEmailService,
  startScheduler,
  runFullTest
};