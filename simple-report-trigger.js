require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testConnection() {
  console.log('🔗 Testing Supabase connection...');
  try {
    const { data, error } = await supabase.from('energy_rite_fuel_data').select('count').limit(1);
    if (error) throw error;
    console.log('✅ Supabase connected successfully');
    return true;
  } catch (error) {
    console.error('❌ Supabase connection failed:', error.message);
    return false;
  }
}

async function getEmailRecipients() {
  console.log('📧 Getting email recipients...');
  try {
    const { data, error } = await supabase
      .from('energyrite_emails')
      .select('email, branch')
      .eq('status', 'active');
    
    if (error) throw error;
    
    console.log(`Found ${data.length} email recipients:`);
    data.forEach(r => console.log(`  - ${r.email} (${r.branch || 'ALL'})`));
    return data;
  } catch (error) {
    console.error('❌ Error getting recipients:', error.message);
    return [];
  }
}

async function simulateReportGeneration() {
  console.log('📊 Simulating report generation...');
  
  // This simulates what happens at 10 PM daily
  const reportData = {
    type: 'daily',
    date: new Date().toISOString().split('T')[0],
    fileName: `Energy_Rite_daily_Report_${new Date().toISOString().split('T')[0]}.xlsx`,
    downloadUrl: 'https://example.com/report.xlsx',
    stats: {
      total_sites: 15,
      total_sessions: 245,
      total_operating_hours: 1250
    }
  };
  
  console.log('✅ Report generated:', reportData.fileName);
  return reportData;
}

async function simulateEmailSending(recipients, reportData) {
  console.log('📨 Simulating email sending...');
  
  // Group recipients by cost code
  const groups = {};
  recipients.forEach(r => {
    const key = r.branch || 'ALL';
    if (!groups[key]) groups[key] = [];
    groups[key].push(r.email);
  });
  
  console.log(`📬 Sending to ${Object.keys(groups).length} cost code groups:`);
  
  Object.entries(groups).forEach(([costCode, emails]) => {
    console.log(`  ✅ ${costCode}: ${emails.length} recipients`);
    emails.forEach(email => console.log(`    📧 ${email}`));
  });
  
  return { success: true, groups: Object.keys(groups).length };
}

async function runAutomatedReportDemo() {
  console.log('🚀 EnergyRite Automated Report Demo\n');
  
  // Step 1: Test connection
  const connected = await testConnection();
  if (!connected) return;
  
  // Step 2: Get recipients
  const recipients = await getEmailRecipients();
  if (recipients.length === 0) {
    console.log('⚠️ No email recipients configured');
    return;
  }
  
  // Step 3: Generate report
  const reportData = await simulateReportGeneration();
  
  // Step 4: Send emails
  const emailResult = await simulateEmailSending(recipients, reportData);
  
  console.log('\n🎉 Automated Report Process Complete!');
  console.log(`📊 Report: ${reportData.fileName}`);
  console.log(`📧 Sent to: ${emailResult.groups} cost code groups`);
  console.log('\n⏰ In production, this runs automatically:');
  console.log('  - Daily: 10 PM every day');
  console.log('  - Weekly: 10 PM every Sunday');
  console.log('  - Monthly: 1 AM on 1st of month');
}

runAutomatedReportDemo().catch(console.error);