#!/usr/bin/env node
/**
 * Test the new styled email template
 */
require('dotenv').config();
const emailService = require('./services/energy-rite/emailService');

async function testStyledEmail() {
  console.log('📧 Testing New Styled Email Template...\n');
  
  try {
    // Test email with sample report data
    const testEmailOptions = {
      reportType: 'daily',
      period: 'October 30, 2025',
      fileName: 'Energy_Rite_daily_Report_2025-10-30.xlsx',
      downloadUrl: 'https://example.com/download/report.xlsx',
      costCode: 'KFC-0001-0001-0002-0004',
      stats: {
        total_sites: 8,
        total_sessions: 12,
        total_operating_hours: '24.5'
      }
    };
    
    console.log('🎨 Sending styled email with brand colors...');
    const result = await emailService.sendReportEmail(testEmailOptions);
    
    if (result.success) {
      console.log(`✅ Styled email sent successfully to ${result.recipients} recipients!`);
      console.log(`📧 Message ID: ${result.messageId}`);
      console.log('\n🎉 New email template is working with brand styling!');
    } else {
      console.log('❌ Email sending failed:', result.error);
    }
    
  } catch (error) {
    console.error('❌ Error testing styled email:', error.message);
  }
}

testStyledEmail();