const notificationapi = require('notificationapi-node-server-sdk').default;
require('dotenv').config();

// Initialize NotificationAPI
notificationapi.init(
  process.env.NOTIFICATIONAPI_CLIENT_ID,
  process.env.NOTIFICATIONAPI_CLIENT_SECRET
);

async function testEmail() {
  try {
    console.log('🧪 Testing email with NotificationAPI...');
    
    const result = await notificationapi.send({
      type: 'Energy_rite_report',
      to: {
        id: 'mabukwa25_gmail_com',
        email: 'mabukwa25@gmail.com'
      },
      email: {
        subject: 'Test Energy Rite Report',
        html: '<h1>Test Email</h1><p>This is a test email from Energy Rite system.</p>',
        senderName: 'Energy Rite Reports',
        senderEmail: 'noreply@energyrite.com'
      }
    });
    
    console.log('✅ Email sent successfully:', result);
  } catch (error) {
    console.error('❌ Email test failed:', error);
  }
}

testEmail();