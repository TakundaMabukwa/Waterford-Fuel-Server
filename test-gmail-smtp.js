require('dotenv').config();
const nodemailer = require('nodemailer');

async function testGmailSMTP() {
  console.log('📧 Testing Gmail SMTP Configuration...\n');
  
  // You need to update these in .env file:
  const gmailUser = 'your-gmail@gmail.com';  // Replace with your Gmail
  const gmailAppPassword = 'your-app-password';  // Replace with Gmail App Password
  
  console.log('🔧 Setup Instructions:');
  console.log('1. Go to your Google Account settings');
  console.log('2. Enable 2-Factor Authentication');
  console.log('3. Generate an App Password for "Mail"');
  console.log('4. Update .env file with:');
  console.log(`   EMAIL_USER=${gmailUser}`);
  console.log(`   EMAIL_PASSWORD=${gmailAppPassword}`);
  console.log(`   EMAIL_FROM=${gmailUser}`);
  console.log('');

  const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 587,
    secure: false, // true for 465, false for other ports
    auth: {
      user: gmailUser,
      pass: gmailAppPassword
    }
  });

  try {
    console.log('🔍 Testing SMTP connection...');
    await transporter.verify();
    console.log('✅ Gmail SMTP connection verified!\n');
    
    console.log('📤 Sending test email...');
    const info = await transporter.sendMail({
      from: `"EnergyRite System" <${gmailUser}>`,
      to: 'brianm@solflo.co.za',
      subject: 'EnergyRite Gmail Test - ' + new Date().toLocaleString(),
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <div style="background-color: #0070f3; color: white; padding: 20px; text-align: center;">
            <h1>✅ EnergyRite Gmail SMTP Working!</h1>
          </div>
          <div style="padding: 20px; background-color: #f9f9f9;">
            <h2>Email System Successfully Configured</h2>
            <p>Gmail SMTP is now working for EnergyRite reports.</p>
            <p><strong>Sent at:</strong> ${new Date().toLocaleString()}</p>
            <hr>
            <p><small>Automated email from EnergyRite system via Gmail SMTP</small></p>
          </div>
        </div>
      `
    });

    console.log('✅ Test email sent successfully!');
    console.log('📧 Message ID:', info.messageId);
    
  } catch (error) {
    console.error('❌ Gmail SMTP test failed:', error.message);
    
    if (error.message.includes('Invalid login')) {
      console.log('\n🔧 Fix:');
      console.log('1. Use a Gmail account');
      console.log('2. Enable 2-Factor Authentication');
      console.log('3. Generate App Password (not regular password)');
      console.log('4. Update EMAIL_USER and EMAIL_PASSWORD in .env');
    }
  }
}

testGmailSMTP();