require('dotenv').config();
const nodemailer = require('nodemailer');

async function testEmailFixed() {
  console.log('📧 Testing Email Configuration...\n');
  
  // Clean password by removing quotes
  const cleanPassword = process.env.EMAIL_PASSWORD.replace(/['"]/g, '');
  
  console.log('Current settings:');
  console.log('- Host:', process.env.EMAIL_HOST);
  console.log('- Port:', process.env.EMAIL_PORT);
  console.log('- Secure:', process.env.EMAIL_SECURE);
  console.log('- User:', process.env.EMAIL_USER);
  console.log('- From:', process.env.EMAIL_FROM);
  console.log('- Password length:', cleanPassword.length, 'characters');
  console.log('');

  const transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: parseInt(process.env.EMAIL_PORT),
    secure: process.env.EMAIL_SECURE === 'true',
    auth: {
      user: process.env.EMAIL_USER,
      pass: cleanPassword
    },
    debug: true, // Enable debug
    logger: true // Enable logging
  });

  try {
    console.log('🔍 Testing SMTP connection...');
    await transporter.verify();
    console.log('✅ SMTP connection verified successfully!\n');
    
    console.log('📤 Sending test email...');
    const info = await transporter.sendMail({
      from: `"EnergyRite Test" <${process.env.EMAIL_FROM}>`,
      to: 'brianm@solflo.co.za',
      subject: 'EnergyRite Email Test - ' + new Date().toLocaleString(),
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <div style="background-color: #0070f3; color: white; padding: 20px; text-align: center;">
            <h1>EnergyRite Email Test</h1>
          </div>
          <div style="padding: 20px; background-color: #f9f9f9;">
            <h2>✅ Email System Working!</h2>
            <p>This test email was sent successfully at: <strong>${new Date().toLocaleString()}</strong></p>
            <p>Email configuration is working correctly.</p>
            <hr>
            <p><small>This is an automated test from the EnergyRite system.</small></p>
          </div>
        </div>
      `
    });

    console.log('✅ Test email sent successfully!');
    console.log('📧 Message ID:', info.messageId);
    console.log('📧 Response:', info.response);
    
  } catch (error) {
    console.error('❌ Email test failed:', error.message);
    console.error('Error code:', error.code);
    console.error('Error command:', error.command);
    
    if (error.message.includes('Invalid login')) {
      console.log('\n🔧 Troubleshooting:');
      console.log('1. Check if EMAIL_USER and EMAIL_PASSWORD are correct');
      console.log('2. Verify the email account exists and is active');
      console.log('3. Check if SMTP is enabled for this account');
      console.log('4. Try different port (587, 465, or 25)');
    }
  }
}

testEmailFixed();