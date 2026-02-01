require('dotenv').config();
const nodemailer = require('nodemailer');

async function testDomainsEmail() {
  console.log('📧 Testing domains.co.za Email Configuration...\n');
  
  const email = 'energyrite@solflo.co.za';
  const password = 'your-actual-password'; // You need to provide this
  
  // Common SMTP configurations for domains.co.za
  const configs = [
    { host: 'mail.solflo.co.za', port: 587, secure: false, name: 'Standard SMTP' },
    { host: 'mail.solflo.co.za', port: 465, secure: true, name: 'Secure SMTP' },
    { host: 'smtp.solflo.co.za', port: 587, secure: false, name: 'SMTP subdomain' },
    { host: 'smtp.solflo.co.za', port: 465, secure: true, name: 'SMTP subdomain secure' },
    { host: 'mail.domains.co.za', port: 587, secure: false, name: 'Domains.co.za server' },
    { host: 'mail.domains.co.za', port: 465, secure: true, name: 'Domains.co.za secure' }
  ];

  console.log('🔧 Please update EMAIL_PASSWORD in .env with your actual password\n');

  for (const config of configs) {
    console.log(`🔍 Testing ${config.name} (${config.host}:${config.port})...`);
    
    const transporter = nodemailer.createTransport({
      host: config.host,
      port: config.port,
      secure: config.secure,
      auth: {
        user: email,
        pass: password
      },
      timeout: 5000
    });

    try {
      await transporter.verify();
      console.log(`✅ ${config.name} - Connection successful!\n`);
      
      // Try sending test email
      console.log('📤 Sending test email...');
      const info = await transporter.sendMail({
        from: `"EnergyRite System" <${email}>`,
        to: 'brianm@solflo.co.za',
        subject: 'EnergyRite Email Test - ' + new Date().toLocaleString(),
        html: `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background-color: #0070f3; color: white; padding: 20px; text-align: center;">
              <h1>✅ EnergyRite Email Working!</h1>
            </div>
            <div style="padding: 20px; background-color: #f9f9f9;">
              <h2>Email Configuration Successful</h2>
              <p><strong>SMTP Server:</strong> ${config.host}:${config.port}</p>
              <p><strong>Sent at:</strong> ${new Date().toLocaleString()}</p>
              <hr>
              <p><small>Test email from EnergyRite system</small></p>
            </div>
          </div>
        `
      });

      console.log('✅ Test email sent successfully!');
      console.log('📧 Message ID:', info.messageId);
      console.log(`\n🎉 Working configuration found:`);
      console.log(`EMAIL_HOST=${config.host}`);
      console.log(`EMAIL_PORT=${config.port}`);
      console.log(`EMAIL_SECURE=${config.secure}`);
      return; // Stop testing once we find a working config
      
    } catch (error) {
      console.log(`❌ ${config.name} - Failed: ${error.message}`);
    }
  }
  
  console.log('\n❌ No working configuration found.');
  console.log('\n🔧 Next steps:');
  console.log('1. Contact domains.co.za support for SMTP settings');
  console.log('2. Check if SMTP is enabled for your email account');
  console.log('3. Verify your email password is correct');
  console.log('4. Update EMAIL_PASSWORD in .env file');
}

testDomainsEmail();