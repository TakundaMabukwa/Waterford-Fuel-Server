require('dotenv').config();
const nodemailer = require('nodemailer');

async function sendEmail() {
  const transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: parseInt(process.env.EMAIL_PORT),
    secure: process.env.EMAIL_SECURE === 'true',
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASSWORD.replace(/'/g, '')
    }
  });

  try {
    const info = await transporter.sendMail({
      from: process.env.EMAIL_FROM,
      to: 'brianm@solflo.co.za',
      subject: 'EnergyRite Report Test',
      html: `
        <h2>EnergyRite Daily Report</h2>
        <p>Test report generated at: ${new Date().toLocaleString()}</p>
        <p>This email confirms the automated reporting system is working.</p>
      `
    });

    console.log('✅ Email sent successfully');
    console.log('Message ID:', info.messageId);
  } catch (error) {
    console.error('❌ Email failed:', error.message);
  }
}

sendEmail();