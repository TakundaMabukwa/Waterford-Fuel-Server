require('dotenv').config();
const nodemailer = require('nodemailer');

async function testEmailAuth() {
  console.log('🔐 Testing Email Authentication...\n');
  
  const transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: parseInt(process.env.EMAIL_PORT),
    secure: process.env.EMAIL_SECURE === 'true',
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASSWORD
    },
    debug: true
  });

  try {
    console.log('🔍 Verifying SMTP connection...');
    await transporter.verify();
    console.log('✅ SMTP connection verified!');
  } catch (error) {
    console.error('❌ SMTP verification failed:', error.message);
  }
}

testEmailAuth();