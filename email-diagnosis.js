require('dotenv').config();

console.log('🔍 Email Configuration Diagnosis\n');

console.log('Current email settings:');
console.log('- Host:', process.env.EMAIL_HOST);
console.log('- Port:', process.env.EMAIL_PORT);
console.log('- Secure:', process.env.EMAIL_SECURE);
console.log('- User:', process.env.EMAIL_USER);
console.log('- From:', process.env.EMAIL_FROM);
console.log('- Password length:', process.env.EMAIL_PASSWORD?.length, 'characters');

console.log('\n❌ Issue Found: Invalid SMTP credentials');
console.log('\n🔧 To fix:');
console.log('1. Verify the EMAIL_USER and EMAIL_PASSWORD in .env');
console.log('2. Check with your email provider for correct SMTP settings');
console.log('3. Ensure the email account allows SMTP access');

console.log('\n📧 Alternative: Use a different email service like:');
console.log('- Gmail SMTP (smtp.gmail.com:587)');
console.log('- Outlook SMTP (smtp-mail.outlook.com:587)');
console.log('- SendGrid, Mailgun, etc.');

console.log('\n⚠️ Current status: Email sending will fail until credentials are corrected');