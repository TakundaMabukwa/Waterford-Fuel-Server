require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkEmailTable() {
  try {
    console.log('📧 Checking energyrite_emails table structure...\n');
    
    // Try to get a sample record to see the columns
    const { data, error } = await supabase
      .from('energyrite_emails')
      .select('*')
      .limit(1);
    
    if (error) {
      console.error('❌ Error:', error.message);
      
      // Try to create the table if it doesn't exist
      console.log('\n🔧 Attempting to create energyrite_emails table...');
      
      const createTableSQL = `
        CREATE TABLE IF NOT EXISTS energyrite_emails (
          id SERIAL PRIMARY KEY,
          email VARCHAR(255) NOT NULL,
          branch VARCHAR(100),
          status VARCHAR(20) DEFAULT 'active',
          email_type VARCHAR(50) DEFAULT 'report',
          created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          UNIQUE(email, branch)
        );
      `;
      
      console.log('SQL to create table:', createTableSQL);
      return;
    }
    
    if (data.length > 0) {
      console.log('✅ Table exists with columns:', Object.keys(data[0]));
      console.log('Sample record:', data[0]);
    } else {
      console.log('✅ Table exists but is empty');
      
      // Add a test email
      console.log('\n📧 Adding test email...');
      const { data: insertData, error: insertError } = await supabase
        .from('energyrite_emails')
        .insert({
          email: 'brianm@solflo.co.za',
          branch: 'KFC-0001-0001-0002-0004',
          status: 'active',
          email_type: 'report'
        })
        .select();
      
      if (insertError) {
        console.error('❌ Insert error:', insertError.message);
      } else {
        console.log('✅ Test email added:', insertData[0]);
      }
    }
    
  } catch (error) {
    console.error('❌ Check error:', error);
  }
}

checkEmailTable();