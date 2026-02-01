const { supabase } = require('./supabase-client');

async function testEmailRecipients() {
  try {
    // Check if there are any email recipients
    const { data: recipients, error } = await supabase
      .from('energyrite_emails')
      .select('*')
      .eq('status', 'active');
    
    if (error) {
      console.error('Error querying recipients:', error);
      return;
    }
    
    console.log('📧 Email Recipients Found:', recipients.length);
    
    if (recipients.length === 0) {
      console.log('⚠️ No email recipients found. Adding test recipient...');
      
      // Add a test recipient
      const { data: newRecipient, error: insertError } = await supabase
        .from('energyrite_emails')
        .insert({
          email: 'test@example.com',
          recipient_name: 'Test User',
          cost_code: 'KFC-0001-0001-0002',
          branch: 'TEST_BRANCH',
          report_types: ['daily', 'weekly', 'monthly'],
          status: 'active'
        })
        .select();
      
      if (insertError) {
        console.error('Error adding test recipient:', insertError);
      } else {
        console.log('✅ Test recipient added:', newRecipient);
      }
    } else {
      recipients.forEach(recipient => {
        console.log(`📧 ${recipient.email} - ${recipient.cost_code || 'No cost code'} - ${recipient.branch || 'No branch'}`);
      });
    }
    
  } catch (error) {
    console.error('Error:', error);
  }
}

testEmailRecipients();