require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function testAPIDirect() {
  try {
    console.log('🌐 Testing API Direct...\n');
    
    // Insert test data
    await supabase.from('energy_rite_fuel_fills').insert({
      plate: 'KROONSTAD2',
      cost_code: 'KFC-KROONSTAD2-001',
      company: 'KFC',
      fill_date: new Date().toISOString(),
      fuel_before: 45,
      fuel_after: 175,
      fill_amount: 130,
      detection_method: 'TEST'
    });
    
    console.log('✅ Test data inserted');
    
    // Test API
    const response = await axios.get('http://localhost:4000/api/energy-rite/fuel-fills', {
      params: { plate: 'KROONSTAD2' }
    });
    
    console.log('✅ API Response:');
    console.log(`Count: ${response.data.count}`);
    console.log(`Success: ${response.data.success}`);
    
    if (response.data.data.length > 0) {
      const fill = response.data.data[0];
      console.log('Fill Data:');
      console.log(`  Plate: ${fill.plate}`);
      console.log(`  Cost Code: ${fill.cost_code}`);
      console.log(`  Amount: ${fill.fill_amount}L`);
    }
    
    // Test activity report
    const reportResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: new Date().toISOString().split('T')[0],
        site_id: 'KROONSTAD2'
      }
    });
    
    console.log('\n✅ Activity Report:');
    console.log(`Fuel Fills: ${reportResponse.data.data.summary.total_fuel_fills || 0}`);
    
    // Cleanup
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', 'KROONSTAD2');
    
    console.log('\n🎉 API test complete!');
    
  } catch (error) {
    console.error('❌ Error:', error.response?.data || error.message);
  }
}

testAPIDirect();