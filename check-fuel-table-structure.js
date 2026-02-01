require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkTableStructure() {
  console.log('🔍 Checking energy_rite_fuel_data table structure...\n');
  
  try {
    // Try to get a sample record to see the structure
    const { data, error } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .limit(1);
    
    if (error) {
      console.error('❌ Error querying table:', error.message);
      return;
    }
    
    if (data && data.length > 0) {
      console.log('📋 Table columns found:');
      Object.keys(data[0]).forEach(column => {
        console.log(`   • ${column}: ${typeof data[0][column]}`);
      });
      
      console.log('\n📊 Sample record:');
      console.log(JSON.stringify(data[0], null, 2));
    } else {
      console.log('📋 Table exists but no data found');
      
      // Try to insert a test record to see what columns are expected
      console.log('\n🧪 Testing insert to discover required columns...');
      const testInsert = await supabase
        .from('energy_rite_fuel_data')
        .insert({
          plate: 'TEST',
          fuel_probe_1_level: 100
        });
        
      if (testInsert.error) {
        console.log('❌ Insert error (shows required columns):');
        console.log(testInsert.error.message);
      } else {
        console.log('✅ Test insert successful');
        // Clean up
        await supabase.from('energy_rite_fuel_data').delete().eq('plate', 'TEST');
      }
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkTableStructure().then(() => process.exit(0));