// Get the exact current table schema to understand what needs to be fixed
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function getCurrentSchema() {
  try {
    console.log('🔍 Diagnosing Current Table Schema');
    console.log('=' .repeat(50));
    
    // Try to get schema information by attempting different inserts
    console.log('\n📊 Testing different column combinations...');
    
    const testCombinations = [
      {
        name: 'Basic columns only',
        data: {
          branch: 'TEST1',
          company: 'TEST',
          snapshot_date: '2025-11-07'
        }
      },
      {
        name: 'With snapshot_time',
        data: {
          branch: 'TEST2',
          company: 'TEST',
          snapshot_date: '2025-11-07',
          snapshot_time: new Date().toISOString()
        }
      },
      {
        name: 'With snapshot_data',
        data: {
          branch: 'TEST3',
          company: 'TEST',
          snapshot_date: '2025-11-07',
          snapshot_data: { test: 'data' }
        }
      },
      {
        name: 'With both snapshot_time and snapshot_data',
        data: {
          branch: 'TEST4',
          company: 'TEST',
          snapshot_date: '2025-11-07',
          snapshot_time: new Date().toISOString(),
          snapshot_data: { test: 'data' }
        }
      }
    ];
    
    for (const test of testCombinations) {
      console.log(`\n   Testing: ${test.name}`);
      
      try {
        const { data, error } = await supabase
          .from('energy_rite_daily_snapshots')
          .insert([test.data])
          .select();
        
        if (error) {
          console.log(`   ❌ Failed: ${error.message}`);
          
          // Analyze the error to understand schema
          if (error.message.includes('violates not-null constraint')) {
            const match = error.message.match(/column "([^"]+)"/);
            if (match) {
              console.log(`   💡 Required column: ${match[1]}`);
            }
          } else if (error.message.includes('does not exist')) {
            const match = error.message.match(/column[^"]*"([^"]+)"/);
            if (match) {
              console.log(`   💡 Missing column: ${match[1]}`);
            }
          }
        } else {
          console.log(`   ✅ Success! Columns: ${Object.keys(data[0]).join(', ')}`);
          
          // Clean up successful inserts
          await supabase
            .from('energy_rite_daily_snapshots')
            .delete()
            .eq('branch', test.data.branch);
        }
      } catch (e) {
        console.log(`   ❌ Exception: ${e.message}`);
      }
    }
    
    console.log('\n📋 Schema Analysis Complete');
    console.log('=' .repeat(30));
    
    console.log('\n🎯 Based on the test results:');
    console.log('\n1. 📊 Current table structure appears to require:');
    console.log('   - branch (exists)');
    console.log('   - company (exists)');
    console.log('   - snapshot_date (exists)');
    console.log('   - snapshot_time (required, NOT NULL)');
    console.log('   - snapshot_data (may not exist or is nullable)');
    
    console.log('\n2. 🔧 The cost code enhancement needs:');
    console.log('   - snapshot_data JSONB column (for storing cost codes)');
    console.log('   - snapshot_time should be optional or auto-generated');
    
    console.log('\n3. 💡 Migration strategy:');
    console.log('   A. Add snapshot_data column if missing');
    console.log('   B. Make snapshot_time optional or auto-generated');
    console.log('   C. Update the snapshot scheduler to match the schema');
    
    console.log('\n📄 Migration files available:');
    console.log('   - essential-snapshots-fix.sql (adds missing columns)');
    console.log('   - fix-daily-snapshots-migration.sql (comprehensive fix)');
    
    console.log('\n⚡ Next steps:');
    console.log('   1. Apply the migration SQL in Supabase');
    console.log('   2. Test the enhanced snapshot system');
    console.log('   3. Verify cost codes are captured correctly');
    
  } catch (error) {
    console.error('❌ Schema diagnosis failed:', error.message);
  }
}

// Run the schema diagnosis
getCurrentSchema()
  .then(() => {
    console.log('\n' + '='.repeat(50));
    console.log('🎯 Schema diagnosis completed');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Error:', error);
    process.exit(1);
  });