require('dotenv').config();
const { supabase } = require('./supabase-client');
const fs = require('fs');

async function updateTableSchema() {
  try {
    console.log('🔧 Updating table schema...');
    
    // Read the SQL file
    const sql = fs.readFileSync('./update-reports-table.sql', 'utf8');
    
    // Execute the SQL
    const { data, error } = await supabase.rpc('exec_sql', { sql_query: sql });
    
    if (error) {
      console.error('❌ SQL execution error:', error);
      
      // Try alternative approach - execute each statement separately
      console.log('🔄 Trying alternative approach...');
      const statements = sql.split(';').filter(s => s.trim());
      
      for (const statement of statements) {
        if (statement.trim()) {
          console.log('📝 Executing:', statement.trim().substring(0, 50) + '...');
          const { error: stmtError } = await supabase.rpc('exec_sql', { 
            sql_query: statement.trim() + ';' 
          });
          
          if (stmtError) {
            console.error('❌ Statement error:', stmtError);
          } else {
            console.log('✅ Statement executed successfully');
          }
        }
      }
    } else {
      console.log('✅ Schema updated successfully');
    }
    
    // Test the updated table
    console.log('🧪 Testing updated table...');
    const { data: testData, error: testError } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .limit(1);
    
    if (testError) {
      console.error('❌ Test query error:', testError);
    } else {
      console.log('✅ Table structure test passed');
      if (testData.length > 0) {
        console.log('📊 Sample columns:', Object.keys(testData[0]));
      }
    }
    
  } catch (error) {
    console.error('❌ Update error:', error);
  }
}

updateTableSchema();