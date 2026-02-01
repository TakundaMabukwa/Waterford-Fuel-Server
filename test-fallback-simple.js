const { supabase } = require('./supabase-client');

async function testFallback() {
  try {
    console.log('🧪 Testing fallback mechanism...');
    
    // Test data
    const testVehicles = [
      { plate: 'UNKNOWN1', branch: 'KEYWEST', company: 'KFC', cost_code: 'KW001', quality: '53.15.1.232' },
      { plate: 'ALEX', branch: 'ALEX', company: 'KFC', cost_code: 'AL001', quality: '192.168.1.101' },
      { plate: 'BLOEM2', branch: 'BLOEM2', company: 'KFC', cost_code: 'BL002', quality: '192.168.1.102' },
      { plate: 'UNKNOWN2', branch: 'DURBANVILL', company: 'KFC', cost_code: 'DV001', quality: '61.172.2.170' },
      { plate: 'BERGBRON', branch: 'BERGBRON', company: 'KFC', cost_code: 'BB001', quality: '62.138.2.35' }
    ];
    
    // Insert test data directly
    for (const vehicle of testVehicles) {
      const { error } = await supabase
        .from('energyrite_vehicle_lookup')
        .upsert(vehicle, { onConflict: 'plate' });
      
      if (error) {
        console.log(`Creating table and inserting ${vehicle.plate}...`);
        // Table might not exist, let's create it
        const createResult = await supabase.rpc('exec_sql', { 
          sql: `
            CREATE TABLE IF NOT EXISTS energyrite_vehicle_lookup (
                id SERIAL PRIMARY KEY,
                plate VARCHAR(50) UNIQUE,
                branch VARCHAR(100),
                company VARCHAR(100) DEFAULT 'KFC',
                cost_code VARCHAR(50),
                quality VARCHAR(50) UNIQUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            INSERT INTO energyrite_vehicle_lookup (plate, branch, company, cost_code, quality) 
            VALUES ('${vehicle.plate}', '${vehicle.branch}', '${vehicle.company}', '${vehicle.cost_code}', '${vehicle.quality}')
            ON CONFLICT (plate) DO UPDATE SET
                branch = EXCLUDED.branch,
                quality = EXCLUDED.quality,
                updated_at = NOW();
          `
        });
        
        if (createResult.error) {
          console.error('Error creating table:', createResult.error);
        } else {
          console.log(`✅ Created table and inserted: ${vehicle.plate} → ${vehicle.branch}`);
        }
      } else {
        console.log(`✅ Inserted: ${vehicle.plate} → ${vehicle.branch}`);
      }
    }
    
    console.log('\n🔍 Testing lookups...');
    
    // Test plate lookup
    const { data: plateTest, error: plateError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('*')
      .eq('plate', 'UNKNOWN1')
      .single();
    
    if (plateError) {
      console.error('Plate lookup error:', plateError);
    } else {
      console.log('✅ Plate lookup:', plateTest);
    }
    
    // Test quality lookup
    const { data: qualityTest, error: qualityError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('*')
      .eq('quality', '53.15.1.232')
      .single();
    
    if (qualityError) {
      console.error('Quality lookup error:', qualityError);
    } else {
      console.log('✅ Quality lookup:', qualityTest);
    }
    
    console.log('\n🎉 Fallback mechanism ready!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testFallback();