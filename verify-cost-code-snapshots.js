// Load environment variables first
require('dotenv').config();

const { supabase } = require('./supabase-client');

async function verifySnapshotsWithCostCodes() {
  console.log('🎯 VERIFYING SNAPSHOT DATA WITH COST CODES');
  console.log('='.repeat(50));
  
  try {
    // Get the most recent snapshots with full data
    const { data: snapshots, error } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .order('snapshot_time', { ascending: false })
      .limit(10);

    if (error) {
      console.log('❌ Error fetching snapshots:', error.message);
      return;
    }

    console.log(`\n📊 Found ${snapshots.length} recent snapshots with cost code integration:`);
    console.log('='.repeat(80));

    snapshots.forEach((snapshot, index) => {
      const data = snapshot.snapshot_data || {};
      const timeFormatted = new Date(snapshot.snapshot_time).toLocaleString();
      
      console.log(`\n📸 SNAPSHOT ${index + 1}:`);
      console.log(`   🆔 ID: ${snapshot.id}`);
      console.log(`   📅 Date/Time: ${snapshot.snapshot_date} ${timeFormatted}`);
      console.log(`   🏷️  Type: ${snapshot.snapshot_type}`);
      console.log(`   🏢 Branch: ${snapshot.branch}`);
      console.log(`   🏭 Company: ${snapshot.company}`);
      console.log(`   💼 Cost Code: ${data.cost_code || '❌ Not Available'}`);
      console.log(`   🚗 Vehicle: ${data.vehicle_plate || snapshot.branch}`);
      console.log(`   ⛽ Fuel Level: ${data.fuel_level || 0}%`);
      console.log(`   📊 Fuel Volume: ${data.fuel_volume || 0}L`);
      console.log(`   🔧 Engine Status: ${data.engine_status || 'UNKNOWN'}`);
      console.log(`   📝 Notes: ${data.notes || 'N/A'}`);
      console.log('   ' + '-'.repeat(60));
    });

    // Analyze cost code coverage
    const withCostCodes = snapshots.filter(s => s.snapshot_data?.cost_code);
    const totalFuel = snapshots.reduce((sum, s) => sum + (s.snapshot_data?.fuel_volume || 0), 0);
    const avgFuel = snapshots.length > 0 ? totalFuel / snapshots.length : 0;

    console.log('\n📈 ANALYSIS SUMMARY:');
    console.log('='.repeat(50));
    console.log(`📊 Total Snapshots: ${snapshots.length}`);
    console.log(`💼 With Cost Codes: ${withCostCodes.length} (${((withCostCodes.length/snapshots.length)*100).toFixed(1)}%)`);
    console.log(`⛽ Total Fuel Tracked: ${totalFuel.toFixed(1)}L`);
    console.log(`📊 Average Fuel per Vehicle: ${avgFuel.toFixed(1)}L`);

    // Show unique cost codes found
    const uniqueCostCodes = [...new Set(withCostCodes.map(s => s.snapshot_data.cost_code))];
    console.log(`\n🏷️  COST CODES DETECTED:`);
    if (uniqueCostCodes.length > 0) {
      uniqueCostCodes.forEach(code => console.log(`   💼 ${code}`));
    } else {
      console.log('   ❌ No cost codes found in snapshots');
    }

    // Show snapshot types breakdown
    const typeBreakdown = {};
    snapshots.forEach(s => {
      typeBreakdown[s.snapshot_type] = (typeBreakdown[s.snapshot_type] || 0) + 1;
    });

    console.log(`\n🕒 SNAPSHOT TYPE BREAKDOWN:`);
    Object.entries(typeBreakdown).forEach(([type, count]) => {
      console.log(`   ${type}: ${count} snapshots`);
    });

    console.log('\n🎉 COST CODE ENHANCEMENT VERIFICATION COMPLETE!');
    
    if (withCostCodes.length > 0) {
      console.log('✅ Cost code integration is WORKING PERFECTLY!');
      console.log('🚀 Enhanced snapshots are capturing cost codes automatically!');
    } else {
      console.log('⚠️  Snapshots created but cost codes not captured - check operating sessions data');
    }

  } catch (error) {
    console.error('❌ Verification failed:', error.message);
  }
}

verifySnapshotsWithCostCodes();