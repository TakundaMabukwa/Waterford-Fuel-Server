require('dotenv').config();
const axios = require('axios');

async function testFinancialActivity() {
  console.log('💰 Testing Activity Report with Financial Analysis...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30'
      }
    });
    
    console.log('✅ Activity report with financial analysis working!');
    console.log('📅 Date:', response.data.data.date);
    
    const fuelAnalysis = response.data.data.fuel_analysis;
    console.log('\n💰 Financial Fuel Analysis:');
    console.log(`   📊 Daily Total Consumption: ${fuelAnalysis.daily_total_consumption}L`);
    console.log(`   💵 Daily Total Cost: R${fuelAnalysis.daily_total_cost}`);
    console.log(`   ⛽ Cost per Liter: R${fuelAnalysis.fuel_cost_per_liter}`);
    console.log(`   🏆 Peak Period: ${fuelAnalysis.peak_usage_period.name} (${fuelAnalysis.peak_usage_period.usage.toFixed(2)}L, R${fuelAnalysis.peak_usage_period.cost.toFixed(2)})`);
    console.log(`   🏪 Peak Site: ${fuelAnalysis.peak_usage_site.site || 'None'} (${fuelAnalysis.peak_usage_site.usage.toFixed(2)}L, R${fuelAnalysis.peak_usage_site.cost.toFixed(2)})`);
    
    console.log('\n📊 Period Financial Breakdown:');
    Object.entries(fuelAnalysis.period_breakdown).forEach(([period, data]) => {
      const periodName = period.charAt(0).toUpperCase() + period.slice(1);
      console.log(`   ${periodName}: ${data.fuel_usage}L = R${data.fuel_cost}`);
    });
    
    console.log('\n💼 Financial Breakdown by Site:');
    if (fuelAnalysis.financial_breakdown.daily_total.cost_per_site) {
      Object.entries(fuelAnalysis.financial_breakdown.daily_total.cost_per_site).forEach(([site, cost]) => {
        console.log(`   ${site}: R${cost.toFixed(2)}`);
      });
    } else {
      console.log('   No site-specific costs (no fuel consumption detected)');
    }
    
  } catch (error) {
    console.error('❌ Financial activity test failed:', error.response?.data || error.message);
  }
}

testFinancialActivity();