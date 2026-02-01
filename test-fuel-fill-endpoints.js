require('dotenv').config();

console.log('🚛 FUEL FILL DATA ENDPOINTS GUIDE');
console.log('===========================================\n');

console.log('📍 ENDPOINT OVERVIEW:');
console.log('1️⃣ Daily Activity Report: /api/energy-rite/reports/activity');
console.log('2️⃣ Multi-Day Report: /api/energy-rite/activity-reports');
console.log('3️⃣ Direct Database: energy_rite_fuel_fills table');
console.log('');

console.log('🔧 USAGE EXAMPLES:');
console.log('');

console.log('📊 GET TODAY\'S FUEL FILLS:');
console.log('   curl "http://localhost:4000/api/energy-rite/reports/activity"');
console.log('');

console.log('📅 GET SPECIFIC DATE:');
console.log('   curl "http://localhost:4000/api/energy-rite/reports/activity?date=2025-11-07"');
console.log('');

console.log('📈 GET 7-DAY TREND:');
console.log('   curl "http://localhost:4000/api/energy-rite/activity-reports?days=7"');
console.log('');

console.log('🎯 FUEL FILL DETECTION METHODS:');
console.log('');
console.log('Method 1: STATUS-BASED (Real-time WebSocket)');
console.log('   ✅ Detects "Possible Fuel Fill" status');
console.log('   ✅ Logs to energy_rite_fuel_fills table');
console.log('   ✅ Immediate notification');
console.log('');

console.log('Method 2: LEVEL-BASED (Smart Analysis)');
console.log('   ✅ Fuel increase >20L within 60 minutes');
console.log('   ✅ Percentage increase >15%');
console.log('   ✅ Time-window validation');
console.log('');

console.log('Method 3: PERIOD-BASED (Report Analysis)');
console.log('   ✅ Daily period comparisons');
console.log('   ✅ Morning/Afternoon/Evening snapshots');
console.log('   ✅ Shows "FUEL_FILL_DETECTED" instead of consumption');
console.log('');

console.log('📋 RESPONSE FORMAT:');
console.log(`{
  "date": "2025-11-07",
  "sites": [
    {
      "siteName": "SITE_NAME", 
      "sessions": [...],
      "fuelAnalysis": {
        "dailyConsumption": 25.5,
        "fuelFills": [
          {
            "time": "10:30:00",
            "amount": 45.2,
            "method": "STATUS_BASED",
            "confidence": "HIGH"
          }
        ],
        "periods": {
          "morning": {
            "consumption": 12.3,
            "fills": [...],
            "status": "FUEL_FILL_DETECTED"
          }
        }
      }
    }
  ]
}`);

console.log('');
console.log('💡 PRO TIP: The activity report automatically detects and flags fuel fills!');
console.log('🚀 Your system is already tracking fuel fills comprehensively!');