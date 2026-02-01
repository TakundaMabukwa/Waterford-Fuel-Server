require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importMonthly7() {
  try {
    console.log('📊 Importing Monthly (7).xlsx...');
    
    const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);

    console.log(`📊 Found ${data.length} rows`);
    console.log('Sample rows:', data.slice(0, 5));

    let imported = 0;
    for (const row of data) {
      const site = row['FUEL REPORT SUMMARY'];
      const date = row['__EMPTY'];
      const operatingHours = row['__EMPTY_1'];
      
      if (!site || !date || !operatingHours || 
          site.includes('FUEL REPORT SUMMARY') || 
          site.includes('Site') || 
          date.includes('Date') || 
          date.includes('Total Running Hours')) {
        continue;
      }
      
      const sessionData = {
        branch: site.trim(),
        company: 'KFC',
        cost_code: 'KFC-0001-0001-0003',
        session_date: new Date('2024-12-01').toISOString().split('T')[0],
        session_start_time: new Date('2024-12-01T06:00:00').toISOString(),
        session_end_time: new Date('2024-12-01T18:00:00').toISOString(),
        operating_hours: parseFloat(operatingHours) || 0,
        opening_fuel: parseFloat(row['__EMPTY_3']) || 0,
        closing_fuel: parseFloat(row['__EMPTY_5']) || 0,
        total_usage: parseFloat(row['__EMPTY_6']) || 0,
        total_fill: parseFloat(row['__EMPTY_7']) || 0,
        cost_for_usage: parseFloat(row['__EMPTY_9']) || 0,
        liter_usage_per_hour: parseFloat(row['__EMPTY_8']) || 0,
        session_status: 'COMPLETED'
      };

      const { error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert(sessionData);

      if (!error) imported++;
    }

    console.log(`✅ Imported ${imported}/${data.length} sessions`);
    if (imported > 0) console.log('✅ Monthly (7) data imported successfully!');
    else console.log('⚠️ No valid data rows found to import');
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

importMonthly7();