require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importMonthly7() {
  try {
    const workbook = XLSX.readFile('./historical-imports/Monthly (8).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);

    console.log(`📊 Found ${data.length} rows`);
    console.log('Sample row:', data[0]);

    let imported = 0;
    for (const row of data) {
      if (!row.Branch || !row['Total Usage']) continue;

      const sessionData = {
        branch: row.Branch,
        company: row.Company || 'KFC',
        cost_code: row['Cost Code'],
        session_date: new Date().toISOString().split('T')[0],
        session_start_time: new Date().toISOString(),
        session_end_time: new Date().toISOString(),
        operating_hours: parseFloat(row['Operating Hours']) || 0,
        opening_fuel: parseFloat(row['Opening Fuel']) || 0,
        closing_fuel: parseFloat(row['Closing Fuel']) || 0,
        total_usage: parseFloat(row['Total Usage']) || 0,
        total_fill: parseFloat(row['Total Fill']) || 0,
        cost_for_usage: parseFloat(row['Cost for Usage']) || 0,
        liter_usage_per_hour: parseFloat(row['Liter Usage per Hour']) || 0,
        session_status: 'COMPLETED'
      };

      const { error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert(sessionData);

      if (!error) imported++;
    }

    console.log(`✅ Imported ${imported}/${data.length} sessions`);
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

importMonthly7();