const ExcelJS = require('exceljs');
const { supabase } = require('./supabase-client');
const path = require('path');

async function testSimpleExcel() {
  try {
    console.log('Creating simple Excel report...');
    
    // Get data
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 6);
    
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_start_time', startDate.toISOString())
      .lte('session_start_time', endDate.toISOString())
      .order('session_start_time', { ascending: false });
    
    if (error) throw error;
    
    // Create workbook
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Fuel Report');
    
    // Add headers
    worksheet.addRow(['Site', 'Date', 'Operating Hours', 'Fuel Usage (L)', 'Cost (R)']);
    
    // Add data
    sessions.forEach(session => {
      worksheet.addRow([
        session.branch,
        session.session_date,
        session.operating_hours || 0,
        session.total_usage || 0,
        session.cost_for_usage || 0
      ]);
    });
    
    // Save file
    const fileName = 'Simple_Report_Test.xlsx';
    const filePath = path.join(__dirname, 'temp', fileName);
    await workbook.xlsx.writeFile(filePath);
    
    console.log(`✅ Simple Excel report created: ${fileName}`);
    console.log(`📊 Data: ${sessions.length} sessions from ${Object.keys(sessions.reduce((acc, s) => ({...acc, [s.branch]: true}), {})).length} sites`);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

testSimpleExcel();