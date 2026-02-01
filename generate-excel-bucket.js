require('dotenv').config();
const { supabase } = require('./supabase-client');
const ExcelJS = require('exceljs');
const axios = require('axios');

async function generateExcelToBucket() {
  try {
    // Get report data
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportData = response.data.data;
    
    // Create Excel workbook
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Fuel Report Summary');
    
    // Setup columns
    worksheet.columns = [
      { width: 20 }, { width: 15 }, { width: 15 }, { width: 12 }, { width: 15 }
    ];
    
    // Add header
    worksheet.mergeCells('A1:E1');
    worksheet.getCell('A1').value = 'FUEL REPORT SUMMARY';
    worksheet.getCell('A1').font = { size: 16, bold: true };
    worksheet.getCell('A1').alignment = { horizontal: 'center' };
    
    worksheet.mergeCells('A2:E2');
    worksheet.getCell('A2').value = reportData.period;
    worksheet.getCell('A2').font = { size: 12, bold: true };
    worksheet.getCell('A2').alignment = { horizontal: 'center' };
    
    worksheet.addRow([]);
    
    // Add column headers
    const headerRow = worksheet.addRow(['Site', 'Cost Code', 'Fuel Level', 'Monthly Usage', 'Operating Hours']);
    headerRow.eachCell(cell => {
      cell.font = { bold: true };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0E0E0' } };
    });
    
    // Add data
    reportData.sites.forEach(site => {
      worksheet.addRow([
        site.branch,
        site.cost_code,
        `${site.current_fuel_level}L`,
        `${site.monthly_data.total_fuel_usage}L`,
        `${site.monthly_data.total_running_hours}h`
      ]);
    });
    
    // Generate buffer
    const buffer = await workbook.xlsx.writeBuffer();
    
    // Upload to bucket
    const filename = `daily-report-${new Date().toISOString().split('T')[0]}.xlsx`;
    
    const { data, error } = await supabase.storage
      .from('reports')
      .upload(filename, buffer, {
        contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        upsert: true
      });
    
    if (error) throw error;
    
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl(filename);
    
    console.log('✅ Excel report uploaded:', publicUrl);
    console.log('📊 Sites included:', reportData.sites.length);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

generateExcelToBucket();