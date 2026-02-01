require('dotenv').config();
const { supabase } = require('./supabase-client');
const ExcelJS = require('exceljs');

class FixedReportGenerator {
  
  async generateReport(reportType = 'daily', targetDate = null, cost_code = null) {
    try {
      const reportDate = targetDate ? new Date(targetDate) : new Date();
      const { startDate, endDate, periodName } = this.calculateDateRange(reportType, reportDate);
      
      // Get fuel data (our main data source)
      const fuelData = await this.getFuelData(startDate, endDate);
      
      // Process into sessions-like structure
      const sessionsData = this.processFuelDataToSessions(fuelData);
      
      // Create Excel workbook
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Fuel Report Summary');
      
      this.setupWorksheetLayout(worksheet);
      this.addReportHeader(worksheet, periodName, reportType);
      await this.addReportData(worksheet, sessionsData, reportType);
      
      // Upload to bucket
      const fileName = `Energy_Rite_${reportType}_Report_${periodName.replace(/\\s+/g, '_')}.xlsx`;
      const buffer = await workbook.xlsx.writeBuffer();
      
      const { data, error } = await supabase.storage
        .from('reports')
        .upload(fileName, buffer, {
          contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          upsert: true
        });
      
      if (error) throw error;
      
      const { data: { publicUrl } } = supabase.storage
        .from('reports')
        .getPublicUrl(fileName);
      
      // Store in database
      const { data: dbData, error: dbError } = await supabase
        .from('energy_rite_generated_reports')
        .insert({
          cost_code: cost_code || 'ALL',
          report_type: reportType,
          report_url: publicUrl,
          report_date: reportDate.toISOString().split('T')[0],
          file_size: buffer.length,
          status: 'generated'
        })
        .select();
      
      if (dbError && !dbError.message.includes('duplicate key')) throw dbError;
      
      console.log(`✅ ${reportType} report generated: ${fileName}`);
      
      return {
        success: true,
        file_name: fileName,
        download_url: publicUrl,
        period: periodName,
        report_type: reportType,
        stats: {
          total_sites: sessionsData.sites.length,
          total_records: sessionsData.totalRecords
        }
      };
      
    } catch (error) {
      console.error('❌ Error generating report:', error);
      throw error;
    }
  }
  
  calculateDateRange(reportType, targetDate) {
    const endDate = new Date(targetDate);
    let startDate = new Date(targetDate);
    let periodName = '';
    
    switch (reportType) {
      case 'daily':
        startDate.setHours(0, 0, 0, 0);
        endDate.setHours(23, 59, 59, 999);
        periodName = endDate.toISOString().slice(0, 10);
        break;
        
      case 'weekly':
        startDate.setDate(endDate.getDate() - 6);
        startDate.setHours(0, 0, 0, 0);
        endDate.setHours(23, 59, 59, 999);
        periodName = `${startDate.toISOString().slice(0, 10)} to ${endDate.toISOString().slice(0, 10)}`;
        break;
        
      case 'monthly':
        startDate = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
        endDate = new Date(endDate.getFullYear(), endDate.getMonth() + 1, 0);
        endDate.setHours(23, 59, 59, 999);
        periodName = `${startDate.getFullYear()}-${(startDate.getMonth() + 1).toString().padStart(2, '0')}`;
        break;
    }
    
    return { startDate, endDate, periodName };
  }
  
  async getFuelData(startDate, endDate) {
    const { data, error } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .gte('created_at', startDate.toISOString())
      .lte('created_at', endDate.toISOString())
      .order('plate')
      .order('created_at');
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    return data;
  }
  
  processFuelDataToSessions(fuelData) {
    const siteGroups = {};
    
    fuelData.forEach(record => {
      if (!siteGroups[record.plate]) {
        siteGroups[record.plate] = {
          branch: record.plate,
          records: [],
          total_fuel_usage: 0,
          total_fuel_filled: 0,
          current_fuel_level: 0,
          record_count: 0
        };
      }
      
      const site = siteGroups[record.plate];
      site.records.push(record);
      site.current_fuel_level = record.fuel_probe_1_level || 0;
      site.record_count++;
    });
    
    // Calculate fuel usage/fills for each site
    Object.values(siteGroups).forEach(site => {
      for (let i = 1; i < site.records.length; i++) {
        const current = parseFloat(site.records[i].fuel_probe_1_level || 0);
        const previous = parseFloat(site.records[i-1].fuel_probe_1_level || 0);
        const difference = current - previous;
        
        if (difference < 0) {
          site.total_fuel_usage += Math.abs(difference);
        } else if (difference > 0) {
          site.total_fuel_filled += difference;
        }
      }
    });
    
    const sites = Object.values(siteGroups);
    const totalRecords = fuelData.length;
    
    return { sites, totalRecords };
  }
  
  setupWorksheetLayout(worksheet) {
    worksheet.columns = [
      { width: 20 }, // Site
      { width: 15 }, // Current Fuel
      { width: 15 }, // Total Usage
      { width: 15 }, // Total Filled
      { width: 12 }, // Records
      { width: 20 }  // Status
    ];
  }
  
  addReportHeader(worksheet, periodName, reportType) {
    worksheet.mergeCells('A1:F6');
    const logoCell = worksheet.getCell('A1');
    logoCell.value = 'Energy Rite - Smart Energy Made Simple';
    logoCell.font = { size: 16, bold: true, color: { argb: 'FF666666' } };
    logoCell.alignment = { horizontal: 'left', vertical: 'middle' };
    
    worksheet.mergeCells('A7:F7');
    const titleCell = worksheet.getCell('A7');
    titleCell.value = 'FUEL REPORT SUMMARY';
    titleCell.font = { size: 20, bold: true };
    titleCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    worksheet.mergeCells('A8:F8');
    const periodCell = worksheet.getCell('A8');
    periodCell.value = periodName;
    periodCell.font = { size: 14, bold: true };
    periodCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    worksheet.addRow([]);
  }
  
  async addReportData(worksheet, sessionsData, reportType) {
    const headerRow = worksheet.addRow([
      'Site', 'Current Fuel Level', 'Total Usage', 'Total Filled', 'Data Points', 'Status'
    ]);
    
    headerRow.eachCell(cell => {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF333333' } };
      cell.alignment = { horizontal: 'center', vertical: 'middle' };
      cell.border = {
        top: { style: 'thin' }, left: { style: 'thin' },
        bottom: { style: 'thin' }, right: { style: 'thin' }
      };
    });
    
    // Add data rows
    for (const site of sessionsData.sites) {
      const dataRow = worksheet.addRow([
        site.branch,
        `${site.current_fuel_level.toFixed(1)}L`,
        `${site.total_fuel_usage.toFixed(2)}L`,
        `${site.total_fuel_filled.toFixed(2)}L`,
        site.record_count,
        site.record_count > 0 ? 'Active' : 'No Data'
      ]);
      
      dataRow.eachCell(cell => {
        cell.border = {
          top: { style: 'thin' }, left: { style: 'thin' },
          bottom: { style: 'thin' }, right: { style: 'thin' }
        };
      });
    }
    
    // Totals row
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      `${reportType.toUpperCase()} TOTALS`,
      '',
      `${sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_usage, 0).toFixed(2)}L`,
      `${sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_filled, 0).toFixed(2)}L`,
      sessionsData.totalRecords,
      `${sessionsData.sites.length} Sites`
    ]);
    
    totalRow.eachCell(cell => {
      cell.font = { bold: true, size: 12 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F0F0' } };
      cell.border = {
        top: { style: 'thick' }, left: { style: 'thin' },
        bottom: { style: 'thick' }, right: { style: 'thin' }
      };
    });
  }
}

// Test with new system
async function testNewSystem() {
  const generator = new FixedReportGenerator();
  
  try {
    const daily = await generator.generateReport('daily');
    console.log('✅ Daily report (new system):', daily.download_url);
    console.log('📊 Stats:', daily.stats);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testNewSystem();