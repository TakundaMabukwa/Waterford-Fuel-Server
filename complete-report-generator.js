require('dotenv').config();
const { supabase } = require('./supabase-client');
const ExcelJS = require('exceljs');

class CompleteReportGenerator {
  
  async generateReport(reportType = 'daily', targetDate = null, cost_code = null) {
    try {
      const reportDate = targetDate ? new Date(targetDate) : new Date();
      const { startDate, endDate, periodName } = this.calculateDateRange(reportType, reportDate);
      
      // Get operating sessions data
      const sessionsData = await this.getOperatingSessionsData(startDate, endDate, cost_code);
      
      // Create Excel workbook
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Fuel Report Summary');
      
      // Setup worksheet exactly like old code
      this.setupWorksheetLayout(worksheet);
      this.addReportHeader(worksheet, periodName, reportType);
      await this.addExpandableReportData(worksheet, sessionsData, reportType);
      
      // Generate filename and upload to bucket
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
      
      // Store in generated reports table
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
          total_sessions: sessionsData.totalSessions,
          total_operating_hours: sessionsData.totalOperatingHours
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
    
    endDate.setHours(23, 59, 59, 999);
    
    switch (reportType) {
      case 'daily':
        startDate.setHours(0, 0, 0, 0);
        periodName = endDate.toISOString().slice(0, 10);
        break;
        
      case 'weekly':
        startDate.setDate(endDate.getDate() - 6);
        startDate.setHours(0, 0, 0, 0);
        periodName = `${startDate.toISOString().slice(0, 10)} to ${endDate.toISOString().slice(0, 10)}`;
        break;
        
      case 'monthly':
        startDate = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
        startDate.setHours(0, 0, 0, 0);
        endDate.setMonth(endDate.getMonth() + 1);
        endDate.setDate(0);
        endDate.setHours(23, 59, 59, 999);
        periodName = `${startDate.getFullYear()}-${(startDate.getMonth() + 1).toString().padStart(2, '0')}`;
        break;
        
      default:
        throw new Error(`Invalid report type: ${reportType}`);
    }
    
    return { startDate, endDate, periodName };
  }
  
  async getOperatingSessionsData(startDate, endDate, cost_code = null) {
    let query = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_start_time', startDate.toISOString())
      .lte('session_start_time', endDate.toISOString());
    
    if (cost_code) {
      query = query.eq('cost_code', cost_code);
    }
    
    const { data: sessions, error } = await query.order('session_start_time', { ascending: false });
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    // Group sessions by branch exactly like old code
    const siteGroups = {};
    sessions.forEach(session => {
      if (!siteGroups[session.branch]) {
        siteGroups[session.branch] = {
          branch: session.branch,
          company: session.company,
          cost_code: session.cost_code,
          sessions: [],
          total_sessions: 0,
          total_operating_hours: 0,
          total_fuel_usage: 0,
          total_fuel_filled: 0,
          total_cost: 0
        };
      }
      
      const site = siteGroups[session.branch];
      site.sessions.push(session);
      site.total_sessions += 1;
      site.total_operating_hours += parseFloat(session.operating_hours || 0);
      site.total_fuel_usage += parseFloat(session.total_usage || 0);
      site.total_fuel_filled += parseFloat(session.total_fill || 0);
      site.total_cost += parseFloat(session.cost_for_usage || 0);
    });
    
    // Calculate averages
    Object.values(siteGroups).forEach(site => {
      if (site.total_operating_hours > 0) {
        site.avg_efficiency = site.total_fuel_usage / site.total_operating_hours;
      } else {
        site.avg_efficiency = 0;
      }
    });
    
    const sites = Object.values(siteGroups);
    const totalSessions = sessions.length;
    const totalOperatingHours = sites.reduce((sum, site) => sum + site.total_operating_hours, 0);
    
    return {
      sites,
      totalSessions,
      totalOperatingHours: totalOperatingHours.toFixed(2)
    };
  }
  
  setupWorksheetLayout(worksheet) {
    worksheet.columns = [
      { width: 4 },   // A: Expand button column
      { width: 20 },  // B: Site
      { width: 12 },  // C: Date  
      { width: 15 },  // D: Operating Hours
      { width: 18 },  // E: Opening Percentage
      { width: 15 },  // F: Opening Fuel
      { width: 18 },  // G: Closing Percentage
      { width: 15 },  // H: Closing Fuel
      { width: 12 },  // I: Usage
      { width: 12 },  // J: Fill
      { width: 15 },  // K: Efficiency
      { width: 12 }   // L: Cost
    ];
    
    worksheet.properties.defaultRowHeight = 20;
  }
  
  addReportHeader(worksheet, periodName, reportType) {
    // Logo space (rows 1-6)
    worksheet.mergeCells('A1:L6');
    const logoCell = worksheet.getCell('A1');
    logoCell.value = 'Energy Rite - Smart Energy Made Simple';
    logoCell.font = { size: 16, bold: true, color: { argb: 'FF666666' } };
    logoCell.alignment = { horizontal: 'left', vertical: 'middle' };
    
    // Title
    worksheet.mergeCells('A7:L7');
    const titleCell = worksheet.getCell('A7');
    titleCell.value = 'FUEL REPORT SUMMARY';
    titleCell.font = { size: 20, bold: true };
    titleCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    // Period
    worksheet.mergeCells('A8:L8');
    const periodCell = worksheet.getCell('A8');
    periodCell.value = periodName;
    periodCell.font = { size: 14, bold: true };
    periodCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    // Empty row
    worksheet.addRow([]);
  }
  
  async addExpandableReportData(worksheet, sessionsData, reportType) {
    // Header row
    const headerRow = worksheet.addRow([
      '', 'Site', 'Date', 'Operating Hours', 'Opening Percentage', 'Opening Fuel',
      'Closing Percentage', 'Closing Fuel', 'Usage', 'Fill', 'Efficiency', 'Cost'
    ]);
    
    // Style header row
    headerRow.eachCell((cell, colNumber) => {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF333333' } };
      cell.alignment = { horizontal: 'center', vertical: 'middle' };
      cell.border = {
        top: { style: 'thin' }, left: { style: 'thin' },
        bottom: { style: 'thin' }, right: { style: 'thin' }
      };
    });
    
    // Add data for each site
    for (const site of sessionsData.sites) {
      const hasSessions = site.sessions && site.sessions.length > 0;
      
      // Summary row for site
      const summaryRow = worksheet.addRow([
        hasSessions ? '+' : '-',
        site.branch,
        hasSessions ? `Total Running Hours` : `No Sessions`,
        this.formatDuration(site.total_operating_hours || 0),
        '', '', '', '',
        (site.total_fuel_usage || 0).toFixed(2),
        (site.total_fuel_filled || 0).toFixed(2),
        (site.avg_efficiency || 0).toFixed(2),
        `R${(site.total_cost || 0).toFixed(2)}`
      ]);
      
      // Style summary row
      summaryRow.eachCell((cell, colNumber) => {
        if (colNumber === 1) {
          cell.font = { bold: true, size: 12 };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        } else {
          cell.font = { bold: true };
        }
        cell.border = {
          top: { style: 'thin' }, left: { style: 'thin' },
          bottom: { style: 'thin' }, right: { style: 'thin' }
        };
      });
      
      // Add detailed session rows if available
      if (site.sessions && site.sessions.length > 0) {
        for (const session of site.sessions) {
          const detailRow = worksheet.addRow([
            '', site.branch, session.session_date,
            this.formatDuration(session.operating_hours || 0),
            `${session.opening_percentage || 0}%`,
            (session.opening_fuel || 0).toFixed(0),
            `${session.closing_percentage || 0}%`,
            (session.closing_fuel || 0).toFixed(0),
            (session.total_usage || 0).toFixed(2),
            (session.total_fill || 0).toFixed(2),
            (session.liter_usage_per_hour || 0).toFixed(2),
            `R${(session.cost_for_usage || 0).toFixed(2)}`
          ]);
          
          // Style detail rows
          detailRow.eachCell((cell, colNumber) => {
            if (colNumber === 2) {
              cell.font = { italic: true };
              cell.alignment = { indent: 1 };
            }
            cell.border = {
              top: { style: 'thin' }, left: { style: 'thin' },
              bottom: { style: 'thin' }, right: { style: 'thin' }
            };
          });
        }
      }
      
      worksheet.addRow([]); // Spacing row
    }
    
    // Add totals summary
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      '', `${reportType.toUpperCase()} TOTALS`, '',
      this.formatDuration(sessionsData.totalOperatingHours),
      '', '', '', '',
      sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_usage, 0).toFixed(2),
      sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_filled, 0).toFixed(2),
      '',
      `R${sessionsData.sites.reduce((sum, site) => sum + site.total_cost, 0).toFixed(2)}`
    ]);
    
    // Style totals row
    totalRow.eachCell((cell, colNumber) => {
      cell.font = { bold: true, size: 12 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F0F0' } };
      cell.border = {
        top: { style: 'thick' }, left: { style: 'thin' },
        bottom: { style: 'thick' }, right: { style: 'thin' }
      };
    });
  }
  
  formatDuration(decimalHours) {
    if (!decimalHours || decimalHours === 0) return '0 minutes';
    
    const hoursNum = parseFloat(decimalHours);
    if (isNaN(hoursNum) || hoursNum === 0) return '0 minutes';
    
    const hours = Math.floor(hoursNum);
    const minutes = Math.round((hoursNum - hours) * 60);
    
    if (hours === 0) {
      return `${minutes} minute${minutes !== 1 ? 's' : ''}`;
    } else if (minutes === 0) {
      return `${hours} hour${hours !== 1 ? 's' : ''}`;
    } else {
      return `${hours} hour${hours !== 1 ? 's' : ''} ${minutes} minute${minutes !== 1 ? 's' : ''}`;
    }
  }
}

// Test all report types
async function testAllReports() {
  const generator = new CompleteReportGenerator();
  
  try {
    console.log('🔄 Generating daily report...');
    const daily = await generator.generateReport('daily');
    console.log('✅ Daily:', daily.download_url);
    
    console.log('🔄 Generating weekly report...');
    const weekly = await generator.generateReport('weekly');
    console.log('✅ Weekly:', weekly.download_url);
    
    console.log('🔄 Generating monthly report...');
    const monthly = await generator.generateReport('monthly');
    console.log('✅ Monthly:', monthly.download_url);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testAllReports();