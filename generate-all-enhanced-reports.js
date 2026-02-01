require('dotenv').config();
const { supabase } = require('./supabase-client');
const ExcelJS = require('exceljs');

class AllEnhancedReports {
  
  async generateReport(reportType = 'daily', targetDate = null) {
    try {
      const reportDate = targetDate ? new Date(targetDate) : new Date();
      const { startDate, endDate, periodName } = this.calculateDateRange(reportType, reportDate);
      
      const sessionsData = await this.getSessionsData(startDate, endDate);
      
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Fuel Report Summary');
      
      this.setupWorksheetLayout(worksheet);
      this.addReportHeader(worksheet, periodName, reportType);
      await this.addExpandableReportData(worksheet, sessionsData, reportType);
      
      const fileName = `Enhanced_${reportType}_Report_${periodName.replace(/[\s\/]/g, '_')}.xlsx`;
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
      
      console.log(`✅ ${reportType} report: ${fileName}`);
      
      return {
        success: true,
        file_name: fileName,
        download_url: publicUrl,
        period: periodName,
        stats: {
          total_sites: sessionsData.sites.length,
          total_sessions: sessionsData.totalSessions
        }
      };
      
    } catch (error) {
      console.error(`❌ ${reportType} error:`, error);
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
        periodName = `${startDate.toISOString().slice(0, 10)}_to_${endDate.toISOString().slice(0, 10)}`;
        break;
      case 'monthly':
        startDate = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
        const monthEndDate = new Date(endDate.getFullYear(), endDate.getMonth() + 1, 0);
        endDate.setTime(monthEndDate.getTime());
        endDate.setHours(23, 59, 59, 999);
        periodName = `${startDate.getFullYear()}-${(startDate.getMonth() + 1).toString().padStart(2, '0')}`;
        break;
    }
    
    return { startDate, endDate, periodName };
  }
  
  async getSessionsData(startDate, endDate) {
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('created_at', startDate.toISOString())
      .lte('created_at', endDate.toISOString())
      .order('created_at', { ascending: false });
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    const siteGroups = {};
    sessions.forEach(session => {
      if (!siteGroups[session.plate]) {
        siteGroups[session.plate] = {
          branch: session.plate,
          sessions: [],
          total_sessions: 0,
          total_operating_hours: 0,
          total_fuel_usage: 0,
          total_fuel_filled: 0
        };
      }
      
      const site = siteGroups[session.plate];
      site.sessions.push(session);
      site.total_sessions += 1;
      site.total_operating_hours += parseFloat(session.operating_hours || 0);
      site.total_fuel_usage += parseFloat(session.total_usage || session.fuel_consumed || 0);
      site.total_fuel_filled += parseFloat(session.total_fill || 0);
    });
    
    const sites = Object.values(siteGroups);
    const totalSessions = sessions.length;
    
    return { sites, totalSessions };
  }
  
  setupWorksheetLayout(worksheet) {
    worksheet.columns = [
      { width: 4 }, { width: 20 }, { width: 12 }, { width: 15 },
      { width: 18 }, { width: 15 }, { width: 18 }, { width: 15 },
      { width: 12 }, { width: 12 }, { width: 15 }, { width: 12 }
    ];
  }
  
  addReportHeader(worksheet, periodName, reportType) {
    worksheet.mergeCells('A1:L6');
    const logoCell = worksheet.getCell('A1');
    logoCell.value = 'Energy Rite - Smart Energy Made Simple';
    logoCell.font = { size: 16, bold: true, color: { argb: 'FF666666' } };
    logoCell.alignment = { horizontal: 'left', vertical: 'middle' };
    
    worksheet.mergeCells('A7:L7');
    const titleCell = worksheet.getCell('A7');
    titleCell.value = `${reportType.toUpperCase()} FUEL REPORT SUMMARY`;
    titleCell.font = { size: 20, bold: true };
    titleCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    worksheet.mergeCells('A8:L8');
    const periodCell = worksheet.getCell('A8');
    periodCell.value = periodName.replace(/_/g, ' ');
    periodCell.font = { size: 14, bold: true };
    periodCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    worksheet.addRow([]);
  }
  
  async addExpandableReportData(worksheet, sessionsData, reportType) {
    const headerRow = worksheet.addRow([
      '', 'Site', 'Date', 'Duration', 'Opening %', 'Opening Fuel',
      'Closing %', 'Closing Fuel', 'Usage', 'Fill', 'Efficiency', 'Status'
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
    
    for (const site of sessionsData.sites) {
      const hasSessions = site.sessions.length > 0;
      
      const summaryRow = worksheet.addRow([
        hasSessions ? '+' : '-',
        site.branch,
        `${reportType} Summary`,
        `${site.total_operating_hours.toFixed(1)}h`,
        '', '',
        '', '',
        `${site.total_fuel_usage.toFixed(2)}L`,
        `${site.total_fuel_filled.toFixed(2)}L`,
        site.total_operating_hours > 0 ? `${(site.total_fuel_usage / site.total_operating_hours).toFixed(2)}L/h` : '0',
        `${site.total_sessions} sessions`
      ]);
      
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
      
      if (hasSessions) {
        for (const session of site.sessions) {
          const detailRow = worksheet.addRow([
            '', site.branch,
            new Date(session.created_at).toLocaleDateString(),
            `${session.duration_minutes || 0}min`,
            `${session.opening_percentage || 0}%`,
            `${(session.opening_fuel || 0).toFixed(1)}L`,
            `${session.closing_percentage || 0}%`,
            `${(session.closing_fuel || 0).toFixed(1)}L`,
            `${(session.fuel_consumed || session.total_usage || 0).toFixed(2)}L`,
            `${(session.total_fill || 0).toFixed(2)}L`,
            session.duration_minutes > 0 ? `${((session.fuel_consumed || 0) / (session.duration_minutes / 60)).toFixed(2)}L/h` : '0',
            session.session_status || 'COMPLETED'
          ]);
          
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
      
      worksheet.addRow([]);
    }
    
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      '', `${reportType.toUpperCase()} TOTALS`, '',
      `${sessionsData.sites.reduce((sum, site) => sum + site.total_operating_hours, 0).toFixed(1)}h`,
      '', '', '', '',
      `${sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_usage, 0).toFixed(2)}L`,
      `${sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_filled, 0).toFixed(2)}L`,
      '', `${sessionsData.totalSessions} total`
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

async function generateAllReports() {
  const generator = new AllEnhancedReports();
  
  try {
    console.log('🔄 Generating all enhanced reports...\n');
    
    const daily = await generator.generateReport('daily');
    console.log('📊 Daily Stats:', daily.stats);
    
    const weekly = await generator.generateReport('weekly');
    console.log('📊 Weekly Stats:', weekly.stats);
    
    const monthly = await generator.generateReport('monthly');
    console.log('📊 Monthly Stats:', monthly.stats);
    
    console.log('\n🎉 All enhanced reports generated successfully!');
    console.log('📄 Daily:', daily.download_url);
    console.log('📄 Weekly:', weekly.download_url);
    console.log('📄 Monthly:', monthly.download_url);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

generateAllReports();