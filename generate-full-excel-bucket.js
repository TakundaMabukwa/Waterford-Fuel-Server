require('dotenv').config();
const { supabase } = require('./supabase-client');
const ExcelJS = require('exceljs');
const axios = require('axios');

async function generateFullExcelReport() {
  try {
    // Get operating sessions data
    const startDate = new Date();
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date();
    endDate.setHours(23, 59, 59, 999);
    
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_start_time', startDate.toISOString())
      .lte('session_start_time', endDate.toISOString())
      .order('session_start_time', { ascending: false });
    
    if (error) throw error;
    
    // Group sessions by branch
    const siteGroups = {};
    sessions.forEach(session => {
      if (!siteGroups[session.branch]) {
        siteGroups[session.branch] = {
          branch: session.branch,
          company: session.company,
          cost_code: session.cost_code,
          sessions: [],
          total_operating_hours: 0,
          total_fuel_usage: 0,
          total_fuel_filled: 0,
          total_cost: 0
        };
      }
      
      const site = siteGroups[session.branch];
      site.sessions.push(session);
      site.total_operating_hours += parseFloat(session.operating_hours || 0);
      site.total_fuel_usage += parseFloat(session.total_usage || 0);
      site.total_fuel_filled += parseFloat(session.total_fill || 0);
      site.total_cost += parseFloat(session.cost_for_usage || 0);
    });
    
    const sites = Object.values(siteGroups);
    
    // Create Excel workbook
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Fuel Report Summary');
    
    // Setup columns
    worksheet.columns = [
      { width: 4 },   // Expand button
      { width: 20 },  // Site
      { width: 12 },  // Date
      { width: 15 },  // Operating Hours
      { width: 18 },  // Opening Percentage
      { width: 15 },  // Opening Fuel
      { width: 18 },  // Closing Percentage
      { width: 15 },  // Closing Fuel
      { width: 12 },  // Usage
      { width: 12 },  // Fill
      { width: 15 },  // Efficiency
      { width: 12 }   // Cost
    ];
    
    // Add header with logo space
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
    periodCell.value = new Date().toISOString().slice(0, 10);
    periodCell.font = { size: 14, bold: true };
    periodCell.alignment = { horizontal: 'right', vertical: 'middle' };
    
    worksheet.addRow([]);
    
    // Header row
    const headerRow = worksheet.addRow([
      '', 'Site', 'Date', 'Operating Hours', 'Opening Percentage', 'Opening Fuel',
      'Closing Percentage', 'Closing Fuel', 'Usage', 'Fill', 'Efficiency', 'Cost'
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
    
    // Add data for each site
    for (const site of sites) {
      const hasSessions = site.sessions.length > 0;
      
      // Summary row
      const summaryRow = worksheet.addRow([
        hasSessions ? '+' : '-',
        site.branch,
        hasSessions ? 'Total Running Hours' : 'No Sessions',
        formatDuration(site.total_operating_hours),
        '', '', '', '',
        site.total_fuel_usage.toFixed(2),
        site.total_fuel_filled.toFixed(2),
        site.total_operating_hours > 0 ? (site.total_fuel_usage / site.total_operating_hours).toFixed(2) : '0',
        `R${site.total_cost.toFixed(2)}`
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
      
      // Detail rows
      if (hasSessions) {
        for (const session of site.sessions) {
          const detailRow = worksheet.addRow([
            '', site.branch, session.session_date,
            formatDuration(session.operating_hours || 0),
            `${session.opening_percentage || 0}%`,
            (session.opening_fuel || 0).toFixed(0),
            `${session.closing_percentage || 0}%`,
            (session.closing_fuel || 0).toFixed(0),
            (session.total_usage || 0).toFixed(2),
            (session.total_fill || 0).toFixed(2),
            (session.liter_usage_per_hour || 0).toFixed(2),
            `R${(session.cost_for_usage || 0).toFixed(2)}`
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
    
    // Totals row
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      '', 'DAILY TOTALS', '',
      formatDuration(sites.reduce((sum, site) => sum + site.total_operating_hours, 0)),
      '', '', '', '',
      sites.reduce((sum, site) => sum + site.total_fuel_usage, 0).toFixed(2),
      sites.reduce((sum, site) => sum + site.total_fuel_filled, 0).toFixed(2),
      '',
      `R${sites.reduce((sum, site) => sum + site.total_cost, 0).toFixed(2)}`
    ]);
    
    totalRow.eachCell(cell => {
      cell.font = { bold: true, size: 12 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F0F0' } };
      cell.border = {
        top: { style: 'thick' }, left: { style: 'thin' },
        bottom: { style: 'thick' }, right: { style: 'thin' }
      };
    });
    
    // Generate buffer and upload
    const buffer = await workbook.xlsx.writeBuffer();
    const filename = `Energy_Rite_daily_Report_${new Date().toISOString().slice(0, 10)}.xlsx`;
    
    const { data, error: uploadError } = await supabase.storage
      .from('reports')
      .upload(filename, buffer, {
        contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        upsert: true
      });
    
    if (uploadError) throw uploadError;
    
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl(filename);
    
    console.log('✅ Full Excel report uploaded:', publicUrl);
    console.log('📊 Sites:', sites.length, 'Sessions:', sessions.length);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

function formatDuration(decimalHours) {
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

generateFullExcelReport();