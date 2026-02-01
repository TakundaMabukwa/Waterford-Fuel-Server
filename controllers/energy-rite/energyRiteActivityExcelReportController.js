const ExcelJS = require('exceljs');
const { supabase } = require('../../supabase-client');
const axios = require('axios');
const path = require('path');
const fs = require('fs');

function setupWorksheetLayout(worksheet) {
  worksheet.columns = [
    { width: 20 }, // Site
    { width: 15 }, // Morning Usage
    { width: 15 }, // Morning Cost
    { width: 15 }, // Afternoon Usage
    { width: 15 }, // Afternoon Cost
    { width: 15 }, // Full Day Usage
    { width: 15 }, // Full Day Cost
    { width: 18 }, // Peak Period
    { width: 12 }, // Peak Usage
    { width: 12 }, // Sessions
    { width: 15 }  // Operating Hours
  ];
}

function addReportHeader(worksheet, activityData, costCode, siteId) {
  const workbook = worksheet.workbook;
  
  // Logo space (rows 1-6)
  worksheet.mergeCells('A1:K6');
  const logoCell = worksheet.getCell('A1');
  logoCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
  
  // Try to add logo image
  const fs = require('fs');
  const path = require('path');
  const logoPath = path.join(__dirname, '../../assets/logo.png');
  
  try {
    if (fs.existsSync(logoPath)) {
      const logoBuffer = fs.readFileSync(logoPath);
      const imageId = workbook.addImage({
        buffer: logoBuffer,
        extension: 'png'
      });
      worksheet.addImage(imageId, {
        tl: { col: 0.1, row: 0.1 },
        br: { col: 5.9, row: 5.9 }
      });
    }
  } catch (error) {
    console.log('Logo not found, using text header');
  }
  
  logoCell.value = '';
  logoCell.alignment = { horizontal: 'left', vertical: 'middle' };
  
  // Title with professional styling
  worksheet.mergeCells('A7:K7');
  const titleCell = worksheet.getCell('A7');
  titleCell.value = 'ACTIVITY FUEL USAGE REPORT';
  titleCell.font = { size: 20, bold: true, color: { argb: 'FFFFFFFF' } };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF333333' } };
  titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  titleCell.border = {
    top: { style: 'medium', color: { argb: 'FF666666' } },
    bottom: { style: 'medium', color: { argb: 'FF666666' } }
  };
  
  // Period with accent styling
  worksheet.mergeCells('A8:K8');
  const periodCell = worksheet.getCell('A8');
  periodCell.value = `Date: ${activityData.date}`;
  periodCell.font = { size: 12, bold: true, color: { argb: 'FF333333' } };
  periodCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
  periodCell.alignment = { horizontal: 'center', vertical: 'middle' };
  periodCell.border = {
    bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } }
  };
  
  // Cost Code / Site
  worksheet.mergeCells('A9:K9');
  const filterCell = worksheet.getCell('A9');
  filterCell.value = siteId ? `Site: ${siteId}` : `Cost Center: ${costCode || 'ALL COST CENTERS'}`;
  filterCell.font = { size: 12, bold: true, color: { argb: 'FF333333' } };
  filterCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
  filterCell.alignment = { horizontal: 'center', vertical: 'middle' };
  
  // Summary
  worksheet.mergeCells('A10:K10');
  const summaryCell = worksheet.getCell('A10');
  const peakPeriod = activityData.fuel_analysis.peak_usage_period;
  summaryCell.value = `Peak Period: ${peakPeriod.name} (${peakPeriod.usage.toFixed(1)}L, R${peakPeriod.cost.toFixed(2)})`;
  summaryCell.font = { size: 11, italic: true, color: { argb: 'FF666666' } };
  summaryCell.alignment = { horizontal: 'center', vertical: 'middle' };
  
  // Empty row with subtle background
  const emptyRow = worksheet.addRow([]);
  emptyRow.eachCell((cell) => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFAFAFA' } };
  });
}

function addActivityData(worksheet, activityData) {
  const fuelAnalysis = activityData.fuel_analysis;
  
  // Header row
  const headerRow = worksheet.addRow([
    'Site',
    'Morning Usage (6AM-12PM)',
    'Morning Cost (R)',
    'Afternoon Usage (12PM-5PM)',
    'Afternoon Cost (R)',
    'Evening Usage (5PM-11PM)',
    'Evening Cost (R)',
    'Daily Total Usage',
    'Daily Total Cost',
    'Sessions',
    'Operating Hours'
  ]);
  
  // Style header
  headerRow.eachCell(cell => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF666666' } };
    cell.alignment = { horizontal: 'center', vertical: 'middle' };
    cell.border = {
      top: { style: 'thin' }, left: { style: 'thin' },
      bottom: { style: 'thin' }, right: { style: 'thin' }
    };
  });
  
  // Add site data
  activityData.sites.forEach(site => {
    if (site.session_count > 0) {
      // Get fuel costs from financial breakdown
      const morningCost = fuelAnalysis.financial_breakdown.morning.cost_per_site[site.branch] || 0;
      const afternoonCost = fuelAnalysis.financial_breakdown.afternoon.cost_per_site[site.branch] || 0;
      const eveningCost = fuelAnalysis.financial_breakdown.evening.cost_per_site[site.branch] || 0;
      const dailyCost = fuelAnalysis.financial_breakdown.daily_total.cost_per_site[site.branch] || 0;
      
      const row = worksheet.addRow([
        site.branch,
        `${(morningCost / fuelAnalysis.fuel_cost_per_liter).toFixed(2)}L`,
        `R${morningCost.toFixed(2)}`,
        `${(afternoonCost / fuelAnalysis.fuel_cost_per_liter).toFixed(2)}L`,
        `R${afternoonCost.toFixed(2)}`,
        `${(eveningCost / fuelAnalysis.fuel_cost_per_liter).toFixed(2)}L`,
        `R${eveningCost.toFixed(2)}`,
        `${(dailyCost / fuelAnalysis.fuel_cost_per_liter).toFixed(2)}L`,
        `R${dailyCost.toFixed(2)}`,
        site.session_count,
        `${site.total_operating_hours.toFixed(2)}h`
      ]);
      
      // Style data rows
      row.eachCell((cell, colNumber) => {
        cell.border = {
          top: { style: 'thin' }, left: { style: 'thin' },
          bottom: { style: 'thin' }, right: { style: 'thin' }
        };
        
        // Highlight cost columns with light yellow background
        if ([3, 5, 7, 9].includes(colNumber)) {
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF9C4' } };
        }
      });
    }
  });
  
  // Add totals row
  worksheet.addRow([]);
  const totalRow = worksheet.addRow([
    'TOTALS',
    fuelAnalysis.period_breakdown.morning.fuel_usage + 'L',
    'R' + fuelAnalysis.period_breakdown.morning.fuel_cost,
    fuelAnalysis.period_breakdown.afternoon.fuel_usage + 'L',
    'R' + fuelAnalysis.period_breakdown.afternoon.fuel_cost,
    fuelAnalysis.period_breakdown.evening.fuel_usage + 'L',
    'R' + fuelAnalysis.period_breakdown.evening.fuel_cost,
    fuelAnalysis.daily_total_consumption + 'L',
    'R' + fuelAnalysis.daily_total_cost,
    activityData.summary.total_sessions,
    `${activityData.summary.total_operating_hours.toFixed(2)}h`
  ]);
  
  // Style totals row
  totalRow.eachCell((cell, colNumber) => {
    cell.font = { bold: true };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F0F0' } };
    cell.border = {
      top: { style: 'thick' }, left: { style: 'thin' },
      bottom: { style: 'thick' }, right: { style: 'thin' }
    };
    
    // Highlight total cost columns with darker yellow
    if ([3, 5, 7, 9].includes(colNumber)) {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFDE68A' } };
    }
  });
}

class EnergyRiteActivityExcelReportController {
  

  
  async generateActivityExcelReport(req, res) {
    try {
      const { cost_code, date, site_id } = req.query;
      
      // Get activity report data via API call (using enhanced endpoint)
      const params = new URLSearchParams();
      if (cost_code) params.append('cost_code', cost_code);
      if (site_id) params.append('site_id', site_id);
      if (date) params.append('date', date);
      
      const response = await axios.get(`http://localhost:4000/api/energy-rite/reports/activity?${params.toString()}`);
      const activityData = response.data.data;
      
      // Create Excel workbook
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Activity Report');
      
      // Setup worksheet
      setupWorksheetLayout(worksheet);
      addReportHeader(worksheet, activityData, cost_code, site_id);
      addActivityData(worksheet, activityData);
      
      // Generate filename
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
      const costCodeSuffix = cost_code ? `_${cost_code}` : (site_id ? `_${site_id}` : '_ALL');
      const fileName = `Activity_Report${costCodeSuffix}_${activityData.date}_${timestamp}.xlsx`;
      
      // Save to buffer
      const buffer = await workbook.xlsx.writeBuffer();
      
      // Upload to Supabase storage
      const bucketPath = `activity-reports/${new Date().getFullYear()}/${fileName}`;
      const { data: uploadData, error: uploadError } = await supabase.storage
        .from('energyrite-reports')
        .upload(bucketPath, buffer, {
          contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          upsert: true
        });
      
      if (uploadError) throw uploadError;
      
      // Get public URL
      const { data: { publicUrl } } = supabase.storage
        .from('energyrite-reports')
        .getPublicUrl(bucketPath);
      
      // Send automated email if requested
      if (req.query.sendEmail === 'true') {
        try {
          const emailService = require('../../services/energy-rite/emailService');
          const emailResult = await emailService.sendReportEmail({
            reportType: 'activity',
            period: activityData.date,
            fileName: fileName,
            downloadUrl: publicUrl,
            costCode: cost_code || null,
            siteId: site_id || null,
            stats: {
              total_sites: activityData.summary.total_sites,
              total_sessions: activityData.summary.total_sessions,
              total_operating_hours: activityData.summary.total_operating_hours
            }
          });
          
          console.log('📧 Email result:', emailResult);
        } catch (emailError) {
          console.error('📧 Email sending failed:', emailError.message);
        }
      }
      
      res.status(200).json({
        success: true,
        message: 'Activity Excel report generated successfully',
        data: {
          file_name: fileName,
          download_url: publicUrl,
          date: activityData.date,
          cost_code: cost_code || 'ALL',
          site_id: site_id || null,
          total_sites: activityData.summary.total_sites,
          file_size: buffer.length
        }
      });
      
    } catch (error) {
      console.error('Error generating activity Excel report:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
  

}

module.exports = new EnergyRiteActivityExcelReportController();