const ExcelJS = require('exceljs');
const { supabase } = require('../../supabase-client');
const axios = require('axios');

function setupWorksheetLayout(worksheet) {
  worksheet.columns = [
    { width: 22 }, // Vehicle
    { width: 16 }, // Morning Usage
    { width: 18 }, // Afternoon Usage
    { width: 16 }, // Total Usage
    { width: 14 }, // Tank 1
    { width: 14 }, // Tank 2
    { width: 14 }, // Fuel Fills
    { width: 12 }, // Sessions
    { width: 16 }, // Operating Hours
  ];
}

function addReportHeader(worksheet, period, costCode, siteId, summary) {
  worksheet.mergeCells('A1:I1');
  const spacerCell = worksheet.getCell('A1');
  spacerCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
  spacerCell.value = '';

  worksheet.mergeCells('A2:I2');
  const titleCell = worksheet.getCell('A2');
  titleCell.value = 'ACTIVITY FUEL USAGE REPORT';
  titleCell.font = { size: 20, bold: true, color: { argb: 'FFFFFFFF' } };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF333333' } };
  titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  titleCell.border = {
    top: { style: 'medium', color: { argb: 'FF666666' } },
    bottom: { style: 'medium', color: { argb: 'FF666666' } }
  };

  worksheet.mergeCells('A3:I3');
  const periodCell = worksheet.getCell('A3');
  const startStr = period.start_date || 'N/A';
  const endStr = period.end_date || 'N/A';
  periodCell.value = startStr === endStr ? `Date: ${startStr}` : `Period: ${startStr} to ${endStr}`;
  periodCell.font = { size: 12, bold: true, color: { argb: 'FF333333' } };
  periodCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
  periodCell.alignment = { horizontal: 'center', vertical: 'middle' };
  periodCell.border = { bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } } };

  worksheet.mergeCells('A4:I4');
  const filterCell = worksheet.getCell('A4');
  filterCell.value = siteId ? `Site: ${siteId}` : `Cost Center: ${costCode || 'ALL COST CENTERS'}`;
  filterCell.font = { size: 12, bold: true, color: { argb: 'FF333333' } };
  filterCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
  filterCell.alignment = { horizontal: 'center', vertical: 'middle' };

  worksheet.mergeCells('A5:I5');
  const summaryCell = worksheet.getCell('A5');
  const totalUsage = (summary.total_fuel_usage || 0).toFixed(1);
  const totalFilled = (summary.total_fuel_filled || 0).toFixed(1);
  summaryCell.value = `Total Usage: ${totalUsage}L | Total Fills: ${totalFilled}L | Sessions: ${summary.total_sessions || 0}`;
  summaryCell.font = { size: 11, italic: true, color: { argb: 'FF666666' } };
  summaryCell.alignment = { horizontal: 'center', vertical: 'middle' };

  const emptyRow = worksheet.addRow([]);
  emptyRow.eachCell((cell) => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFAFAFA' } };
  });
}

function addActivityData(worksheet, siteReports, summary) {
  const headerRow = worksheet.addRow([
    'Vehicle',
    'Morning Usage',
    'Afternoon Usage',
    'Total Usage',
    'Tank 1',
    'Tank 2',
    'Fuel Fills',
    'Sessions',
    'Operating Hours'
  ]);

  headerRow.eachCell(cell => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF666666' } };
    cell.alignment = { horizontal: 'center', vertical: 'middle' };
    cell.border = {
      top: { style: 'thin' }, left: { style: 'thin' },
      bottom: { style: 'thin' }, right: { style: 'thin' }
    };
  });

  siteReports.forEach(site => {
    const morningUsage = (site.morning_to_afternoon_usage || 0).toFixed(2);
    const afternoonUsage = (site.afternoon_to_evening_usage || 0).toFixed(2);
    const totalUsage = (site.total_fuel_usage || 0).toFixed(2);
    const tank1 = (site.total_fuel_usage_probe_1 || 0).toFixed(2);
    const tank2 = (site.total_fuel_usage_probe_2 || 0).toFixed(2);
    const fills = (site.total_fuel_filled || 0).toFixed(2);
    const sessions = site.total_sessions || 0;
    const hours = (site.total_operating_hours || 0).toFixed(2);

    const row = worksheet.addRow([
      site.branch,
      `${morningUsage}L`,
      `${afternoonUsage}L`,
      `${totalUsage}L`,
      `${tank1}L`,
      `${tank2}L`,
      `${fills}L`,
      sessions,
      `${hours}h`
    ]);

    row.eachCell((cell, colNumber) => {
      cell.border = {
        top: { style: 'thin' }, left: { style: 'thin' },
        bottom: { style: 'thin' }, right: { style: 'thin' }
      };
      if ([4, 7].includes(colNumber)) {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF9C4' } };
      }
    });
  });

  worksheet.addRow([]);
  const totalRow = worksheet.addRow([
    'TOTALS',
    `${(summary.total_morning_to_afternoon_usage || 0).toFixed(2)}L`,
    `${(summary.total_afternoon_to_evening_usage || 0).toFixed(2)}L`,
    `${(summary.total_fuel_usage || 0).toFixed(2)}L`,
    `${(summary.total_fuel_usage_probe_1 || 0).toFixed(2)}L`,
    `${(summary.total_fuel_usage_probe_2 || 0).toFixed(2)}L`,
    `${(summary.total_fuel_filled || 0).toFixed(2)}L`,
    summary.total_sessions || 0,
    `${(summary.total_operating_hours || 0).toFixed(2)}h`
  ]);

  totalRow.eachCell((cell, colNumber) => {
    cell.font = { bold: true };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F0F0' } };
    cell.border = {
      top: { style: 'thick' }, left: { style: 'thin' },
      bottom: { style: 'thick' }, right: { style: 'thin' }
    };
    if ([4, 7].includes(colNumber)) {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFDE68A' } };
    }
  });
}

async function addFuelTheftData(worksheet, date, costCode, siteId) {
  let query = supabase
    .from('energy_rite_fuel_anomalies')
    .select('*')
    .eq('anomaly_type', 'FUEL_THEFT')
    .gte('anomaly_date', `${date}T00:00:00`)
    .lte('anomaly_date', `${date}T23:59:59`)
    .order('anomaly_date', { ascending: true });

  if (siteId) {
    query = query.eq('plate', siteId);
  }

  const { data: thefts, error } = await query;

  if (error || !thefts || thefts.length === 0) {
    return;
  }

  worksheet.addRow([]);
  worksheet.addRow([]);

  worksheet.mergeCells(`A${worksheet.lastRow.number + 1}:I${worksheet.lastRow.number + 1}`);
  const theftHeaderCell = worksheet.getCell(`A${worksheet.lastRow.number}`);
  theftHeaderCell.value = 'FUEL THEFT ALERTS';
  theftHeaderCell.font = { size: 14, bold: true, color: { argb: 'FFFFFFFF' } };
  theftHeaderCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFF9800' } };
  theftHeaderCell.alignment = { horizontal: 'center', vertical: 'middle' };

  const theftDataHeader = worksheet.addRow([
    'Vehicle', 'Time', 'Fuel Before', 'Fuel After', 'Amount Lost', 'Severity', 'Status', '', ''
  ]);

  theftDataHeader.eachCell((cell, colNumber) => {
    if (colNumber <= 7) {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFB74D' } };
      cell.alignment = { horizontal: 'center', vertical: 'middle' };
      cell.border = {
        top: { style: 'thin' }, left: { style: 'thin' },
        bottom: { style: 'thin' }, right: { style: 'thin' }
      };
    }
  });

  thefts.forEach(theft => {
    const time = new Date(theft.anomaly_date).toLocaleTimeString('en-ZA', { hour: '2-digit', minute: '2-digit' });
    const row = worksheet.addRow([
      theft.plate,
      time,
      `${theft.fuel_before.toFixed(1)}L`,
      `${theft.fuel_after.toFixed(1)}L`,
      `${Math.abs(theft.difference).toFixed(1)}L`,
      theft.severity,
      theft.status.toUpperCase(),
      '', ''
    ]);

    row.eachCell((cell, colNumber) => {
      if (colNumber <= 7) {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF9C4' } };
        cell.border = {
          top: { style: 'thin' }, left: { style: 'thin' },
          bottom: { style: 'thin' }, right: { style: 'thin' }
        };
        cell.alignment = { horizontal: 'center', vertical: 'middle' };

        if (colNumber === 6) {
          if (theft.severity === 'CRITICAL') {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFF5252' } };
            cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
          } else if (theft.severity === 'HIGH') {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFAB40' } };
            cell.font = { bold: true };
          }
        }
      }
    });
  });
}

class EnergyRiteActivityExcelReportController {

  async generateActivityExcelReport(req, res) {
    try {
      const { cost_code, date, site_id, start_date, end_date } = req.query;

      const params = new URLSearchParams();
      if (cost_code) params.append('cost_code', cost_code);
      if (site_id) params.append('site_id', site_id);
      if (date) {
        params.append('start_date', date);
        params.append('end_date', date);
      } else {
        if (start_date) params.append('start_date', start_date);
        if (end_date) params.append('end_date', end_date);
      }

      const response = await axios.get(`http://localhost:4000/api/energy-rite/reports/activity?${params.toString()}`);
      const activityData = response.data.data;

      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Activity Report');

      setupWorksheetLayout(worksheet);
      addReportHeader(worksheet, activityData.period, cost_code, site_id, activityData.summary);
      addActivityData(worksheet, activityData.site_reports || [], activityData.summary);

      const reportDate = date || activityData.period?.start_date || new Date().toISOString().split('T')[0];
      await addFuelTheftData(worksheet, reportDate, cost_code, site_id);

      const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
      const costCodeSuffix = cost_code ? `_${cost_code}` : (site_id ? `_${site_id}` : '_ALL');
      const fileName = `Waterford_Activity_Report${costCodeSuffix}_${reportDate}_${timestamp}.xlsx`;

      const buffer = await workbook.xlsx.writeBuffer();

      const bucketPath = `activity-reports/${new Date().getFullYear()}/${fileName}`;
      const { data: uploadData, error: uploadError } = await supabase.storage
        .from('energyrite-reports')
        .upload(bucketPath, buffer, {
          contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          upsert: true
        });

      if (uploadError) throw uploadError;

      const { data: { publicUrl } } = supabase.storage
        .from('energyrite-reports')
        .getPublicUrl(bucketPath);

      if (req.query.sendEmail === 'true') {
        try {
          const emailService = require('../../services/energy-rite/emailService');
          await emailService.sendReportEmail({
            reportType: 'activity',
            period: reportDate,
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
        } catch (emailError) {
          console.error('Email sending failed:', emailError.message);
        }
      }

      res.status(200).json({
        success: true,
        message: 'Activity Excel report generated successfully',
        data: {
          file_name: fileName,
          download_url: publicUrl,
          date: reportDate,
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
