const ExcelJS = require('exceljs');
const { supabase } = require('../../supabase-client');
const path = require('path');
const fs = require('fs');

class EnergyRiteExcelReportGenerator {
  
  /**
   * Generate comprehensive Excel report with expandable sections
   */
  async generateExcelReport(reportType = 'daily', targetDate = null, cost_code = null, site_id = null, month_type = 'previous', start_date = null, end_date = null) {
    try {
      console.log(`🔄 Generating ${reportType} Excel report...`);
      
      const reportDate = targetDate ? new Date(targetDate) : new Date();
      console.log(`📅 Target date: ${reportDate.toISOString().split('T')[0]}`);
      const { startDate, endDate, periodName } = this.calculateDateRange(reportType, reportDate, month_type, start_date, end_date);
      
      // Get operating sessions data
      const sessionsData = await this.getOperatingSessionsData(startDate, endDate, cost_code, site_id, reportType);
      
      // Create Excel workbook
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Fuel Report Summary');
      
      // Configure worksheet
      this.setupWorksheetLayout(worksheet);
      
      // Add header with logo space and title
      this.addReportHeader(worksheet, periodName, reportType);
      
      // Add summary data with expandable rows
      await this.addExpandableReportData(worksheet, sessionsData, reportType);
      
      // Generate unique filename with timestamp to avoid conflicts
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
      const fileName = `Waterford_${reportType}_Report_${periodName.replace(/\s+/g, '_')}_${timestamp}.xlsx`;
      const filePath = path.join(__dirname, '../../temp', fileName);
      
      // Ensure temp directory exists
      const tempDir = path.dirname(filePath);
      if (!fs.existsSync(tempDir)) {
        fs.mkdirSync(tempDir, { recursive: true });
      }
      
      // Save workbook to buffer for Supabase upload
      const buffer = await workbook.xlsx.writeBuffer();
      
      console.log(`💾 Saving Excel file: ${fileName}`);
      
      // Upload to Supabase storage bucket
      const bucketPath = `reports/${new Date().getFullYear()}/${fileName}`;
      const { data: uploadData, error: uploadError } = await supabase.storage
        .from('energyrite-reports')
        .upload(bucketPath, buffer, {
          contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          upsert: true
        });
      
      if (uploadError) {
        console.error('❌ Upload error:', uploadError);
        // Fallback to local storage
        await workbook.xlsx.writeFile(filePath);
        var publicUrl = `/temp/${fileName}`;
        var fileSize = fs.statSync(filePath).size;
      } else {
        console.log('✅ File uploaded to Supabase storage');
        
        // Get public URL
        const { data: { publicUrl: bucketUrl } } = supabase.storage
          .from('energyrite-reports')
          .getPublicUrl(bucketPath);
        
        var publicUrl = bucketUrl;
        var fileSize = buffer.length;
        
        // Also save locally as backup
        await workbook.xlsx.writeFile(filePath);
      }
      
      // Store report record in database with public URL
      const reportRecord = await this.storeReportRecord({
        report_type: reportType,
        period_name: periodName,
        start_date: startDate,
        end_date: endDate,
        cost_code: cost_code || 'ALL',
        final_cost_code: cost_code,
        file_name: fileName,
        download_url: publicUrl,
        file_size: fileSize,
        bucket_path: uploadData ? bucketPath : null,
        total_sites: sessionsData.sites.length,
        total_sessions: sessionsData.totalSessions,
        total_fills: sessionsData.totalFills || 0,
        total_operating_hours: sessionsData.totalOperatingHours
      });
      
      console.log(`📋 Report record stored with ID: ${reportRecord.id}`);
      
      console.log(`✅ Excel report generated successfully: ${fileName}`);
      
      return {
        success: true,
        report_id: reportRecord.id,
        file_name: fileName,
        download_url: publicUrl,
        public_url: publicUrl,
        bucket_path: uploadData ? bucketPath : null,
        file_size: fileSize,
        period: periodName,
        report_type: reportType,
        generated_at: new Date().toISOString(),
        stats: {
          total_sites: sessionsData.sites.length,
          total_sessions: sessionsData.totalSessions,
          total_fills: sessionsData.totalFills || 0,
          total_operating_hours: sessionsData.totalOperatingHours
        }
      };
      
    } catch (error) {
      console.error('❌ Error generating Excel report:', error);
      throw error;
    }
  }
  
  /**
   * Calculate date range based on report type
   */
  calculateDateRange(reportType, targetDate, month_type = 'previous', start_date = null, end_date = null) {
    // If both start_date and end_date provided, use them directly
    if (start_date && end_date) {
      const startDate = new Date(start_date);
      const endDate = new Date(end_date);
      startDate.setHours(0, 0, 0, 0);
      endDate.setHours(23, 59, 59, 999);
      const periodName = `${start_date}_to_${end_date}`;
      console.log(`📅 Using custom date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);
      return { startDate, endDate, periodName };
    }
    
    const endDate = new Date(targetDate);
    let startDate = new Date(targetDate);
    let periodName = '';
    
    endDate.setHours(23, 59, 59, 999);
    
    switch (reportType) {
      case 'daily':
        startDate.setHours(0, 0, 0, 0);
        // Use local date format to avoid timezone issues
        periodName = `${startDate.getFullYear()}-${String(startDate.getMonth() + 1).padStart(2, '0')}-${String(startDate.getDate()).padStart(2, '0')}`;
        break;
        
      case 'weekly':
        startDate.setDate(endDate.getDate() - 6);
        startDate.setHours(0, 0, 0, 0);
        periodName = `${startDate.toISOString().slice(0, 10)} to ${endDate.toISOString().slice(0, 10)}`;
        break;
        
      case 'monthly':
        if (month_type === 'current') {
          // Month-to-date: 1st of target month to target date
          startDate = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
          startDate.setHours(0, 0, 0, 0);
          // endDate already set to targetDate
          periodName = `${startDate.getFullYear()}-${(startDate.getMonth() + 1).toString().padStart(2, '0')}-MTD`;
        } else {
          // Previous month: 1st to last day of previous month
          startDate = new Date(endDate.getFullYear(), endDate.getMonth() - 1, 1);
          startDate.setHours(0, 0, 0, 0);
          endDate.setFullYear(startDate.getFullYear());
          endDate.setMonth(startDate.getMonth() + 1);
          endDate.setDate(0);
          endDate.setHours(23, 59, 59, 999);
          periodName = `${startDate.getFullYear()}-${(startDate.getMonth() + 1).toString().padStart(2, '0')}`;
        }
        break;
        
      default:
        throw new Error(`Invalid report type: ${reportType}`);
    }
    
    console.log(`📅 Date range for ${reportType} report: ${startDate.toISOString()} to ${endDate.toISOString()}`);
    return { startDate, endDate, periodName };
  }
  
  /**
   * Get operating sessions and fuel fills data from Supabase
   */
  async getOperatingSessionsData(startDate, endDate, cost_code = null, site_id = null, reportType = 'daily') {
    try {
      console.log(`📊 Getting operating sessions and fuel fills data for Excel report`);
      
      // Get all sites for the cost code from vehicle lookup with hierarchical access
      let allSites = [];
      if (site_id) {
        // Single site filtering
        console.log(`📊 Excel Report - Single site filter: ${site_id}`);
        const { data: siteData, error: siteError } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .eq('plate', site_id)
          .single();
        
        if (siteError || !siteData) {
          console.log(`⚠️ Site ${site_id} not found, falling back to cost_code filtering`);
          // Fallback to cost_code filtering if site not found
        } else {
          allSites = [siteData.plate];
          console.log(`📊 Excel Report - Single site: ${siteData.plate}`);
        }
      }
      
      if ((!site_id || allSites.length === 0) && cost_code) {
        console.log(`📊 Excel Report - Input cost code: ${cost_code}`);
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        console.log(`📊 Excel Report - Accessible cost codes: ${JSON.stringify(accessibleCostCodes)}`);
        
        const { data: vehicleLookup, error: lookupError } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .in('cost_code', accessibleCostCodes);
        
        if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);
        allSites = vehicleLookup.map(v => v.plate);
        console.log(`📊 Excel Report - Found ${allSites.length} sites for ${accessibleCostCodes.length} accessible cost codes`);
        console.log(`📊 Excel Report - Sites: ${JSON.stringify(allSites)}`);
      } else if (!site_id && !cost_code) {
        // For ALL cost codes (null), get ALL sites
        console.log(`📊 Excel Report - Getting ALL sites (no cost code filter)`);
        const { data: vehicleLookup, error: lookupError } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code');
        
        if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);
        allSites = vehicleLookup.map(v => v.plate);
        console.log(`📊 Excel Report - Found ${allSites.length} total sites across all cost codes`);
      }
      
      // For monthly reports, query by date range instead of single date
      const startDateStr = startDate.toISOString().split('T')[0];
      const endDateStr = endDate.toISOString().split('T')[0];
      
      console.log(`📅 Querying sessions from ${startDateStr} to ${endDateStr}`);
      
      // Get operating sessions (COMPLETED status)
      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_status', 'COMPLETED')
        .gte('session_date', startDateStr)
        .lte('session_date', endDateStr);
      
      // Get fuel fills (FUEL_FILL_COMPLETED status)
      let fillsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_status', 'FUEL_FILL_COMPLETED')
        .gte('session_date', startDateStr)
        .lte('session_date', endDateStr);
      
      // Filter by branch names if cost_code or site_id is provided
      if ((cost_code || site_id) && allSites.length > 0) {
        sessionsQuery = sessionsQuery.in('branch', allSites);
        fillsQuery = fillsQuery.in('branch', allSites);
      }
      
      const [sessionsResult, fillsResult] = await Promise.all([
        sessionsQuery.order('session_start_time', { ascending: false }),
        fillsQuery.order('session_start_time', { ascending: false })
      ]);
      
      if (sessionsResult.error) throw new Error(`Sessions error: ${sessionsResult.error.message}`);
      if (fillsResult.error) throw new Error(`Fills error: ${fillsResult.error.message}`);
      
      const sessions = sessionsResult.data;
      const fills = fillsResult.data;
      
      console.log(`📊 Found ${sessions.length} operating sessions and ${fills.length} fuel fills`);
      
      // Initialize all sites with zero values
      const siteGroups = {};
      
      // Add all sites from vehicle lookup with zero values
      allSites.forEach(siteName => {
        siteGroups[siteName] = {
          branch: siteName,
          company: 'KFC',
          cost_code: cost_code,
          sessions: [],
          fills: [],
          total_sessions: 0,
          total_operating_hours: 0,
          total_fuel_usage: 0,
          total_fuel_filled: 0,
          total_cost: 0,
          avg_efficiency: 0
        };
      });
      
      // Add operating session data
      sessions.forEach(session => {
        if (!siteGroups[session.branch]) {
          siteGroups[session.branch] = {
            branch: session.branch,
            company: session.company || 'KFC',
            cost_code: cost_code || session.cost_code,
            sessions: [],
            fills: [],
            total_sessions: 0,
            total_operating_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            total_cost: 0
          };
        }
        
        const site = siteGroups[session.branch];
        site.sessions.push({
          ...session,
          type: 'session',
          opening_percentage: parseFloat(session.opening_percentage || 0),
          closing_percentage: parseFloat(session.closing_percentage || 0)
        });
        site.total_sessions += 1;
        site.total_operating_hours += parseFloat(session.operating_hours || 0);
        site.total_fuel_usage += parseFloat(session.total_usage || 0);
        site.total_cost += parseFloat(session.cost_for_usage || 0);
      });
      
      // Add fuel fill data without combining. Each fill session stays separate.
      const fillsByVehicle = {};
      fills.forEach(fill => {
        if (!fillsByVehicle[fill.branch]) {
          fillsByVehicle[fill.branch] = [];
        }
        fillsByVehicle[fill.branch].push(fill);
      });
      
      // Add raw fills for each vehicle
      Object.keys(fillsByVehicle).forEach(branch => {
        
        if (!siteGroups[branch]) {
          siteGroups[branch] = {
            branch: branch,
            company: 'KFC',
            cost_code: cost_code,
            sessions: [],
            fills: [],
            total_sessions: 0,
            total_operating_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            total_cost: 0
          };
        }
        
        const site = siteGroups[branch];
        fillsByVehicle[branch].forEach(fill => {
          site.fills.push({
            ...fill,
            type: 'fill',
            opening_percentage: parseFloat(fill.opening_percentage || 0),
            closing_percentage: parseFloat(fill.closing_percentage || 0)
          });
          site.total_fuel_filled += parseFloat(fill.total_fill || 0);
        });
      });
      
      // Calculate averages
      Object.values(siteGroups).forEach(site => {
        if (site.total_operating_hours > 0) {
          site.avg_efficiency = site.total_fuel_usage / site.total_operating_hours;
        } else {
          site.avg_efficiency = 0;
        }
      });
      
      const sites = Object.values(siteGroups).sort((a, b) => a.branch.localeCompare(b.branch));
      const totalSessions = sessions.length;
      const totalFills = fills.length;
      const totalOperatingHours = sites.reduce((sum, site) => sum + site.total_operating_hours, 0);
      
      console.log(`📊 Retrieved ${sites.length} sites with ${totalSessions} sessions and ${totalFills} fills`);
      
      return {
        sites,
        totalSessions,
        totalFills,
        totalOperatingHours: totalOperatingHours.toFixed(2)
      };
      
    } catch (error) {
      console.error('❌ Error fetching operating sessions data:', error);
      throw error;
    }
  }
  
  /**
   * Setup worksheet layout and styling
   */
  setupWorksheetLayout(worksheet) {
    worksheet.columns = [
      { width: 5 },   // A: Expand button column
      { width: 22 },  // B: Plate
      { width: 20 },  // C: Date + Time
      { width: 28 },  // D: Operating Hours (increased from 16)
      { width: 16 },  // E: Opening Percentage
      { width: 14 },  // F: Opening Fuel (Total)
      { width: 14 },  // G: Opening Fuel T1
      { width: 14 },  // H: Opening Fuel T2
      { width: 16 },  // I: Closing Percentage
      { width: 14 },  // J: Closing Fuel (Total)
      { width: 14 },  // K: Closing Fuel T1
      { width: 14 },  // L: Closing Fuel T2
      { width: 12 },  // M: Usage
      { width: 12 },  // N: Fill (Total)
      { width: 12 },  // O: Fill (T1)
      { width: 12 },  // P: Fill (T2)
      { width: 14 },  // Q: Efficiency
      { width: 14 },  // R: Cost
      { width: 14 },  // S: Total Liters
      { width: 14 }   // T: Closing Liters
    ];
    
    worksheet.properties.defaultRowHeight = 20;
    worksheet.views = [{ state: 'frozen', ySplit: 5, showGridLines: false, showOutlineSymbols: true }];
  }
  
  /**
   * Add report header
   */
  addReportHeader(worksheet, periodName, reportType) {
    worksheet.mergeCells('A1:T1');
    const titleCell = worksheet.getCell('A1');
    titleCell.value = 'FUEL REPORT SUMMARY';
    titleCell.font = { size: 22, bold: true, color: { argb: 'FFFFFFFF' } };
    titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF333333' } };
    titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
    titleCell.border = {
      top: { style: 'medium', color: { argb: 'FF666666' } },
      bottom: { style: 'medium', color: { argb: 'FF666666' } }
    };
    
    worksheet.mergeCells('A2:T2');
    const periodCell = worksheet.getCell('A2');
    periodCell.value = `Report Period: ${periodName}`;
    periodCell.font = { size: 14, bold: true, color: { argb: 'FF333333' } };
    periodCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
    periodCell.alignment = { horizontal: 'center', vertical: 'middle' };
    periodCell.border = {
      bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } }
    };

    worksheet.mergeCells('A3:T3');
    const typeCell = worksheet.getCell('A3');
    typeCell.value = `Report Type: ${String(reportType || '').toUpperCase()}`;
    typeCell.font = { size: 12, bold: true, color: { argb: 'FF333333' } };
    typeCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFAFAFA' } };
    typeCell.alignment = { horizontal: 'center', vertical: 'middle' };

    worksheet.addRow([]);
  }
  
  /**
   * Add report data grouped by day with mixed sessions/fills sorted by time
   */
  async addExpandableReportData(worksheet, sessionsData, reportType) {
    const parseNumber = (value) => {
      const parsed = parseFloat(value);
      return Number.isFinite(parsed) ? parsed : 0;
    };

    const formatDate = (value) => {
      if (!value) return '';
      return new Date(value).toLocaleDateString('en-GB');
    };

    const formatTime = (value) => {
      if (!value) return '';
      return new Date(value).toLocaleTimeString('en-GB', { hour12: false });
    };

    const getDateKey = (event) => {
      if (event.session_date) return event.session_date;
      if (!event.session_start_time) return '';
      return new Date(event.session_start_time).toISOString().slice(0, 10);
    };

    const headerRow = worksheet.addRow([
      '+/-',
      'Plate / Event',
      'Date & Time',
      'Operating Hours',
      'Opening Percentage',
      'Opening Fuel (Total)',
      'Opening Fuel (T1)',
      'Opening Fuel (T2)',
      'Closing Percentage',
      'Closing Fuel (Total)',
      'Closing Fuel (T1)',
      'Closing Fuel (T2)',
      'Usage',
      'Fill (Total)',
      'Fill (T1)',
      'Fill (T2)',
      'Liters Used Per Hour',
      'Cost',
      'Total Liters',
      'Closing Liters'
    ]);

    headerRow.eachCell((cell) => {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF20344A' } };
      cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
      cell.border = {
        top: { style: 'thin', color: { argb: 'FF172635' } },
        left: { style: 'thin', color: { argb: 'FFCFD8E3' } },
        bottom: { style: 'thin', color: { argb: 'FF172635' } },
        right: { style: 'thin', color: { argb: 'FFCFD8E3' } }
      };
    });
    headerRow.height = 24;

    const overall = {
      sessions: 0,
      fills: 0,
      usageLiters: 0,
      fillLiters: 0,
      totalLiters: 0,
      totalCost: 0
    };

    for (const site of sessionsData.sites) {
      const mixedEvents = [
        ...(site.sessions || []).map((session) => ({ ...session, event_type: 'session' })),
        ...(site.fills || []).map((fill) => ({ ...fill, event_type: 'fill' }))
      ].sort((a, b) => new Date(a.session_start_time || 0) - new Date(b.session_start_time || 0));

      const hasActivity = mixedEvents.length > 0;

      const summaryRow = worksheet.addRow([
        hasActivity ? 'v' : '',
        site.branch,
        hasActivity ? `${mixedEvents.length} events` : 'No activity',
        this.formatDuration(site.total_operating_hours || 0),
        '', '', '', '', '', '', '', '',
        (site.total_fuel_usage || 0).toFixed(2),
        (site.total_fuel_filled || 0).toFixed(2),
        '',
        '',
        (site.avg_efficiency || 0).toFixed(2),
        site.total_cost > 0 && site.total_fuel_usage > 0 ? `@R${(site.total_cost / site.total_fuel_usage).toFixed(2)} = R${(site.total_cost || 0).toFixed(2)}` : 'R0.00',
        (parseNumber(site.total_fuel_usage) + parseNumber(site.total_fuel_filled)).toFixed(2),
        ''
      ]);

      summaryRow.eachCell((cell, colNumber) => {
        if (colNumber === 1) {
          cell.font = { bold: true, color: { argb: 'FF20344A' }, size: 11 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF2F5F8' } };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        } else {
          cell.font = { bold: hasActivity, color: { argb: 'FF1F2933' }, size: 9 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: hasActivity ? 'FFE8EEF4' : 'FFF6F8FA' } };
          cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
        }
        cell.border = {
          top: { style: 'thin', color: { argb: 'FFD7DFE8' } },
          left: { style: 'thin', color: { argb: 'FFD7DFE8' } },
          bottom: { style: 'thin', color: { argb: 'FFD7DFE8' } },
          right: { style: 'thin', color: { argb: 'FFD7DFE8' } }
        };
      });

      if (!hasActivity) {
        continue;
      }

      const eventsByDay = new Map();
      for (const event of mixedEvents) {
        const dayKey = getDateKey(event);
        if (!eventsByDay.has(dayKey)) eventsByDay.set(dayKey, []);
        eventsByDay.get(dayKey).push(event);
      }

      const sortedDayKeys = [...eventsByDay.keys()].sort((a, b) => a.localeCompare(b));

      for (const dayKey of sortedDayKeys) {
        const dayEvents = eventsByDay.get(dayKey).sort(
          (a, b) => new Date(a.session_start_time || 0) - new Date(b.session_start_time || 0)
        );

        const dayHeaderRow = worksheet.addRow([
          '',
          `Date: ${dayKey}`,
          `${dayEvents.length} events`,
          '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''
        ]);

        dayHeaderRow.eachCell((cell, colNumber) => {
          cell.font = { bold: true, size: 9, color: { argb: 'FF0F2A43' } };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEAF1F7' } };
          cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
          cell.border = {
            top: { style: 'thin', color: { argb: 'FFD7DFE8' } },
            left: { style: 'thin', color: { argb: 'FFD7DFE8' } },
            bottom: { style: 'thin', color: { argb: 'FFD7DFE8' } },
            right: { style: 'thin', color: { argb: 'FFD7DFE8' } }
          };
        });

        let dayUsageLiters = 0;
        let dayFillLiters = 0;
        let dayTotalLiters = 0;
        let dayClosingLiters = 0;

        dayEvents.forEach((event, index) => {
          const isFill = event.event_type === 'fill';
          const startIso = event.session_start_time;
          const endIso = event.session_end_time;
          const startTime = formatTime(startIso);
          const endTime = endIso ? formatTime(endIso) : 'Ongoing';
          const timeRange = `From: ${startTime}    To: ${endTime}`;

          let operatingHours = parseNumber(event.operating_hours);
          if (!operatingHours && startIso && endIso) {
            const deltaMs = new Date(endIso).getTime() - new Date(startIso).getTime();
            if (deltaMs > 0) operatingHours = deltaMs / (1000 * 60 * 60);
          }

          const openingFuel = parseNumber(event.opening_fuel);
          const closingFuel = parseNumber(event.closing_fuel);
          const openingFuelProbe1 = parseNumber(event.opening_fuel_probe_1);
          const openingFuelProbe2 = parseNumber(event.opening_fuel_probe_2);
          const closingFuelProbe1 = parseNumber(event.closing_fuel_probe_1);
          const closingFuelProbe2 = parseNumber(event.closing_fuel_probe_2);
          const usageLiters = isFill ? 0 : parseNumber(event.total_usage);
          const fillLiters = isFill
            ? Math.max(0, parseNumber(event.total_fill) || (closingFuel - openingFuel))
            : parseNumber(event.total_fill);
          const litersPerHour = operatingHours > 0 ? usageLiters / operatingHours : 0;
          const costPerLiter = parseNumber(event.cost_per_liter);
          const costForUsage = parseNumber(event.cost_for_usage);
          const eventTotalLiters = isFill ? fillLiters : usageLiters;

          dayUsageLiters += usageLiters;
          dayFillLiters += fillLiters;
          dayTotalLiters += eventTotalLiters;
          dayClosingLiters = closingFuel;

          overall.usageLiters += usageLiters;
          overall.fillLiters += fillLiters;
          overall.totalLiters += eventTotalLiters;
          overall.totalCost += costForUsage;
          if (isFill) overall.fills += 1;
          else overall.sessions += 1;

          const rowLabel = isFill ? `  + Fill ${index + 1}` : `  - Session ${index + 1}`;

          const eventRow = worksheet.addRow([
            '',
            rowLabel,
            `${formatDate(startIso)} ${startTime}`,
            `${this.formatDuration(operatingHours)}\n${timeRange}`,
            `${parseNumber(event.opening_percentage).toFixed(0)}%`,
            `${openingFuel.toFixed(1)}L`,
            `${openingFuelProbe1.toFixed(1)}L`,
            `${openingFuelProbe2.toFixed(1)}L`,
            `${parseNumber(event.closing_percentage).toFixed(0)}%`,
            `${closingFuel.toFixed(1)}L`,
            `${closingFuelProbe1.toFixed(1)}L`,
            `${closingFuelProbe2.toFixed(1)}L`,
            `${usageLiters.toFixed(2)}L`,
            `${fillLiters.toFixed(2)}L`,
            isFill ? `${Math.max(0, closingFuelProbe1 - openingFuelProbe1).toFixed(2)}L` : '',
            isFill ? `${Math.max(0, closingFuelProbe2 - openingFuelProbe2).toFixed(2)}L` : '',
            isFill ? 'N/A' : `${litersPerHour.toFixed(2)}L/h`,
            isFill ? 'R0.00' : `@R${costPerLiter.toFixed(2)} = R${costForUsage.toFixed(2)}`,
            `${eventTotalLiters.toFixed(2)}L`,
            `${closingFuel.toFixed(2)}L`
          ]);

          eventRow.eachCell((cell, colNumber) => {
            if (colNumber === 1) {
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: isFill ? 'FFF8FBF7' : 'FFF7F9FB' } };
            } else if (colNumber === 4) {
              cell.font = { italic: true, bold: true, size: 9, color: { argb: 'FF1F2933' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: isFill ? 'FFE8F4EA' : 'FFE9F1F8' } };
              cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
            } else {
              cell.font = { size: 9, color: { argb: 'FF1F2933' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: isFill ? 'FFF7FBF7' : 'FFF7FAFD' } };
              cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
            }
            cell.border = {
              left: { style: 'thin', color: { argb: isFill ? 'FFDFEADF' : 'FFE1E7EE' } },
              right: { style: 'thin', color: { argb: isFill ? 'FFDFEADF' : 'FFE1E7EE' } },
              bottom: { style: 'thin', color: { argb: isFill ? 'FFDFEADF' : 'FFE1E7EE' } }
            };
          });
          eventRow.height = 24;
        });

        const dayTotalRow = worksheet.addRow([
          '',
          `Daily Total (${dayKey})`,
          `${dayEvents.length} events`,
          '', '', '', '', '', '', '', '', '',
          `${dayUsageLiters.toFixed(2)}L`,
          `${dayFillLiters.toFixed(2)}L`,
          '',
          '',
          '',
          '',
          `${dayTotalLiters.toFixed(2)}L`,
          `${dayClosingLiters.toFixed(2)}L`
        ]);

        dayTotalRow.eachCell((cell, colNumber) => {
          cell.font = { bold: true, size: 9, color: { argb: 'FF1F2933' } };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEFF5EC' } };
          cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
          cell.border = {
            top: { style: 'thin', color: { argb: 'FFD4E2CF' } },
            left: { style: 'thin', color: { argb: 'FFD4E2CF' } },
            bottom: { style: 'thin', color: { argb: 'FFD4E2CF' } },
            right: { style: 'thin', color: { argb: 'FFD4E2CF' } }
          };
        });
      }
    }

    worksheet.addRow([]);

    const totalRow = worksheet.addRow([
      '',
      `${reportType.toUpperCase()} TOTALS`,
      `${overall.sessions + overall.fills} events`,
      this.formatDuration(sessionsData.totalOperatingHours),
      '', '', '', '', '', '', '', '',
      `${overall.usageLiters.toFixed(2)}L`,
      `${overall.fillLiters.toFixed(2)}L`,
      '',
      '',
      '',
      `R${overall.totalCost.toFixed(2)}`,
      `${overall.totalLiters.toFixed(2)}L`,
      ''
    ]);

    totalRow.eachCell((cell, colNumber) => {
      cell.font = { bold: true, size: 10, color: { argb: 'FF1F2933' } };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE6EDF5' } };
      cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
      cell.border = {
        top: { style: 'thin', color: { argb: 'FFD0D9E3' } },
        left: { style: 'thin', color: { argb: 'FFD0D9E3' } },
        bottom: { style: 'thin', color: { argb: 'FFD0D9E3' } },
        right: { style: 'thin', color: { argb: 'FFD0D9E3' } }
      };
    });
    totalRow.height = 22;

    worksheet.addRow([]);

    const breakdownRow = worksheet.addRow([
      '',
      'BREAKDOWN:',
      `${overall.sessions} sessions, ${overall.fills} fills (mixed timeline, grouped by day)`,
      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''
    ]);

    breakdownRow.eachCell((cell, colNumber) => {
      if (colNumber <= 3) {
        cell.font = { italic: true, size: 10, color: { argb: 'FF666666' } };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8F8F8' } };
        cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
        cell.border = {
          top: { style: 'thin', color: { argb: 'FFCCCCCC' } },
          left: { style: 'thin', color: { argb: 'FFCCCCCC' } },
          bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } },
          right: { style: 'thin', color: { argb: 'FFCCCCCC' } }
        };
      }
    });
  }

  /**
   * Format duration from decimal hours to readable format
   */
  formatDuration(decimalHours) {
    if (!decimalHours || decimalHours === 0) return '0 hours 0 minutes 0 seconds';
    
    const hoursNum = parseFloat(decimalHours);
    if (isNaN(hoursNum) || hoursNum === 0) return '0 hours 0 minutes 0 seconds';
    
    const hours = Math.floor(hoursNum);
    const remainingMinutes = (hoursNum - hours) * 60;
    const minutes = Math.floor(remainingMinutes);
    const seconds = Math.round((remainingMinutes - minutes) * 60);
    
    return `${hours} hours ${minutes} minutes ${seconds} seconds`;
  }
  
  /**
   * Store report record in database with full metadata
   */
  async storeReportRecord(reportData) {
    try {
      console.log(`💾 Storing report: ${reportData.report_type} for ${reportData.cost_code}`);
      console.log(`📊 Excel Report - Final cost code: ${reportData.final_cost_code}`);
      
      const reportDate = reportData.start_date.toISOString().split('T')[0];
      
      // Always create new report record with unique filename
      const reportRecord = {
        cost_code: reportData.cost_code,
        report_type: reportData.report_type,
        report_url: reportData.download_url,
        report_date: reportDate,
        file_name: reportData.file_name,
        file_size: reportData.file_size || 0,
        bucket_path: reportData.bucket_path,
        period_name: reportData.period_name,
        total_sites: reportData.total_sites || 0,
        total_sessions: reportData.total_sessions || 0,
        total_operating_hours: parseFloat(reportData.total_operating_hours || 0),
        status: 'generated',
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      };
      
      const { data, error } = await supabase
        .from('energy_rite_generated_reports')
        .insert(reportRecord)
        .select();
      
      console.log(`➕ Created new report`);
      
      if (error) {
        console.error('Database error details:', error);
        // If there's still a constraint error, return a mock ID to continue
        if (error.message.includes('duplicate key') || error.message.includes('unique constraint')) {
          console.log('⚠️ Constraint violation, continuing without database record...');
          return { id: Date.now() }; // Use timestamp as mock ID
        }
        throw new Error(`Database error: ${error.message}`);
      }
      
      console.log(`✅ Report stored successfully with ID: ${data[0].id}`);
      return { id: data[0].id };
      
    } catch (error) {
      console.error('❌ Error storing report record:', error);
      throw error;
    }
  }
  
  /**
   * Generate daily report
   */
  async generateDailyReport(targetDate = null, cost_code = null, site_id = null) {
    return this.generateExcelReport('daily', targetDate, cost_code, site_id, 'previous');
  }
  
  /**
   * Generate weekly report (last 7 days)
   */
  async generateWeeklyReport(targetDate = null, cost_code = null, site_id = null) {
    return this.generateExcelReport('weekly', targetDate, cost_code, site_id, 'previous');
  }
  
  /**
   * Generate monthly report
   */
  async generateMonthlyReport(targetDate = null, cost_code = null, site_id = null, month_type = 'previous') {
    return this.generateExcelReport('monthly', targetDate, cost_code, site_id, month_type);
  }
}

module.exports = new EnergyRiteExcelReportGenerator();



