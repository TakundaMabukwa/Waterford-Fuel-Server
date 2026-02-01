const ExcelJS = require('exceljs');
const { supabase } = require('../../supabase-client');
const { combineFuelFills } = require('../../helpers/fuel-fill-combiner');
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
      const fileName = `Energy_Rite_${reportType}_Report_${periodName.replace(/\s+/g, '_')}_${timestamp}.xlsx`;
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
      
      // Add fuel fill data and combine consecutive fills
      const fillsByVehicle = {};
      fills.forEach(fill => {
        if (!fillsByVehicle[fill.branch]) {
          fillsByVehicle[fill.branch] = [];
        }
        fillsByVehicle[fill.branch].push(fill);
      });
      
      // Combine fills for each vehicle
      Object.keys(fillsByVehicle).forEach(branch => {
        const combinedFills = combineFuelFills(fillsByVehicle[branch], 2);
        
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
        combinedFills.forEach(fill => {
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
      { width: 22 },  // B: Site
      { width: 14 },  // C: Date  
      { width: 28 },  // D: Operating Hours (increased from 16)
      { width: 16 },  // E: Opening Percentage
      { width: 14 },  // F: Opening Fuel
      { width: 16 },  // G: Closing Percentage
      { width: 14 },  // H: Closing Fuel
      { width: 12 },  // I: Usage
      { width: 12 },  // J: Fill
      { width: 14 },  // K: Efficiency
      { width: 14 }   // L: Cost
    ];
    
    worksheet.properties.defaultRowHeight = 22;
  }
  
  /**
   * Add report header with logo and title
   */
  addReportHeader(worksheet, periodName, reportType) {
    const workbook = worksheet.workbook;
    
    // Logo space (rows 1-6)
    worksheet.mergeCells('A1:L6');
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
    worksheet.mergeCells('A7:L7');
    const titleCell = worksheet.getCell('A7');
    titleCell.value = 'FUEL REPORT SUMMARY';
    titleCell.font = { size: 22, bold: true, color: { argb: 'FFFFFFFF' } };
    titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF333333' } };
    titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
    titleCell.border = {
      top: { style: 'medium', color: { argb: 'FF666666' } },
      bottom: { style: 'medium', color: { argb: 'FF666666' } }
    };
    
    // Period with accent styling
    worksheet.mergeCells('A8:L8');
    const periodCell = worksheet.getCell('A8');
    periodCell.value = `Report Period: ${periodName}`;
    periodCell.font = { size: 14, bold: true, color: { argb: 'FF333333' } };
    periodCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
    periodCell.alignment = { horizontal: 'center', vertical: 'middle' };
    periodCell.border = {
      bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } }
    };
    
    // Empty row with subtle background
    const emptyRow = worksheet.addRow([]);
    emptyRow.eachCell((cell) => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFAFAFA' } };
    });
  }
  
  /**
   * Add expandable report data with sessions and fills separated
   */
  async addExpandableReportData(worksheet, sessionsData, reportType) {
    // Header row
    const headerRow = worksheet.addRow([
      '+/-', // Expand button column
      'Site',
      'Date',
      'Operating Hours',
      'Opening Percentage',
      'Opening Fuel',
      'Closing Percentage', 
      'Closing Fuel',
      'Usage',
      'Fill',
      'Liters Used Per Hour',
      'Cost'
    ]);
    
    // Style header row with professional gradient effect
    headerRow.eachCell((cell, colNumber) => {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2A2A2A' } };
      cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
      cell.border = {
        top: { style: 'medium', color: { argb: 'FF1A1A1A' } },
        left: { style: 'thin', color: { argb: 'FF666666' } },
        bottom: { style: 'medium', color: { argb: 'FF1A1A1A' } },
        right: { style: 'thin', color: { argb: 'FF666666' } }
      };
    });
    headerRow.height = 32;
    
    // Add data for each site with sessions and fills breakdown
    for (const site of sessionsData.sites) {
      const hasActivity = site.sessions.length > 0 || site.fills.length > 0;
      const totalActivities = site.sessions.length + site.fills.length;
      
      // Main summary row
      const summaryRow = worksheet.addRow([
        hasActivity ? '▼' : '',
        site.branch,
        `${site.total_sessions} sessions`,
        this.formatDuration(site.total_operating_hours || 0),
        '', '', '', '',
        (site.total_fuel_usage || 0).toFixed(2),
        (site.total_fuel_filled || 0).toFixed(2),
        (site.avg_efficiency || 0).toFixed(2),
        site.total_cost > 0 && site.total_fuel_usage > 0 ? `@R${(site.total_cost / site.total_fuel_usage).toFixed(2)} = R${(site.total_cost || 0).toFixed(2)}` : 'R0.00'
      ]);
      
      // Style main row with alternating professional colors
      summaryRow.eachCell((cell, colNumber) => {
        if (colNumber === 1) {
          // Toggle button with accent
          cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 12 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        } else {
          cell.font = { bold: hasActivity, color: { argb: 'FFFFFFFF' }, size: 10 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: hasActivity ? 'FF404040' : 'FF333333' } };
          cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
        }
        cell.border = {
          top: { style: 'thin', color: { argb: 'FF666666' } },
          left: { style: 'thin', color: { argb: 'FF666666' } },
          bottom: { style: 'thin', color: { argb: 'FF666666' } },
          right: { style: 'thin', color: { argb: 'FF666666' } }
        };
      });
      if (hasActivity) summaryRow.height = 26;
      
      // Add individual session rows (sorted by start time)
      if (site.sessions.length > 0) {
        // Sort sessions by start time (earliest first)
        const sortedSessions = [...site.sessions].sort((a, b) => 
          new Date(a.session_start_time) - new Date(b.session_start_time)
        );
        
        for (const session of sortedSessions) {
          const startTime = new Date(session.session_start_time).toLocaleTimeString('en-GB', { hour12: false });
          const endTime = session.session_end_time ? new Date(session.session_end_time).toLocaleTimeString('en-GB', { hour12: false }) : 'Ongoing';
          const timeRange = `From: ${startTime}    To: ${endTime}`;
          const operatingHoursWithTime = `${this.formatDuration(session.operating_hours || 0)}\n${timeRange}`;
          
          const sessionRow = worksheet.addRow([
            '',
            `  └ Session ${sortedSessions.indexOf(session) + 1}`,
            new Date(session.session_start_time).toLocaleDateString(),
            operatingHoursWithTime,
            `${(session.opening_percentage || 0).toFixed(0)}%`,
            `${(session.opening_fuel || 0).toFixed(1)}L`,
            `${(session.closing_percentage || 0).toFixed(0)}%`,
            `${(session.closing_fuel || 0).toFixed(1)}L`,
            `${(session.total_usage || 0).toFixed(2)}L`,
            `${(session.total_fill || 0).toFixed(2)}L`,
            `${(session.liter_usage_per_hour || (session.operating_hours > 0 ? session.total_usage / session.operating_hours : 0)).toFixed(2)}L/h`,
            `@R${(session.cost_per_liter || 0).toFixed(2)} = R${(session.cost_for_usage || 0).toFixed(2)}`
          ]);
          
          // Style session row (blue tint for sessions)
          sessionRow.eachCell((cell, colNumber) => {
            if (colNumber === 1) {
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
            } else if (colNumber === 4) {
              // Time column - bold and italic
              cell.font = { italic: true, bold: true, size: 11, color: { argb: 'FFFFFFFF' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4A4A6A' } };
              cell.alignment = { 
                horizontal: 'center', 
                vertical: 'middle',
                wrapText: true
              };
            } else {
              cell.font = { size: 10, bold: true, color: { argb: 'FFFFFFFF' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4A4A6A' } };
              cell.alignment = { 
                horizontal: colNumber === 2 ? 'left' : 'center', 
                vertical: 'middle'
              };
            }
            cell.border = {
              left: { style: 'thin', color: { argb: 'FF666666' } },
              right: { style: 'thin', color: { argb: 'FF666666' } },
              bottom: { style: 'hair', color: { argb: 'FF888888' } }
            };
          });
          sessionRow.height = 35;
        }
      }
      
      // Add individual fuel fill rows (now combined, sorted by start time)
      if (site.fills.length > 0) {
        // Sort fills by start time (earliest first)
        const sortedFills = [...site.fills].sort((a, b) => 
          new Date(a.session_start_time) - new Date(b.session_start_time)
        );
        
        for (const fill of sortedFills) {
          const startTime = new Date(fill.session_start_time).toLocaleTimeString('en-GB', { hour12: false });
          const endTime = fill.session_end_time ? new Date(fill.session_end_time).toLocaleTimeString('en-GB', { hour12: false }) : 'Ongoing';
          const timeRange = `From: ${startTime}    To: ${endTime}`;
          const fillDurationWithTime = `${fill.duration_formatted || this.formatDuration(fill.operating_hours || 0)}\n${timeRange}`;
          const fillLabel = fill.is_combined ? `  └ Fill ${sortedFills.indexOf(fill) + 1} (${fill.fill_count} combined)` : `  └ Fill ${sortedFills.indexOf(fill) + 1}`;
          
          const fillRow = worksheet.addRow([
            '',
            fillLabel,
            new Date(fill.session_start_time).toLocaleDateString(),
            fillDurationWithTime,
            `${(fill.opening_percentage || 0).toFixed(0)}%`,
            `${(fill.opening_fuel || 0).toFixed(1)}L`,
            `${(fill.closing_percentage || 0).toFixed(0)}%`,
            `${(fill.closing_fuel || 0).toFixed(1)}L`,
            '0.00L',
            `${(fill.total_fill || 0).toFixed(2)}L`,
            'N/A',
            'R0.00'
          ]);
          
          // Style fill row (green tint for fills)
          fillRow.eachCell((cell, colNumber) => {
            if (colNumber === 1) {
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
            } else if (colNumber === 4) {
              // Time column - bold and italic
              cell.font = { italic: true, bold: true, size: 11, color: { argb: 'FFFFFFFF' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4A6A4A' } };
              cell.alignment = { 
                horizontal: 'center', 
                vertical: 'middle',
                wrapText: true
              };
            } else {
              cell.font = { size: 10, bold: true, color: { argb: 'FFFFFFFF' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4A6A4A' } };
              cell.alignment = { 
                horizontal: colNumber === 2 ? 'left' : 'center', 
                vertical: 'middle'
              };
            }
            cell.border = {
              left: { style: 'thin', color: { argb: 'FF666666' } },
              right: { style: 'thin', color: { argb: 'FF666666' } },
              bottom: { style: 'hair', color: { argb: 'FF888888' } }
            };
          });
          fillRow.height = 35;
        }
      }
    }
    
    // Apply grouping after all rows are added
    const groupRanges = [];
    let currentRow = headerRow.number + 1;
    
    for (const site of sessionsData.sites) {
      const hasSessions = site.sessions.length > 0;
      currentRow++; // Skip summary row
      
      if (hasSessions) {
        const startRow = currentRow;
        const endRow = currentRow + site.sessions.length - 1;
        groupRanges.push({ start: startRow, end: endRow });
        currentRow = endRow + 1;
      }
    }
    
    // Apply outline levels
    groupRanges.forEach(range => {
      for (let i = range.start; i <= range.end; i++) {
        worksheet.getRow(i).outlineLevel = 1;
      }
    });
    
    // Enable outline view
    worksheet.views = [{
      state: 'normal',
      showOutlineSymbols: true
    }];
    
    
    // Add totals summary
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      '',
      `${reportType.toUpperCase()} TOTALS`,
      `${sessionsData.totalSessions} sessions`,
      this.formatDuration(sessionsData.totalOperatingHours),
      '', '', '', '',
      sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_usage, 0).toFixed(2),
      sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_filled, 0).toFixed(2),
      '',
      (() => {
        const totalCost = sessionsData.sites.reduce((sum, site) => sum + site.total_cost, 0);
        const totalUsage = sessionsData.sites.reduce((sum, site) => sum + site.total_fuel_usage, 0);
        return totalCost > 0 ? `@R${(totalCost / totalUsage).toFixed(2)} = R${totalCost.toFixed(2)}` : 'R0.00';
      })()
    ]);
    
    // Add breakdown summary
    worksheet.addRow([]);
    const breakdownRow = worksheet.addRow([
      '',
      'BREAKDOWN:',
      `${sessionsData.totalSessions} operating sessions, ${sessionsData.totalFills || 0} fuel fills`,
      '', '', '', '', '', '', '', '', ''
    ]);
    
    // Style breakdown row
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
    
    // Style totals row with emphasis
    totalRow.eachCell((cell, colNumber) => {
      cell.font = { bold: true, size: 12, color: { argb: 'FFFFFFFF' } };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
      cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
      cell.border = {
        top: { style: 'thick', color: { argb: 'FF333333' } },
        left: { style: 'thin', color: { argb: 'FF666666' } },
        bottom: { style: 'thick', color: { argb: 'FF333333' } },
        right: { style: 'thin', color: { argb: 'FF666666' } }
      };
    });
    totalRow.height = 30;
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