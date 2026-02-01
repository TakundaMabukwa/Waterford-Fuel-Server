const { supabase } = require('../../supabase-client');
const fs = require('fs').promises;
const path = require('path');

class EnergyRiteReportStorageController {
  
  // Generate and store daily report
  async generateAndStoreDailyReport(req, res) {
    try {
      const { cost_code, date } = req.body;
      const reportDate = date || new Date().toISOString().split('T')[0];
      
      // Generate report data
      const reportData = await this.getDailyReportData(cost_code, reportDate);
      
      // Create report file
      const fileName = `daily_report_${cost_code}_${reportDate}.json`;
      const filePath = path.join(__dirname, '../../reports', fileName);
      
      // Ensure reports directory exists
      await fs.mkdir(path.dirname(filePath), { recursive: true });
      
      // Write report to file
      await fs.writeFile(filePath, JSON.stringify(reportData, null, 2));
      
      // Get file size
      const stats = await fs.stat(filePath);
      
      // Store report metadata in database
      const reportUrl = `/api/energy-rite/reports/files/${fileName}`;
      
      const { data, error } = await supabase
        .from('energy_rite_generated_reports')
        .upsert({
          cost_code,
          report_type: 'daily',
          report_url: reportUrl,
          report_date: reportDate,
          file_size: stats.size,
          status: 'generated'
        }, {
          onConflict: 'cost_code,report_type,report_date'
        })
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.json({
        success: true,
        message: 'Daily report generated and stored',
        data: {
          report_id: data[0].id,
          cost_code,
          report_type: 'daily',
          report_date: reportDate,
          report_url: reportUrl,
          file_size: stats.size
        }
      });
      
    } catch (error) {
      console.error('Error generating daily report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate daily report',
        message: error.message
      });
    }
  }
  
  // Generate and store weekly report
  async generateAndStoreWeeklyReport(req, res) {
    try {
      const { cost_code, week, year } = req.body;
      const targetYear = year || new Date().getFullYear();
      const targetWeek = week || this.getWeekNumber(new Date());
      const reportDate = this.getDateFromWeek(targetWeek, targetYear).toISOString().split('T')[0];
      
      // Generate report data
      const reportData = await this.getWeeklyReportData(cost_code, targetWeek, targetYear);
      
      // Create report file
      const fileName = `weekly_report_${cost_code}_week${targetWeek}_${targetYear}.json`;
      const filePath = path.join(__dirname, '../../reports', fileName);
      
      await fs.mkdir(path.dirname(filePath), { recursive: true });
      await fs.writeFile(filePath, JSON.stringify(reportData, null, 2));
      
      const stats = await fs.stat(filePath);
      const reportUrl = `/api/energy-rite/reports/files/${fileName}`;
      
      const { data, error } = await supabase
        .from('energy_rite_generated_reports')
        .upsert({
          cost_code,
          report_type: 'weekly',
          report_url: reportUrl,
          report_date: reportDate,
          file_size: stats.size,
          status: 'generated'
        }, {
          onConflict: 'cost_code,report_type,report_date'
        })
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.json({
        success: true,
        message: 'Weekly report generated and stored',
        data: {
          report_id: data[0].id,
          cost_code,
          report_type: 'weekly',
          week: targetWeek,
          year: targetYear,
          report_date: reportDate,
          report_url: reportUrl,
          file_size: stats.size
        }
      });
      
    } catch (error) {
      console.error('Error generating weekly report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate weekly report',
        message: error.message
      });
    }
  }
  
  // Generate and store monthly report
  async generateAndStoreMonthlyReport(req, res) {
    try {
      const { cost_code, month, year } = req.body;
      const targetMonth = month || new Date().getMonth() + 1;
      const targetYear = year || new Date().getFullYear();
      const reportDate = new Date(targetYear, targetMonth - 1, 1).toISOString().split('T')[0];
      
      // Generate report data
      const reportData = await this.getMonthlyReportData(cost_code, targetMonth, targetYear);
      
      // Create report file
      const fileName = `monthly_report_${cost_code}_${targetMonth}_${targetYear}.json`;
      const filePath = path.join(__dirname, '../../reports', fileName);
      
      await fs.mkdir(path.dirname(filePath), { recursive: true });
      await fs.writeFile(filePath, JSON.stringify(reportData, null, 2));
      
      const stats = await fs.stat(filePath);
      const reportUrl = `/api/energy-rite/reports/files/${fileName}`;
      
      const { data, error } = await supabase
        .from('energy_rite_generated_reports')
        .upsert({
          cost_code,
          report_type: 'monthly',
          report_url: reportUrl,
          report_date: reportDate,
          file_size: stats.size,
          status: 'generated'
        }, {
          onConflict: 'cost_code,report_type,report_date'
        })
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.json({
        success: true,
        message: 'Monthly report generated and stored',
        data: {
          report_id: data[0].id,
          cost_code,
          report_type: 'monthly',
          month: targetMonth,
          year: targetYear,
          report_date: reportDate,
          report_url: reportUrl,
          file_size: stats.size
        }
      });
      
    } catch (error) {
      console.error('Error generating monthly report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate monthly report',
        message: error.message
      });
    }
  }
  
  // Get stored reports list
  async getStoredReports(req, res) {
    try {
      const { cost_code, report_type, limit = 50 } = req.query;
      
      let query = supabase
        .from('energy_rite_generated_reports')
        .select('*')
        .order('generated_at', { ascending: false })
        .limit(limit);
      
      if (cost_code) {
        query = query.eq('cost_code', cost_code);
      }
      
      if (report_type) {
        query = query.eq('report_type', report_type);
      }
      
      const { data, error } = await query;
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.json({
        success: true,
        data: data
      });
      
    } catch (error) {
      console.error('Error getting stored reports:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to get stored reports',
        message: error.message
      });
    }
  }
  
  // Serve report file
  async serveReportFile(req, res) {
    try {
      const { fileName } = req.params;
      const filePath = path.join(__dirname, '../../reports', fileName);
      
      // Check if file exists
      await fs.access(filePath);
      
      // Read and serve file
      const fileContent = await fs.readFile(filePath, 'utf8');
      const reportData = JSON.parse(fileContent);
      
      res.json({
        success: true,
        fileName,
        data: reportData
      });
      
    } catch (error) {
      console.error('Error serving report file:', error);
      res.status(404).json({
        success: false,
        error: 'Report file not found',
        message: error.message
      });
    }
  }
  
  // Helper methods to get report data (simplified versions)
  async getDailyReportData(cost_code, date) {
    let query = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_date', date);
    
    if (cost_code) {
      query = query.eq('cost_code', cost_code);
    }
    
    const { data: sessions, error } = await query;
    if (error) throw new Error(`Database error: ${error.message}`);
    
    // If no sessions found, get plates from fuel table
    if (sessions.length === 0) {
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate')
        .not('plate', 'is', null);
      
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);
      
      // Get unique plates
      const uniquePlates = [...new Set(fuelData.map(item => item.plate))];
      
      const plateSessions = uniquePlates.map(plate => ({
        branch: plate,
        cost_code: cost_code || 'N/A',
        session_date: date,
        total_sessions: 0,
        operating_hours: 0,
        total_usage: 0,
        total_fill: 0,
        session_status: 'NO_ACTIVITY'
      }));
      
      return {
        report_type: 'daily',
        cost_code: cost_code || 'ALL',
        date,
        sessions: plateSessions,
        summary: {
          total_sessions: 0,
          total_fuel_usage: 0,
          total_operating_hours: 0
        }
      };
    }
    
    return {
      report_type: 'daily',
      cost_code: cost_code || 'ALL',
      date,
      sessions: sessions,
      summary: {
        total_sessions: sessions.length,
        total_fuel_usage: sessions.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0),
        total_operating_hours: sessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0)
      }
    };
  }
  
  async getWeeklyReportData(cost_code, week, year) {
    const weekStart = this.getDateFromWeek(week, year);
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekEnd.getDate() + 6);
    
    let query = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', weekStart.toISOString().split('T')[0])
      .lte('session_date', weekEnd.toISOString().split('T')[0]);
    
    if (cost_code) {
      query = query.eq('cost_code', cost_code);
    }
    
    const { data: sessions, error } = await query;
    if (error) throw new Error(`Database error: ${error.message}`);
    
    // If no sessions found, get plates from fuel table
    if (sessions.length === 0) {
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate')
        .not('plate', 'is', null);
      
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);
      
      // Get unique plates
      const uniquePlates = [...new Set(fuelData.map(item => item.plate))];
      
      const plateSessions = uniquePlates.map(plate => ({
        branch: plate,
        cost_code: cost_code || 'N/A',
        operating_hours: 0,
        total_usage: 0,
        total_fill: 0,
        session_status: 'NO_ACTIVITY'
      }));
      
      return {
        report_type: 'weekly',
        cost_code: cost_code || 'ALL',
        week,
        year,
        week_start: weekStart.toISOString().split('T')[0],
        week_end: weekEnd.toISOString().split('T')[0],
        sessions: plateSessions,
        summary: {
          total_sessions: 0,
          total_fuel_usage: 0,
          total_operating_hours: 0
        }
      };
    }
    
    return {
      report_type: 'weekly',
      cost_code: cost_code || 'ALL',
      week,
      year,
      week_start: weekStart.toISOString().split('T')[0],
      week_end: weekEnd.toISOString().split('T')[0],
      sessions: sessions,
      summary: {
        total_sessions: sessions.length,
        total_fuel_usage: sessions.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0),
        total_operating_hours: sessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0)
      }
    };
  }
  
  async getMonthlyReportData(cost_code, month, year) {
    const startOfMonth = new Date(year, month - 1, 1).toISOString().split('T')[0];
    const endOfMonth = new Date(year, month, 0).toISOString().split('T')[0];
    
    let query = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', startOfMonth)
      .lte('session_date', endOfMonth);
    
    if (cost_code) {
      query = query.eq('cost_code', cost_code);
    }
    
    const { data: sessions, error } = await query;
    if (error) throw new Error(`Database error: ${error.message}`);
    
    // If no sessions found, get plates from fuel table
    if (sessions.length === 0) {
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate')
        .not('plate', 'is', null);
      
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);
      
      // Get unique plates
      const uniquePlates = [...new Set(fuelData.map(item => item.plate))];
      
      const plateSessions = uniquePlates.map(plate => ({
        branch: plate,
        cost_code: cost_code || 'N/A',
        operating_hours: 0,
        total_usage: 0,
        total_fill: 0,
        session_status: 'NO_ACTIVITY'
      }));
      
      return {
        report_type: 'monthly',
        cost_code: cost_code || 'ALL',
        month,
        year,
        month_start: startOfMonth,
        month_end: endOfMonth,
        sessions: plateSessions,
        summary: {
          total_sessions: 0,
          total_fuel_usage: 0,
          total_operating_hours: 0
        }
      };
    }
    
    return {
      report_type: 'monthly',
      cost_code: cost_code || 'ALL',
      month,
      year,
      month_start: startOfMonth,
      month_end: endOfMonth,
      sessions: sessions,
      summary: {
        total_sessions: sessions.length,
        total_fuel_usage: sessions.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0),
        total_operating_hours: sessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0)
      }
    };
  }
  
  // Helper functions for week calculations
  getWeekNumber(date) {
    const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    const dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
    return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  }

  getDateFromWeek(week, year) {
    const simple = new Date(year, 0, 1 + (week - 1) * 7);
    const dow = simple.getDay();
    const ISOweekStart = simple;
    if (dow <= 4) {
      ISOweekStart.setDate(simple.getDate() - simple.getDay() + 1);
    } else {
      ISOweekStart.setDate(simple.getDate() + 8 - simple.getDay());
    }
    return ISOweekStart;
  }
}

module.exports = new EnergyRiteReportStorageController();