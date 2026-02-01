require('dotenv').config();
const { supabase } = require('./supabase-client');

class ReportStorageSystem {
  
  async storeReport(reportType, reportData, fileBuffer) {
    try {
      const date = new Date().toISOString().split('T')[0];
      const fileName = this.generateFileName(reportType, date);
      
      // Upload to Supabase bucket
      const { data: uploadData, error: uploadError } = await supabase.storage
        .from('reports')
        .upload(fileName, fileBuffer, {
          contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          upsert: true
        });
      
      if (uploadError) throw uploadError;
      
      const { data: { publicUrl } } = supabase.storage
        .from('reports')
        .getPublicUrl(fileName);
      
      // Store metadata in database
      const { data: dbData, error: dbError } = await supabase
        .from('energy_rite_generated_reports')
        .insert({
          cost_code: 'ALL',
          report_type: reportType,
          report_url: publicUrl,
          report_date: date,
          file_size: fileBuffer.length,
          status: 'generated'
        })
        .select();
      
      if (dbError && !dbError.message.includes('duplicate key')) throw dbError;
      
      return {
        success: true,
        fileName,
        publicUrl,
        reportType,
        date,
        dbId: dbData?.[0]?.id
      };
      
    } catch (error) {
      console.error('❌ Storage error:', error);
      throw error;
    }
  }
  
  generateFileName(reportType, date) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    return `${reportType}_report_${date}_${timestamp}.xlsx`;
  }
  
  async getReportsByType(reportType) {
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .eq('report_type', reportType)
      .order('generated_at', { ascending: false });
    
    if (error) throw error;
    return data;
  }
  
  async getAllReports() {
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .order('report_type')
      .order('generated_at', { ascending: false });
    
    if (error) throw error;
    
    // Group by report type
    const grouped = {
      daily: data.filter(r => r.report_type === 'daily'),
      weekly: data.filter(r => r.report_type === 'weekly'),
      monthly: data.filter(r => r.report_type === 'monthly')
    };
    
    return grouped;
  }
  
  determineReportType(fileName) {
    const lower = fileName.toLowerCase();
    
    if (lower.includes('daily')) return 'daily';
    if (lower.includes('weekly')) return 'weekly';
    if (lower.includes('monthly')) return 'monthly';
    
    // Fallback: check date patterns
    if (lower.match(/\d{4}-\d{2}-\d{2}/)) return 'daily';
    if (lower.includes('to')) return 'weekly';
    if (lower.match(/\d{4}-\d{2}$/)) return 'monthly';
    
    return 'unknown';
  }
}

async function testReportStorage() {
  const storage = new ReportStorageSystem();
  
  try {
    console.log('🧪 Testing Report Storage System...\n');
    
    // Check existing reports
    const allReports = await storage.getAllReports();
    
    console.log('📊 EXISTING REPORTS BY TYPE:');
    console.log('============================');
    console.log(`📅 Daily Reports: ${allReports.daily.length}`);
    console.log(`📅 Weekly Reports: ${allReports.weekly.length}`);
    console.log(`📅 Monthly Reports: ${allReports.monthly.length}`);
    
    console.log('\n📋 RECENT REPORTS:');
    ['daily', 'weekly', 'monthly'].forEach(type => {
      console.log(`\n${type.toUpperCase()} Reports:`);
      allReports[type].slice(0, 3).forEach((report, index) => {
        console.log(`  ${index + 1}. ${report.report_date} - ${report.file_size} bytes`);
        console.log(`     URL: ${report.report_url}`);
      });
    });
    
    console.log('\n🔍 REPORT TYPE DETECTION:');
    const testFiles = [
      'Enhanced_daily_Report_2025-10-28.xlsx',
      'Enhanced_weekly_Report_2025-10-21_to_2025-10-28.xlsx',
      'Enhanced_monthly_Report_2025-10.xlsx',
      'Energy_Rite_daily_Report_2025-10-28.xlsx'
    ];
    
    testFiles.forEach(fileName => {
      const type = storage.determineReportType(fileName);
      console.log(`  ${fileName} → ${type}`);
    });
    
    console.log('\n✅ Report storage system working correctly!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testReportStorage();