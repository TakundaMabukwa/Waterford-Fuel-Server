const cron = require('node-cron');
const { supabase } = require('../supabase-client');
const ExcelReportGenerator = require('../controllers/energy-rite/energyRiteExcelReportGenerator');

/**
 * Energy Rite Report Scheduler with Supabase
 */

/**
 * Get all branches and companies for report generation
 */
async function getAllBranchesAndCompanies() {
    try {
        const { data, error } = await supabase
            .from('energy_rite_fuel_data')
            .select('plate')
            .not('plate', 'is', null);
        
        if (error) throw new Error(`Database error: ${error.message}`);
        
        // Create entities from unique plates
        const uniquePlates = [...new Set(data.map(item => item.plate))];
        return uniquePlates.map(plate => ({
            branch: plate,
            company: 'EnergyRite',
            cost_code: plate
        }));
        
    } catch (error) {
        console.error('Error getting branches and companies:', error.message);
        return [];
    }
}

/**
 * Generate reports for all entities
 */
async function generateReportsForAllEntities(reportDate) {
    try {
        const entities = await getAllBranchesAndCompanies();
        const results = [];
        
        console.log(`Generating reports for ${entities.length} entities`);
        
        for (const entity of entities) {
            try {
                const dailyReport = await ExcelReportGenerator.generateDailyReport(reportDate, entity.cost_code);
                results.push(dailyReport);
                
                console.log(`Generated report for ${entity.company} - ${entity.branch}`);
                    
            } catch (error) {
                console.error(`Error generating reports for ${entity.company} - ${entity.branch}:`, error.message);
            }
        }
        
        return results;
    } catch (error) {
        console.error('Error generating reports for all entities:', error.message);
        return [];
    }
}

/**
 * Daily report generation (10 PM every day)
 */
const dailyReportJob = cron.schedule('0 22 * * *', async () => {
    try {
        console.log('Starting daily report generation at 10 PM');
        
        const reportDate = new Date();
        const results = await generateReportsForAllEntities(reportDate);
        
        console.log(`Daily report generation completed. Generated ${results.length} reports`);
            
    } catch (error) {
        console.error('Error in daily report generation:', error.message);
    }
}, {
    scheduled: false,
    timezone: "Africa/Johannesburg"
});

/**
 * Weekly report generation (10 PM every Sunday)
 */
const weeklyReportJob = cron.schedule('0 22 * * 0', async () => {
    try {
        console.log('Starting weekly report generation at 10 PM Sunday');
        
        const reportDate = new Date();
        const results = await generateReportsForAllEntities(reportDate);
        
        console.log(`Weekly report generation completed. Generated ${results.length} reports`);
            
    } catch (error) {
        console.error('Error in weekly report generation:', error.message);
    }
}, {
    scheduled: false,
    timezone: "Africa/Johannesburg"
});

/**
 * Monthly report generation (1 AM on the 1st of every month)
 */
const monthlyReportJob = cron.schedule('0 1 1 * *', async () => {
    try {
        console.log('Starting monthly report generation at 1 AM on 1st of month');
        
        const reportDate = new Date();
        const results = await generateReportsForAllEntities(reportDate);
        
        console.log(`Monthly report generation completed. Generated ${results.length} reports`);
            
    } catch (error) {
        console.error('Error in monthly report generation:', error.message);
    }
}, {
    scheduled: false,
    timezone: "Africa/Johannesburg"
});

/**
 * Start all scheduled jobs
 */
function startScheduledReports() {
    try {
        dailyReportJob.start();
        weeklyReportJob.start();
        monthlyReportJob.start();
        
        console.log('All scheduled report jobs started successfully');
        console.log('Daily reports: 10 PM every day');
        console.log('Weekly reports: 10 PM every Sunday');
        console.log('Monthly reports: 1 AM on 1st of every month');
        
    } catch (error) {
        console.error('Error starting scheduled reports:', error.message);
    }
}

/**
 * Stop all scheduled jobs
 */
function stopScheduledReports() {
    try {
        dailyReportJob.stop();
        weeklyReportJob.stop();
        monthlyReportJob.stop();
        
        console.log('All scheduled report jobs stopped');
        
    } catch (error) {
        console.error('Error stopping scheduled reports:', error.message);
    }
}

/**
 * Get scheduler status
 */
function getSchedulerStatus() {
    return {
        daily: dailyReportJob.running,
        weekly: weeklyReportJob.running,
        monthly: monthlyReportJob.running,
        timezone: "Africa/Johannesburg"
    };
}

/**
 * Manual trigger for testing
 */
async function triggerManualReport(reportType, reportDate, options = {}) {
    try {
        console.log(`Manual trigger: ${reportType} report for ${reportDate}`);
        
        let result;
        switch (reportType) {
            case 'daily':
                result = await ExcelReportGenerator.generateDailyReport(new Date(reportDate), options.cost_code);
                break;
            case 'weekly':
                result = await ExcelReportGenerator.generateWeeklyReport(new Date(reportDate), options.cost_code);
                break;
            case 'monthly':
                result = await ExcelReportGenerator.generateMonthlyReport(new Date(reportDate), options.cost_code);
                break;
            default:
                throw new Error(`Invalid report type: ${reportType}`);
        }
        
        return result;
    } catch (error) {
        console.error('Error in manual report trigger:', error.message);
        throw error;
    }
}

module.exports = {
    startScheduledReports,
    stopScheduledReports,
    getSchedulerStatus,
    triggerManualReport,
    generateReportsForAllEntities
};