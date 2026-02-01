require('dotenv').config();
const express = require('express');
const cors = require('cors');
const EnergyRiteWebSocketClient = require('./websocket-client');

const app = express();

// Middleware
app.use(express.json());
app.use(cors());

// Routes
app.use('/api/energy-rite', require('./routes/energy-rite-data'));
app.use('/api/energy-rite/vehicles', require('./routes/energy-rite-vehicles'));
app.use('/api/energy-rite/reports', require('./routes/energy-rite-reports'));
app.use('/api/energy-rite/report-storage', require('./routes/energy-rite-report-storage'));
app.use('/api/energy-rite/fuel-analysis', require('./routes/energy-rite-fuel-analysis'));
app.use('/api/energy-rite/emails', require('./routes/energy-rite-emails'));
app.use('/api/energy-rite/excel-reports', require('./routes/energy-rite-excel-reports'));
app.use('/api/energy-rite/activity-reports', require('./routes/energy-rite-activity-reports'));
app.use('/api/energy-rite/activity-excel-reports', require('./routes/energy-rite-activity-excel-reports'));
app.use('/api/energy-rite/monitoring', require('./routes/energy-rite-monitoring'));
app.use('/api/energy-rite/executive-dashboard', require('./routes/energy-rite-executive-dashboard'));
app.use('/api/energy-rite/enhanced-executive-dashboard', require('./routes/enhanced-executive-dashboard'));
app.use('/api/energy-rite/report-distribution', require('./routes/energy-rite-report-distribution'));
app.use('/api/energy-rite/fuel-fills', require('./routes/energy-rite-fuel-fills'));
app.use('/api/energy-rite/cumulative-snapshots', require('./routes/energy-rite-cumulative-snapshots'));
app.use('/api/cost-center-access', require('./routes/cost-center-access'));

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    service: 'EnergyRite Supabase Server'
  });
});

// Initialize WebSocket client
const wsClient = new EnergyRiteWebSocketClient(process.env.WEBSOCKET_URL);

// Start server
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🚀 EnergyRite Supabase server running on port ${PORT}`);
  
  // Connect to WebSocket for real-time data
  wsClient.connect();
  
  // Run session cleanup every hour
  const { cleanupOrphanedSessions } = require('./helpers/session-cleanup');
  setInterval(cleanupOrphanedSessions, 60 * 60 * 1000);
  
  // Run initial cleanup after 5 minutes
  setTimeout(cleanupOrphanedSessions, 5 * 60 * 1000);
  
  // Schedule activity snapshots
  const activitySnapshots = require('./helpers/activity-snapshots');
  
  // Take snapshots at specific times: 9AM, 2PM, 8PM
  const scheduleSnapshots = () => {
    const now = new Date();
    const hour = now.getHours();
    
    // Check if it's time for a snapshot (9, 14, 20)
    if ([9, 14, 20].includes(hour) && now.getMinutes() === 0) {
      activitySnapshots.takeSnapshot();
    }
  };
  
  // Check every minute for snapshot times
  setInterval(scheduleSnapshots, 60 * 1000);
  
  // Take initial snapshot after 2 minutes
  setTimeout(() => activitySnapshots.takeSnapshot(), 2 * 60 * 1000);
  
  // Schedule automated report distribution
  const cron = require('node-cron');
  const reportDistributionService = require('./services/energy-rite/reportDistributionService');
  
  // Daily reports at 6:30 AM
  cron.schedule('30 6 * * *', async () => {
    console.log('🕕 6:30 AM - Starting automated daily report distribution...');
    try {
      const result = await reportDistributionService.scheduleDailyReports();
      if (result.success) {
        console.log(`✅ Daily reports distributed to ${result.successful_distributions} cost code groups`);
      } else {
        console.error('❌ Daily report distribution failed:', result.error);
      }
    } catch (error) {
      console.error('❌ Error in scheduled daily report distribution:', error.message);
    }
  }, { timezone: 'Africa/Johannesburg' });
  
  // Weekly reports every Monday at 7:00 AM
  cron.schedule('0 7 * * 1', async () => {
    console.log('📊 Monday 7:00 AM - Starting automated weekly report distribution...');
    try {
      const result = await reportDistributionService.scheduleWeeklyReports();
      if (result.success) {
        console.log(`✅ Weekly reports distributed to ${result.successful_distributions} cost code groups`);
      } else {
        console.error('❌ Weekly report distribution failed:', result.error);
      }
    } catch (error) {
      console.error('❌ Error in scheduled weekly report distribution:', error.message);
    }
  }, { timezone: 'Africa/Johannesburg' });
  
  // Monthly reports on 1st day of month at 8:00 AM
  cron.schedule('0 8 1 * *', async () => {
    console.log('📈 1st of month 8:00 AM - Starting automated monthly report distribution...');
    try {
      const result = await reportDistributionService.scheduleMonthlyReports();
      if (result.success) {
        console.log(`✅ Monthly reports distributed to ${result.successful_distributions} cost code groups`);
      } else {
        console.error('❌ Monthly report distribution failed:', result.error);
      }
    } catch (error) {
      console.error('❌ Error in scheduled monthly report distribution:', error.message);
    }
  }, { timezone: 'Africa/Johannesburg' });
  
  console.log('⏰ Scheduled automated reports:');
  console.log('   📅 Daily: 6:30 AM (South Africa time)');
  console.log('   📅 Weekly: Monday 7:00 AM (South Africa time)');
  console.log('   📅 Monthly: 1st day 8:00 AM (South Africa time)');
});

// Graceful shutdown
process.on('SIGTERM', () => {
  wsClient.close();
  process.exit(0);
});

process.on('SIGINT', () => {
  wsClient.close();
  process.exit(0);
});

module.exports = { app, wsClient };