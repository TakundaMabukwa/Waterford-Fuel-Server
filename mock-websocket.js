const WebSocket = require('ws');

class MockDataSender {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.ws = null;
    this.mockData = [
      { Plate: "WOODMEAD", Speed: 45, fuel_probe_1_level: 242.7, fuel_probe_1_level_percentage: 88, DriverName: "ENGINE ON", Quality: "53.16.1.61" },
      { Plate: "NEW ROAD", Speed: 0, fuel_probe_1_level: 148.4, fuel_probe_1_level_percentage: 83, DriverName: "ENGINE OFF", Quality: "53.15.1.50" },
      { Plate: "BLUEVALLEY", Speed: 35, fuel_probe_1_level: 236, fuel_probe_1_level_percentage: 77, DriverName: "PTO ON", Quality: "60.42.2.7" },
      { Plate: "FATIMA", Speed: 0, fuel_probe_1_level: 185, fuel_probe_1_level_percentage: 100, DriverName: "PTO OFF", Quality: "61.13.2.58" },
      { Plate: "OAKDALE", Speed: 25, fuel_probe_1_level: 311.1, fuel_probe_1_level_percentage: 100, DriverName: "POSSIBLE FUEL FILL", Quality: "61.13.2.84" },
      { Plate: "MORULA", Speed: 30, fuel_probe_1_level: 236.8, fuel_probe_1_level_percentage: 83, DriverName: "ENGINE ON", Quality: "60.42.2.146" },
      { Plate: "THLABANE", Speed: 0, fuel_probe_1_level: 295, fuel_probe_1_level_percentage: 100, DriverName: "ENGINE OFF", Quality: "63.213.2.171" }
    ];
    this.currentIndex = 0;
  }

  connect() {
    console.log(`🔌 Connecting to local WebSocket: ${this.wsUrl}`);
    this.ws = new WebSocket(this.wsUrl);

    this.ws.on('open', () => {
      console.log('✅ Connected to local WebSocket');
      this.startSendingMockData();
    });

    this.ws.on('error', (error) => {
      console.error('❌ WebSocket error:', error);
    });
  }

  startSendingMockData() {
    console.log('🧪 Starting mock data transmission...');
    
    setInterval(() => {
      if (this.ws.readyState === WebSocket.OPEN) {
        const data = this.mockData[this.currentIndex];
        console.log(`📤 Sending: ${data.Plate} - ${data.DriverName} - Speed: ${data.Speed}`);
        
        this.ws.send(JSON.stringify(data));
        this.currentIndex = (this.currentIndex + 1) % this.mockData.length;
      }
    }, 5000);
  }
}

const mockSender = new MockDataSender('ws://localhost:8005');
mockSender.connect();