const WebSocket = require('ws');

const PORT = parseInt(process.env.DUMMY_WS_PORT || '8099', 10);
const TEST_PLATE = process.env.DUMMY_TEST_PLATE || 'ZZTESTFUEL01';
const START_TIME = new Date(process.env.DUMMY_START_TIME || '2026-04-02T06:06:00Z');

const wss = new WebSocket.Server({ port: PORT });

function formatLocTime(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function buildMessage({ secondsFromStart, fuel, driverName = '', speed = 0 }) {
  const locDate = new Date(START_TIME.getTime() + (secondsFromStart * 1000));
  const percentage = Math.round((fuel / 300) * 100);

  return {
    Plate: TEST_PLATE,
    Speed: speed,
    Latitude: -26.223996,
    Longitude: 28.14653,
    Quality: '54.125.1.247',
    Mileage: 579400,
    Pocsagstr: '54.125.1.247',
    Head: '',
    Geozone: '',
    DriverName: driverName,
    NameEvent: '',
    Temperature: '19,405,5700,54,00,256,00,255,00,3D0,00,70,03,100,00,BE,00,5B,00,523,00,247,41A4,6E,25,175,23,395,3180FD55,177,1B,1086,0270,1087,0278,1088,0278',
    LocTime: formatLocTime(locDate),
    message_type: 405,
    fuel_probe_1_level: fuel.toFixed(1),
    fuel_probe_1_volume_in_tank: fuel.toFixed(1),
    fuel_probe_1_temperature: '25',
    fuel_probe_1_level_percentage: String(percentage)
  };
}

const sequence = [
  { delayMs: 1000, message: buildMessage({ secondsFromStart: 0, fuel: 100.0, speed: 0 }) },
  { delayMs: 4000, message: buildMessage({ secondsFromStart: 30, fuel: 100.2, speed: 0 }) },
  { delayMs: 7000, message: buildMessage({ secondsFromStart: 70, fuel: 110.8, speed: 0 }) },
  { delayMs: 10000, message: buildMessage({ secondsFromStart: 90, fuel: 111.1, speed: 0 }) },
  { delayMs: 13000, message: buildMessage({ secondsFromStart: 120, fuel: 111.0, speed: 0 }) },
  { delayMs: 16000, message: buildMessage({ secondsFromStart: 150, fuel: 111.2, speed: 0 }) }
];

function describe(message) {
  return `${message.Plate} @ ${message.LocTime} fuel=${message.fuel_probe_1_volume_in_tank}L speed=${message.Speed}`;
}

function streamSequence(ws) {
  for (const step of sequence) {
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      console.log(`Sending: ${describe(step.message)}`);
      ws.send(JSON.stringify(step.message));
    }, step.delayMs);
  }

  setTimeout(() => {
    console.log('Sequence sent. Leave the client running for at least 2 real minutes to allow watcher stabilization.');
  }, Math.max(...sequence.map(step => step.delayMs)) + 500);
}

wss.on('connection', (ws) => {
  console.log(`Client connected. Streaming test fill sequence for ${TEST_PLATE}.`);
  streamSequence(ws);

  ws.on('close', () => {
    console.log('Client disconnected.');
  });
});

console.log(`Dummy Waterford fill WebSocket listening on ws://localhost:${PORT}`);
console.log(`Test plate: ${TEST_PLATE}`);
console.log('Scenario: 100.2L -> 110.8L in 40 seconds while stationary, then stabilize around 111L.');
