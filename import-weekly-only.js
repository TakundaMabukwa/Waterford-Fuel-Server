require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importWeeklyOnly() {
  console.log('📊 IMPORTING Weekly (4).xlsx ONLY\n');
  
  try {
    const workbook = XLSX.readFile('./historical-imports/Weekly (4).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);
    
    console.log(`📄 Found ${data.length} rows`);
    
    let imported = 0;
    
    for (const row of data) {
      const session = parseWeeklyRow(row);
      if (session) {
        await insertSession(session);
        imported++;
      }
    }
    
    console.log(`✅ Imported ${imported} sessions from Weekly (4).xlsx`);
    
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

function parseWeeklyRow(row) {
  const site = row.Site;
  if (!site || site.includes('Total') || site.includes('Site')) return null;
  
  const sessionDate = parseExcelDate(row.Date);
  if (!sessionDate) return null;
  
  const hours = parseFloat(row['Operating Hours']) || 0;
  if (hours <= 0) return null;
  
  const startTime = new Date(sessionDate);
  startTime.setHours(6, 0, 0, 0);
  const endTime = new Date(startTime.getTime() + (hours * 60 * 60 * 1000));
  
  return {
    branch: site.trim(),
    company: 'KFC',
    cost_code: getActualCostCode(site.trim()),
    session_date: sessionDate.toISOString().split('T')[0],
    session_start_time: startTime.toISOString(),
    session_end_time: endTime.toISOString(),
    operating_hours: hours,
    opening_percentage: parseFloat(row['Opening Percentage']) || 0,
    opening_fuel: parseFloat(row['Opening Fuel']) || 0,
    closing_percentage: parseFloat(row['Closing Percentage']) || 0,
    closing_fuel: parseFloat(row['Closing Fuel']) || 0,
    total_usage: parseFloat(row['Total Usage']) || 0,
    total_fill: parseFloat(row['Total Fill']) || 0,
    liter_usage_per_hour: parseFloat(row['Liter Usage Per Hour']) || 0,
    cost_for_usage: parseFloat(row['Cost For Usage']) || 0,
    session_status: 'COMPLETED',
    notes: 'Imported from Weekly (4).xlsx'
  };
}

function parseExcelDate(dateValue) {
  if (!dateValue) return null;
  
  if (typeof dateValue === 'number') {
    return new Date((dateValue - 25569) * 86400 * 1000);
  }
  
  if (typeof dateValue === 'string') {
    const parsed = new Date(dateValue);
    if (!isNaN(parsed.getTime())) return parsed;
  }
  
  return null;
}

function getActualCostCode(site) {
  const SITE_COST_CODES = {
    'NEW ROAD': 'KFC-0001-0001-0002-0004',
    'KEYWEST': 'KFC-0001-0001-0003',
    'DURBANVILL': 'KFC-0001-0001-0002-0002',
    'BAMBANANI': 'KFC-0001-0001-0003',
    'BLOEM1': 'KFC-0001-0001-0003',
    'KYALAMI': 'KFC-0001-0001-0002-0004',
    'REDS': 'KFC-0001-0001-0002-0003',
    'GALESHEWE': 'KFC-0001-0001-0003',
    'TAMBOTIE': 'KFC-0001-0001-0003',
    'JANSENPARK': 'KFC-0001-0001-0003',
    'FARRAMERE': 'KFC-0001-0001-0003',
    'KIMBERLEY2': 'KFC-0001-0001-0003',
    'ELDORADO': 'KFC-0001-0001-0003',
    'CHUMA MALL': 'KFC-0001-0001-0003',
    'FLORENTIA': 'KFC-0001-0001-0003',
    'NORTHMEAD': 'KFC-0001-0001-0003',
    'SPUR THUVL': 'KFC-0001-0002',
    'KRUGERSDOR': 'KFC-0001-0001-0003',
    'THE WEDGE': 'KFC-0001-0001-0003',
    'MOBILE 2': 'KFC-0001-0001-0003',
    'BLOEM2': 'KFC-0001-0001-0003',
    'BRACKENHUR': 'KFC-0001-0001-0003',
    'NOORDHEUW': 'KFC-0001-0001-0003',
    'PRELLER': 'KFC-0001-0001-0003',
    'MOBILE 4': 'KFC-0001-0001-0003',
    'LANGENHOVE': 'KFC-0001-0001-0003',
    'FERNDALE': 'KFC-0001-0001-0003',
    'BRAAMFONTE': 'KFC-0001-0001-0003',
    'BERGBRON': 'KFC-0001-0001-0003',
    'BIRCHNORTH': 'KFC-0001-0001-0003',
    'MEADOWDALE': 'KFC-0001-0001-0003',
    'CARLTONVIL': 'KFC-0001-0001-0003',
    'MOBILE 3': 'KFC-0001-0001-0003',
    'WOODMEAD': 'KFC-0001-0001-0003-test',
    'NEW MARKET': 'KFC-0001-0001-0003',
    'BLUE HILL': 'KFC-0001-0001-0002-0004',
    'GRAYSTON': 'KFC-0001-0001-0003',
    'BLUEVALLEY': 'KFC-0001-0001-0002-0003',
    'BEYERSPARK': 'KFC-0001-0001-0003',
    'SPRINGS': 'KFC-0001-0001-0003',
    'HQ UNIT': 'KFC-0001-0001-0002-0001',
    'HQ BACKUP': 'KFC-0001-0001-0002-0001',
    'THABONG': 'KFC-0001-0001-0003',
    'PHOKENG': 'KFC-0001-0001-0002-0001-0002',
    'DENVER': 'KFC-0001-0001-0003',
    'RYNFIELD': 'KFC-0001-0001-0003',
    'CARLTONV 2': 'KFC-0001-0001-0003',
    'MARLBORO': 'KFC-0001-0001-0002-0004',
    'MILNERTON': 'KFC-0001-0001-0002-0002',
    'KBY WEST': 'KFC-0001-0001-0003',
    'MIDRANDMAL': 'KFC-0001-0001-0002-0004',
    'BIRCHSOUTH': 'KFC-0001-0001-0003',
    'RANDFONTEI': 'KFC-0001-0001-0003',
    'SUNWARD': 'KFC-0001-0001-0003',
    'LAMBTON': 'KFC-0001-0001-0003',
    'KROONSTAD2': 'KFC-0001-0001-0003',
    'HEBRON': 'KFC-0001-0001-0002-0005',
    'FOURWAYS': 'KFC-0001-0001-0003',
    'WILLOW': 'KFC-0001-0001-0003',
    'LYTTELTON': 'KFC-0001-0001-0002-0001-0001',
    'MORULA': 'KFC-0001-0001-0002-0005',
    'KIMBERLEY3': 'KFC-0001-0001-0003',
    'RANDBURG': 'KFC-0001-0001-0003',
    'CENTURION': 'KFC-0001-0001-0002-0003',
    'IRENE': 'KFC-0001-0001-0002-0003',
    'CASTLEGATE': 'KFC-0001-0001-0002-0005',
    'ALEX': 'KFC-0001-0001-0001',
    'MODDERSPRU': 'KFC-0001-0001-0002-0001-0001',
    'CARLTONV 3': 'KFC-0001-0001-0003',
    'GERMSITON': 'KFC-0001-0001-0003',
    'WESTONARIA': 'KFC-0001-0001-0003',
    'BOITEKONG': 'KFC-0001-0001-0002-0001-0002',
    'MUSHROOM': 'KFC-0001-0001-0002-0004',
    'FATIMA': 'KFC-0001-0001-0002-0001-0002',
    'RIETFONTEI': 'KFC-0001-0001-0002-0003',
    'BALLYCLARE': 'KFC-0001-0001-0002-0004',
    'EBONY': 'KFC-0001-0001-0002-0005',
    'LIONSPRIDE': 'KFC-0001-0001-0002-0004',
    'OLIEVENHOU': 'KFC-0001-0001-0002-0003',
    'WELKOM': 'KFC-0001-0001-0003',
    'OAKDALE': 'KFC-0001-0001-0002-0002'
  };
  
  return SITE_COST_CODES[site.toUpperCase()] || 'KFC-0001-0001-0003';
}

async function insertSession(session) {
  try {
    const { error } = await supabase
      .from('energy_rite_operating_sessions')
      .insert(session);
    
    if (error) throw error;
    
  } catch (error) {
    console.error(`❌ Error inserting ${session.branch}:`, error.message);
  }
}

importWeeklyOnly();