const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);

// New sites from the vehicle data
const newSites = [
  { plate: 'SUNVALLEY', cost_code: 'KFC-0001-0001-0002-0002' },
  { plate: 'VORNAVALL', cost_code: 'KFC-0001-0001-0002-0004' },
  { plate: 'GLENMARAIS', cost_code: 'KFC-0001-0001-0003' },
  { plate: 'RUSTENBU3', cost_code: 'KFC-0001-0001-0002-0005' },
  { plate: 'SAFARI GAR', cost_code: 'KFC-0001-0001-0002-0001-0001' },
  { plate: 'YARONA', cost_code: 'KFC-0001-0001-0002-0005' },
  { plate: 'THLABANE', cost_code: 'KFC-0001-0001-0002-0001-0002' },
  { plate: 'TRIANGLE', cost_code: 'KFC-0001-0001-0002-0001-0001' },
  { plate: 'RUSTENBUR2', cost_code: 'KFC-0001-0001-0002-0001-0002' },
  { plate: 'WAVERLY', cost_code: 'KFC-0001-0001-0002-0005' },
  { plate: 'KROONSTAD', cost_code: 'KFC-0001-0001-0003' },
  { plate: 'WIERDAPARK', cost_code: 'KFC-0001-0001-0002-0003' },
  { plate: 'RIVONIA', cost_code: 'KFC-0001-0001-0002-0004' },
  { plate: 'KEEPALIVE', cost_code: null }
];

async function updateVehicleLookup() {
  try {
    console.log('Checking existing sites...');
    
    // Get existing sites
    const { data: existingSites, error: fetchError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate');
    
    if (fetchError) {
      console.error('Error fetching existing sites:', fetchError);
      return;
    }
    
    const existingPlates = existingSites.map(site => site.plate);
    console.log(`Found ${existingPlates.length} existing sites`);
    
    // Filter new sites that don't exist
    const sitesToAdd = newSites.filter(site => !existingPlates.includes(site.plate));
    
    if (sitesToAdd.length === 0) {
      console.log('All sites already exist in the lookup table');
      return;
    }
    
    console.log(`Adding ${sitesToAdd.length} new sites:`, sitesToAdd.map(s => s.plate));
    
    // Insert new sites
    const { data, error } = await supabase
      .from('energyrite_vehicle_lookup')
      .insert(sitesToAdd)
      .select();
    
    if (error) {
      console.error('Error inserting new sites:', error);
      return;
    }
    
    console.log(`Successfully added ${data.length} new sites to vehicle lookup table`);
    data.forEach(site => {
      console.log(`- ${site.plate}: ${site.cost_code || 'No cost code'}`);
    });
    
  } catch (error) {
    console.error('Script error:', error);
  }
}

updateVehicleLookup();