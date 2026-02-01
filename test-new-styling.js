const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');

async function testNewStyling() {
  try {
    console.log('🔄 Testing new light gray Excel styling...');
    
    // Create workbook
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Fuel Report Summary');
    
    // Setup columns
    worksheet.columns = [
      { width: 5 },   // A: Expand button column
      { width: 22 },  // B: Site
      { width: 14 },  // C: Date  
      { width: 16 },  // D: Operating Hours
      { width: 16 },  // E: Opening Percentage
      { width: 14 },  // F: Opening Fuel
      { width: 16 },  // G: Closing Percentage
      { width: 14 },  // H: Closing Fuel
      { width: 12 },  // I: Usage
      { width: 12 },  // J: Fill
      { width: 14 },  // K: Efficiency
      { width: 14 }   // L: Cost
    ];
    
    // Logo space (rows 1-6)
    worksheet.mergeCells('A1:L6');
    const logoCell = worksheet.getCell('A1');
    logoCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5A5A5A' } };
    logoCell.value = 'LOGO AREA';
    logoCell.font = { size: 16, bold: true, color: { argb: 'FFFFFFFF' } };
    logoCell.alignment = { horizontal: 'center', vertical: 'middle' };
    
    // Title
    worksheet.mergeCells('A7:L7');
    const titleCell = worksheet.getCell('A7');
    titleCell.value = 'FUEL REPORT SUMMARY';
    titleCell.font = { size: 22, bold: true, color: { argb: 'FFFFFFFF' } };
    titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5A5A5A' } };
    titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
    titleCell.border = {
      top: { style: 'medium', color: { argb: 'FF5A5A5A' } },
      bottom: { style: 'medium', color: { argb: 'FF5A5A5A' } }
    };
    
    // Period
    worksheet.mergeCells('A8:L8');
    const periodCell = worksheet.getCell('A8');
    periodCell.value = 'Report Period: 2024-12-19';
    periodCell.font = { size: 14, bold: true, color: { argb: 'FF5A5A5A' } };
    periodCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
    periodCell.alignment = { horizontal: 'center', vertical: 'middle' };
    periodCell.border = {
      bottom: { style: 'thin', color: { argb: 'FF5A5A5A' } }
    };
    
    // Empty row
    worksheet.addRow([]);
    
    // Header row
    const headerRow = worksheet.addRow([
      '+/-', 'Site', 'Date', 'Operating Hours', 'Opening %', 'Opening Fuel',
      'Closing %', 'Closing Fuel', 'Usage', 'Fill', 'Efficiency', 'Cost'
    ]);
    
    headerRow.eachCell((cell) => {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5A5A5A' } };
      cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
      cell.border = {
        top: { style: 'medium', color: { argb: 'FF5A5A5A' } },
        left: { style: 'thin', color: { argb: 'FF5A5A5A' } },
        bottom: { style: 'medium', color: { argb: 'FF5A5A5A' } },
        right: { style: 'thin', color: { argb: 'FF5A5A5A' } }
      };
    });
    headerRow.height = 32;
    
    // Sample data rows
    const sites = [
      { name: 'Site A', sessions: 2, hours: 8.5, usage: 45.2, fill: 50.0, cost: 678.50 },
      { name: 'Site B', sessions: 1, hours: 6.2, usage: 32.1, fill: 35.0, cost: 481.50 },
      { name: 'Site C', sessions: 3, hours: 12.1, usage: 68.9, fill: 75.0, cost: 1033.50 }
    ];
    
    sites.forEach((site, index) => {
      const hasMultipleSessions = site.sessions > 1;
      
      // Main summary row
      const summaryRow = worksheet.addRow([
        hasMultipleSessions ? '▼' : '',
        site.name,
        `${site.sessions} sessions`,
        `${site.hours} hours`,
        '', '', '', '',
        `${site.usage}L`,
        `${site.fill}L`,
        `${(site.usage/site.hours).toFixed(2)}L/h`,
        `R${site.cost}`
      ]);
      
      summaryRow.eachCell((cell, colNumber) => {
        if (colNumber === 1) {
          cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 12 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5A5A5A' } };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        } else {
          cell.font = { bold: hasMultipleSessions, color: { argb: 'FF000000' }, size: 10 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: hasMultipleSessions ? 'FFE8E8E8' : 'FFF5F5F5' } };
          cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
        }
        cell.border = {
          top: { style: 'thin', color: { argb: 'FF5A5A5A' } },
          left: { style: 'thin', color: { argb: 'FF5A5A5A' } },
          bottom: { style: 'thin', color: { argb: 'FF5A5A5A' } },
          right: { style: 'thin', color: { argb: 'FF5A5A5A' } }
        };
      });
      
      // Add session details for multi-session sites
      if (hasMultipleSessions) {
        for (let i = 1; i <= site.sessions; i++) {
          const sessionRow = worksheet.addRow([
            '',
            `  └ Session ${i}`,
            '2024-12-19',
            `${(site.hours/site.sessions).toFixed(1)} hours`,
            '85%', '42.5L', '15%', '7.5L',
            `${(site.usage/site.sessions).toFixed(1)}L`,
            `${(site.fill/site.sessions).toFixed(1)}L`,
            `${(site.usage/site.hours).toFixed(2)}L/h`,
            `R${(site.cost/site.sessions).toFixed(2)}`
          ]);
          
          sessionRow.eachCell((cell, colNumber) => {
            if (colNumber === 1) {
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5A5A5A' } };
            } else {
              cell.font = { italic: true, size: 9, color: { argb: 'FF666666' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEEEEEE' } };
              cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
            }
            cell.border = {
              left: { style: 'thin', color: { argb: 'FF5A5A5A' } },
              right: { style: 'thin', color: { argb: 'FF5A5A5A' } },
              bottom: { style: 'hair', color: { argb: 'FF5A5A5A' } }
            };
          });
          sessionRow.height = 22;
          sessionRow.outlineLevel = 1;
        }
      }
    });
    
    // Totals row
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      '', 'DAILY TOTALS', '6 sessions', '26.8 hours', '', '', '', '',
      '146.2L', '160.0L', '', 'R2193.50'
    ]);
    
    totalRow.eachCell((cell, colNumber) => {
      cell.font = { bold: true, size: 12, color: { argb: 'FFFFFFFF' } };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5A5A5A' } };
      cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
      cell.border = {
        top: { style: 'thick', color: { argb: 'FF5A5A5A' } },
        left: { style: 'thin', color: { argb: 'FF5A5A5A' } },
        bottom: { style: 'thick', color: { argb: 'FF5A5A5A' } },
        right: { style: 'thin', color: { argb: 'FF5A5A5A' } }
      };
    });
    totalRow.height = 30;
    
    // Enable outline view
    worksheet.views = [{
      state: 'normal',
      showOutlineSymbols: true
    }];
    
    // Save file
    const fileName = 'Light_Gray_Styling_Test.xlsx';
    const filePath = path.join(__dirname, 'temp', fileName);
    
    // Ensure temp directory exists
    if (!fs.existsSync(path.dirname(filePath))) {
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
    }
    
    await workbook.xlsx.writeFile(filePath);
    
    console.log(`✅ Light gray styling test report created: ${fileName}`);
    console.log(`📁 Location: ${filePath}`);
    console.log(`🎨 Styling: Light gray headers (#5A5A5A) with light backgrounds`);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testNewStyling();