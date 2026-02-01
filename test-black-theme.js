const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');

async function testBlackTheme() {
  try {
    console.log('🔄 Testing black theme Excel styling with logo...');
    
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
    
    // Logo space (rows 1-6) - BLACK THEME
    worksheet.mergeCells('A1:L6');
    const logoCell = worksheet.getCell('A1');
    logoCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
    
    // Try to add logo image
    const logoPath = path.join(__dirname, 'assets/logo.png');
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
        console.log('✅ Logo image embedded successfully');
      } else {
        logoCell.value = 'LOGO AREA';
        logoCell.font = { size: 16, bold: true, color: { argb: 'FFFFFFFF' } };
        logoCell.alignment = { horizontal: 'center', vertical: 'middle' };
        console.log('⚠️ Logo file not found, using text placeholder');
      }
    } catch (error) {
      logoCell.value = 'LOGO AREA';
      logoCell.font = { size: 16, bold: true, color: { argb: 'FFFFFFFF' } };
      logoCell.alignment = { horizontal: 'center', vertical: 'middle' };
      console.log('⚠️ Logo error, using text placeholder');
    }
    
    // Title - BLACK THEME
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
    
    // Period
    worksheet.mergeCells('A8:L8');
    const periodCell = worksheet.getCell('A8');
    periodCell.value = 'Report Period: 2024-12-19';
    periodCell.font = { size: 14, bold: true, color: { argb: 'FF333333' } };
    periodCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } };
    periodCell.alignment = { horizontal: 'center', vertical: 'middle' };
    periodCell.border = {
      bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } }
    };
    
    // Empty row
    const emptyRow = worksheet.addRow([]);
    emptyRow.eachCell((cell) => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFAFAFA' } };
    });
    
    // Header row - BLACK THEME
    const headerRow = worksheet.addRow([
      '+/-', 'Site', 'Date', 'Operating Hours', 'Opening %', 'Opening Fuel',
      'Closing %', 'Closing Fuel', 'Usage', 'Fill', 'Efficiency', 'Cost'
    ]);
    
    headerRow.eachCell((cell) => {
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
    
    // Sample data rows - BLACK THEME
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
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        } else {
          cell.font = { bold: hasMultipleSessions, color: { argb: 'FFFFFFFF' }, size: 10 };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: hasMultipleSessions ? 'FF404040' : 'FF333333' } };
          cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
        }
        cell.border = {
          top: { style: 'thin', color: { argb: 'FF666666' } },
          left: { style: 'thin', color: { argb: 'FF666666' } },
          bottom: { style: 'thin', color: { argb: 'FF666666' } },
          right: { style: 'thin', color: { argb: 'FF666666' } }
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
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1A1A1A' } };
            } else {
              cell.font = { italic: true, size: 9, color: { argb: 'FFEEEEEE' } };
              cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4A4A4A' } };
              cell.alignment = { horizontal: colNumber === 2 ? 'left' : 'center', vertical: 'middle' };
            }
            cell.border = {
              left: { style: 'thin', color: { argb: 'FF666666' } },
              right: { style: 'thin', color: { argb: 'FF666666' } },
              bottom: { style: 'hair', color: { argb: 'FF888888' } }
            };
          });
          sessionRow.height = 22;
          sessionRow.outlineLevel = 1;
        }
      }
    });
    
    // Totals row - BLACK THEME
    worksheet.addRow([]);
    const totalRow = worksheet.addRow([
      '', 'DAILY TOTALS', '6 sessions', '26.8 hours', '', '', '', '',
      '146.2L', '160.0L', '', 'R2193.50'
    ]);
    
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
    
    // Enable outline view
    worksheet.views = [{
      state: 'normal',
      showOutlineSymbols: true
    }];
    
    // Save file
    const fileName = 'Black_Theme_With_Logo_Test.xlsx';
    const filePath = path.join(__dirname, 'temp', fileName);
    
    // Ensure temp directory exists
    if (!fs.existsSync(path.dirname(filePath))) {
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
    }
    
    await workbook.xlsx.writeFile(filePath);
    
    console.log(`✅ Black theme report with logo created: ${fileName}`);
    console.log(`📁 Location: ${filePath}`);
    console.log(`🎨 Styling: Black theme with 6-row logo header`);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testBlackTheme();