const { supabase } = require('../../supabase-client');
const ExcelJS = require('exceljs');

/**
 * Energy Rite Report Generator with Supabase Storage
 */

/**
 * Generate Excel report and store in Supabase
 */
async function generateReportWithDBStorage(reportType, reportDate, options = {}) {
    try {
        console.log(`Generating ${reportType} report with Supabase storage for ${reportDate.toISOString().split('T')[0]}`);

        const { startDate, endDate } = calculateDateRange(reportType, reportDate);
        const reportData = await getReportData(startDate, endDate, options);
        
        if (reportData.length === 0) {
            console.log(`No data found for ${reportType} report`);
            return null;
        }

        const workbook = await createExcelReport(reportData, startDate, endDate, reportType);
        const buffer = await workbook.xlsx.writeBuffer();
        const fileName = generateFileName(reportType, reportDate, options);
        
        const docRecord = await storeFileInDatabase({
            reportType,
            reportDate,
            startDate,
            endDate,
            fileName,
            fileBuffer: buffer,
            fileSize: buffer.length,
            options
        });

        console.log(`${reportType} report generated and stored: ${fileName} (${buffer.length} bytes)`);
        
        return {
            success: true,
            fileName,
            fileSize: buffer.length,
            docRecord,
            downloadUrl: `/api/energy-rite-reports/download-db/${docRecord.id}`
        };

    } catch (error) {
        console.error(`Error generating ${reportType} report:`, error.message);
        throw error;
    }
}

function calculateDateRange(reportType, reportDate) {
    const date = new Date(reportDate);
    let startDate, endDate;

    switch (reportType) {
        case 'daily':
            startDate = new Date(date);
            endDate = new Date(date);
            break;
            
        case 'weekly':
            const dayOfWeek = date.getDay();
            startDate = new Date(date);
            startDate.setDate(date.getDate() - dayOfWeek);
            endDate = new Date(startDate);
            endDate.setDate(startDate.getDate() + 6);
            break;
            
        case 'monthly':
            startDate = new Date(date.getFullYear(), date.getMonth(), 1);
            endDate = new Date(date.getFullYear(), date.getMonth() + 1, 0);
            break;
            
        default:
            throw new Error(`Invalid report type: ${reportType}`);
    }

    return { startDate, endDate };
}

async function getReportData(startDate, endDate, options = {}) {
    let query = supabase
        .from('energy_rite_reports')
        .select('*')
        .gte('report_date', startDate.toISOString().split('T')[0])
        .lte('report_date', endDate.toISOString().split('T')[0]);

    if (options.branch) {
        query = query.eq('branch', options.branch);
    }
    
    if (options.company) {
        query = query.eq('company', options.company);
    }
    
    if (options.cost_code) {
        query = query.eq('cost_code', options.cost_code);
    }

    const { data, error } = await query.order('branch').order('report_date');
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    return data;
}

async function createExcelReport(data, startDate, endDate, reportType) {
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Energy Rite Supabase Server';
    workbook.created = new Date();

    const worksheet = workbook.addWorksheet('Energy Rite Report');
    
    worksheet.columns = [
        { width: 15 }, { width: 12 }, { width: 15 }, { width: 18 }, { width: 15 },
        { width: 18 }, { width: 15 }, { width: 12 }, { width: 12 }, { width: 18 },
        { width: 15 }, { width: 20 }, { width: 15 }, { width: 15 }
    ];

    // Add title
    worksheet.mergeCells('A1:N1');
    const titleCell = worksheet.getCell('A1');
    titleCell.value = `${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`;
    titleCell.font = { bold: true, size: 14 };
    titleCell.alignment = { horizontal: 'center' };

    worksheet.getRow(2).height = 10;

    // Add headers
    const headers = [
        'Branch', 'Date', 'Total Usage', 'Total Cost', 'Report Data',
        'Company', 'Cost Code', 'Created At', 'Updated At'
    ];

    headers.forEach((header, index) => {
        const cell = worksheet.getCell(3, index + 1);
        cell.value = header;
        cell.font = { bold: true };
        cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FFE0E0E0' }
        };
        cell.border = {
            top: { style: 'thin' },
            left: { style: 'thin' },
            bottom: { style: 'thin' },
            right: { style: 'thin' }
        };
    });

    // Add data rows
    let row = 4;
    data.forEach(item => {
        worksheet.getCell(row, 1).value = item.branch || '';
        worksheet.getCell(row, 2).value = item.report_date ? new Date(item.report_date).toLocaleDateString() : '';
        worksheet.getCell(row, 3).value = item.total_usage || '';
        worksheet.getCell(row, 4).value = item.total_cost || '';
        worksheet.getCell(row, 5).value = item.report_data ? JSON.stringify(item.report_data) : '';
        worksheet.getCell(row, 6).value = item.company || '';
        worksheet.getCell(row, 7).value = item.cost_code || '';
        worksheet.getCell(row, 8).value = item.created_at ? new Date(item.created_at).toLocaleDateString() : '';
        worksheet.getCell(row, 9).value = item.updated_at ? new Date(item.updated_at).toLocaleDateString() : '';

        for (let col = 1; col <= 9; col++) {
            worksheet.getCell(row, col).border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
        }
        row++;
    });

    if (data.length > 0) {
        row++;
        worksheet.mergeCells(`A${row}:B${row}`);
        worksheet.getCell(row, 1).value = 'TOTAL RECORDS:';
        worksheet.getCell(row, 3).value = data.length;
        worksheet.getCell(row, 1).font = { bold: true };
        worksheet.getCell(row, 3).font = { bold: true };
    }

    return workbook;
}

function generateFileName(reportType, reportDate, options = {}) {
    const dateStr = reportDate.toISOString().split('T')[0];
    const branch = options.branch ? `_${options.branch.replace(/[^a-zA-Z0-9]/g, '_')}` : '';
    const company = options.company ? `_${options.company.replace(/[^a-zA-Z0-9]/g, '_')}` : '';
    const costCode = options.cost_code ? `_${options.cost_code.replace(/[^a-zA-Z0-9]/g, '_')}` : '';
    
    return `EnergyRite_${reportType}_${dateStr}${branch}${company}${costCode}.xlsx`;
}

async function storeFileInDatabase({ reportType, reportDate, startDate, endDate, fileName, fileBuffer, fileSize, options }) {
    const { data, error } = await supabase
        .from('energy_rite_report_docs')
        .insert({
            document_name: fileName,
            document_type: reportType,
            branch: options.branch || null,
            company: options.company || null,
            document_path: `/reports/${fileName}`,
            document_size: fileSize,
            document_date: reportDate.toISOString().split('T')[0]
        })
        .select();

    if (error) throw new Error(`Database error: ${error.message}`);
    
    return data[0];
}

async function getFileFromDatabase(id) {
    const { data, error } = await supabase
        .from('energy_rite_report_docs')
        .select('*')
        .eq('id', id)
        .single();
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    return data;
}

async function getDocumentsFromDB(filters = {}) {
    let query = supabase
        .from('energy_rite_report_docs')
        .select('id, branch, company, document_type, document_name, document_size, document_date, created_at');

    if (filters.branch) {
        query = query.eq('branch', filters.branch);
    }
    
    if (filters.company) {
        query = query.eq('company', filters.company);
    }
    
    if (filters.reportType) {
        query = query.eq('document_type', filters.reportType);
    }
    
    if (filters.startDate) {
        query = query.gte('document_date', filters.startDate);
    }
    
    if (filters.endDate) {
        query = query.lte('document_date', filters.endDate);
    }

    const { data, error } = await query.order('created_at', { ascending: false });
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    return data;
}

module.exports = {
    generateReportWithDBStorage,
    getFileFromDatabase,
    getDocumentsFromDB
};