const { supabase } = require('../../supabase-client');

/**
 * Energy Rite Report Documents Controller
 */

async function getReportDocuments(req, res) {
    try {
        const { branch, company, reportType, startDate, endDate, cost_code } = req.query;
        
        console.log('Fetching report documents');

        let query = supabase
            .from('energy_rite_report_docs')
            .select('*');

        if (branch) query = query.eq('branch', branch);
        if (company) query = query.eq('company', company);
        if (reportType) query = query.eq('document_type', reportType);
        if (cost_code) query = query.eq('branch', cost_code);
        if (startDate) query = query.gte('document_date', startDate);
        if (endDate) query = query.lte('document_date', endDate);

        const { data: documents, error } = await query.order('created_at', { ascending: false });
        
        if (error) throw new Error(`Database error: ${error.message}`);
        
        // Add download URLs
        const documentsWithUrls = documents.map(doc => ({
            ...doc,
            downloadUrl: `/api/energy-rite-reports/download/${doc.id}`
        }));

        res.status(200).json({
            success: true,
            data: documentsWithUrls,
            count: documents.length,
            message: "Report documents retrieved successfully"
        });

    } catch (error) {
        console.error('Error fetching report documents:', error.message);
        res.status(500).json({
            success: false,
            error: "Failed to fetch report documents",
            message: error.message
        });
    }
}

async function downloadReportDocument(req, res) {
    try {
        const { id } = req.params;
        
        console.log(`Downloading report document: ${id}`);

        const { data: document, error } = await supabase
            .from('energy_rite_report_docs')
            .select('*')
            .eq('id', id)
            .single();
        
        if (error || !document) {
            return res.status(404).json({
                success: false,
                error: "Document not found"
            });
        }

        // For now, return document info (file serving would need additional setup)
        res.status(200).json({
            success: true,
            data: document,
            message: "Document found - file serving not implemented yet"
        });

    } catch (error) {
        console.error('Error downloading document:', error.message);
        res.status(500).json({
            success: false,
            error: "Failed to download document",
            message: error.message
        });
    }
}

async function generateManualReport(req, res) {
    try {
        const { reportType, reportDate, branch, company, cost_code } = req.body;
        
        if (!reportType || !reportDate) {
            return res.status(400).json({
                success: false,
                error: "Report type and date are required"
            });
        }

        console.log(`Generating manual ${reportType} report for ${reportDate}`);

        // This would integrate with the Excel report generator
        const result = {
            success: true,
            reportType,
            reportDate,
            branch,
            company,
            cost_code,
            message: "Manual report generation not fully implemented yet"
        };

        res.status(200).json({
            success: true,
            data: result,
            message: `${reportType} report generated successfully`
        });

    } catch (error) {
        console.error('Error generating manual report:', error.message);
        res.status(500).json({
            success: false,
            error: "Failed to generate report",
            message: error.message
        });
    }
}

async function getReportStatistics(req, res) {
    try {
        console.log('Fetching report statistics');

        // Get basic statistics
        const { data: allDocs, error: allError } = await supabase
            .from('energy_rite_report_docs')
            .select('document_type, document_size, branch, company, created_at');

        if (allError) throw new Error(`Database error: ${allError.message}`);

        // Calculate statistics
        const stats = {
            total_reports: allDocs.length,
            daily_reports: allDocs.filter(d => d.document_type === 'daily').length,
            weekly_reports: allDocs.filter(d => d.document_type === 'weekly').length,
            monthly_reports: allDocs.filter(d => d.document_type === 'monthly').length,
            total_file_size: allDocs.reduce((sum, doc) => sum + (doc.document_size || 0), 0),
            unique_branches: [...new Set(allDocs.map(d => d.branch).filter(Boolean))].length,
            unique_companies: [...new Set(allDocs.map(d => d.company).filter(Boolean))].length
        };

        // Get recent reports by type
        const recentReports = ['daily', 'weekly', 'monthly'].map(type => {
            const typeReports = allDocs.filter(d => d.document_type === type);
            return {
                report_type: type,
                count: typeReports.length,
                latest_report: typeReports.length > 0 ? 
                    Math.max(...typeReports.map(r => new Date(r.created_at).getTime())) : null
            };
        });

        // Get top branches
        const branchCounts = {};
        allDocs.forEach(doc => {
            if (doc.branch) {
                branchCounts[doc.branch] = (branchCounts[doc.branch] || 0) + 1;
            }
        });

        const topBranches = Object.entries(branchCounts)
            .map(([branch, count]) => ({ branch, report_count: count }))
            .sort((a, b) => b.report_count - a.report_count)
            .slice(0, 10);

        res.status(200).json({
            success: true,
            data: {
                summary: stats,
                recentReports: recentReports,
                topBranches: topBranches
            },
            message: "Report statistics retrieved successfully"
        });

    } catch (error) {
        console.error('Error fetching report statistics:', error.message);
        res.status(500).json({
            success: false,
            error: "Failed to fetch report statistics",
            message: error.message
        });
    }
}

async function deleteReportDocument(req, res) {
    try {
        const { id } = req.params;
        
        console.log(`Deleting report document: ${id}`);

        const { data, error } = await supabase
            .from('energy_rite_report_docs')
            .delete()
            .eq('id', id)
            .select();
        
        if (error) throw new Error(`Database error: ${error.message}`);
        
        if (data.length === 0) {
            return res.status(404).json({
                success: false,
                error: "Document not found"
            });
        }

        res.status(200).json({
            success: true,
            message: "Report document deleted successfully"
        });

    } catch (error) {
        console.error('Error deleting document:', error.message);
        res.status(500).json({
            success: false,
            error: "Failed to delete document",
            message: error.message
        });
    }
}

module.exports = {
    getReportDocuments,
    downloadReportDocument,
    generateManualReport,
    getReportStatistics,
    deleteReportDocument
};