"""
Translation system for CropDrive™ AI Assistant
Supports English and Bahasa Malaysia (Malaysian)
"""

import streamlit as st
from typing import Dict, Any

# Translation dictionaries
TRANSLATIONS = {
    'en': {
        # Common
        'app_title': 'CropDrive™ AI Assistant',
        'app_subtitle': 'Advanced Oil Palm Cultivation Analysis System',
        'language': 'Language',
        'english': 'English',
        'malay': 'Malaysian',
        'toggle_language': 'Toggle Language',
        
        # Navigation
        'nav_home': 'Home',
        'nav_analyze': 'Analyze Files',
        'nav_admin': 'Admin Panel',
        'nav_help_improve': '💬 Help Us Improve',
        'nav_upload': 'Upload',
        
        # Home Page
        'home_title': 'Welcome to CropDrive™ AI Assistant',
        'home_what_title': 'What this tool does:',
        'home_what_1': 'Reads your soil and leaf test reports',
        'home_what_2': 'Analyzes the data using AI',
        'home_what_3': 'Gives you farming recommendations',
        'home_what_4': 'Shows yield predictions for your plantation',
        'home_how_title': 'How to use it:',
        'home_how_1': 'Upload your test reports',
        'home_how_2': 'Enter your farm details',
        'home_how_3': 'Get your analysis results',
        'home_how_4': 'Review recommendations and insights',
        'home_ready': 'Ready to get started?',
        'home_ready_desc': 'Upload your oil palm test reports and get helpful farming advice.',
        'home_start': 'Start Analysis',
        'home_upload_files': 'Upload Your Files',
        
        # Dashboard
        'dashboard_title': 'Dashboard',
        'dashboard_welcome': 'Welcome Back, {}!',
        'dashboard_welcome_msg': 'Ready to revolutionize your oil palm cultivation with AI-powered insights?',
        'dashboard_tab_1': '📊 Dashboard',
        'dashboard_tab_2': '💬 Help Us Improve',
        'dashboard_reports': 'Previous Reports',
        'dashboard_no_reports': 'No Reports Found',
        'dashboard_no_reports_msg': 'Upload your first oil palm agricultural report to get started with AI analysis!',
        'dashboard_no_reports_desc': 'Our AI will analyze your soil and leaf data to provide intelligent insights.',
        'dashboard_actions': 'AI Agriculture Actions',
        'dashboard_action_analyze': 'Analyze Agricultural Reports',
        'dashboard_action_analyze_desc': 'Upload your oil palm soil and leaf test reports for AI analysis',
        'dashboard_action_results': 'View Latest Results',
        'dashboard_action_results_desc': 'Open your most recent AI analysis summary',
        'dashboard_start_analysis': '📤 Start AI Analysis',
        'dashboard_view_results': '📊 View Results',
        'dashboard_profile': 'AI Agriculture Profile',
        'dashboard_status': 'AI Agriculture System Status',
        'dashboard_status_operational': 'AI Agriculture Systems Operational',
        'dashboard_status_analysis': 'AI Analysis: Ready',
        'dashboard_status_database': 'Database: Connected',
        'dashboard_status_ocr': 'OCR: Active',
        'dashboard_status_online': 'Analysis: Online',
        'dashboard_member_since': 'AI Agriculture Member since {}',
        'dashboard_logout': '🚪 Logout',
        'dashboard_help_title': '💬 Help Us Improve AI Agriculture',
        'dashboard_help_desc': 'Your feedback helps us make our AI-powered agricultural analysis platform better for oil palm cultivation!',
        
        # Upload
        'upload_title': 'Upload SP LAB Reports',
        'upload_desc': 'Upload your soil and leaf analysis reports for comprehensive AI-powered analysis',
        'upload_select_files': 'Select files',
        'upload_file_types': 'Supported: PDF, JPG, PNG',
        'upload_analyzing': 'Analyzing...',
        'upload_success': 'Analysis complete!',
        'upload_error': 'Error during analysis',
        'upload_section_title': 'Upload SP LAB Test Reports',
        'upload_tip': 'Tip: Upload both soil and leaf analysis reports for comprehensive analysis.',
        'upload_soil_title': 'Soil Analysis',
        'upload_soil_desc': 'Upload **soil test reports** for nutrient analysis',
        'upload_soil_expected': 'Expected: Soil analysis with pH, organic carbon, available P, exchangeable cations, etc.',
        'upload_soil_file': 'Choose soil analysis file',
        'upload_soil_help': 'Upload SP LAB soil analysis reports',
        'upload_leaf_title': 'Leaf Analysis',
        'upload_leaf_desc': 'Upload **leaf test reports** for nutrient deficiency analysis',
        'upload_leaf_expected': 'Expected: Leaf analysis with nutrient content, dry matter, etc.',
        'upload_leaf_file': 'Choose leaf analysis file',
        'upload_leaf_help': 'Upload SP LAB leaf analysis reports',
        'upload_land_size': 'Land Size',
        'upload_land_unit': 'Unit',
        'upload_current_yield': 'Current Yield',
        'upload_yield_unit': 'Yield Unit',
        'upload_palm_density': 'Palm Density (per hectare)',
        'upload_start_analysis': 'Start Comprehensive Analysis',
        'upload_requirements': 'Requirements for Analysis:',
        'upload_need_soil': 'Upload a soil analysis report',
        'upload_need_leaf': 'Upload a leaf analysis report',
        'upload_need_yield': 'Provide land size and current yield data',
        'upload_uploaded_soil': 'Uploaded Soil Report',
        'upload_uploaded_leaf': 'Uploaded Leaf Report',
        'upload_restored_soil': 'Uploaded Soil Report (Restored)',
        'upload_restored_leaf': 'Uploaded Leaf Report (Restored)',
        'upload_file': 'File',
        'upload_size': 'Size',
        'upload_format': 'Format',
        'upload_type': 'Type',
        'upload_ocr_preview': 'OCR Data Processing & Preview',
        'upload_refresh_ocr': 'Refresh OCR',
        'upload_last_processed': 'Last processed',
        'upload_soil_data_summary': 'Soil Analysis Summary',
        'upload_leaf_data_summary': 'Leaf Analysis Summary',
        'upload_raw_text': 'Raw Extracted Text Data',
        'upload_structured_data': 'Structured OCR Data (JSON Format)',
        'upload_extracted_samples': 'Extracted {} samples directly from {} file',
        'upload_raw_ocr': 'Raw OCR Text (Reference Only)',
        'upload_ai_analysis_note': 'This data will be used by the AI for analysis. Each sample ID contains its parameter values:',
        'upload_land_yield_info': 'Land & Yield Information (Required)',
        'upload_land_yield_desc': 'Essential for generating accurate economic forecasts and 5-year yield projections',
        
        # Results
        'results_title': 'Analysis Results',
        'results_no_results': 'No results available',
        'results_header': 'Agricultural Analysis Results',
        'results_summary': 'Summary',
        'results_step_analysis': 'Step-by-Step Analysis',
        'results_data_analysis': 'Data Analysis',
        'results_issue_diagnosis': 'Issue Diagnosis',
        'results_solution_recommendations': 'Solution Recommendations',
        'results_regenerative_agriculture': 'Regenerative Agriculture',
        'results_economic_impact': 'Economic Impact Forecast',
        'results_yield_forecast': 'Yield Forecast & Projections',
        'results_soil_parameters': 'Soil Parameters',
        'results_leaf_parameters': 'Leaf Parameters',
        'results_key_findings': 'Key Findings',
        'results_detailed_analysis': 'Detailed Analysis',
        'results_recommendations': 'Recommendations',
        'results_download_pdf': 'Download PDF Report',
        'results_print_results': 'Print Results',
        'results_nutrient_status': 'Nutrient Status',
        'results_deficiency_detected': 'Deficiency Detected',
        'results_optimal_level': 'Optimal Level',
        'results_current_value': 'Current Value',
        'results_mpob_standard': 'MPOB Standard',
        'results_gap_analysis': 'Gap Analysis',
        'results_percent_gap': 'Percent Gap',
        'results_severity': 'Severity',
        'results_critical': 'Critical',
        'results_low': 'Low',
        'results_balanced': 'Balanced',
        'results_parameter': 'Parameter',
        'results_average': 'Average',
        'results_min': 'Minimum',
        'results_max': 'Maximum',
        'results_std_dev': 'Standard Deviation',
        'results_sample_count': 'Sample Count',
        'results_action_required': 'Action Required',
        'results_priority': 'Priority',
        'results_timeline': 'Timeline',
        'results_expected_impact': 'Expected Impact',
        'results_cost_estimate': 'Cost Estimate',
        'results_investment_scenario': 'Investment Scenario',
        'results_high_investment': 'High Investment',
        'results_medium_investment': 'Medium Investment',
        'results_low_investment': 'Low Investment',
        'results_yield_improvement': 'Yield Improvement',
        'results_revenue': 'Revenue',
        'results_net_profit': 'Net Profit',
        'results_roi': 'Return on Investment (ROI)',
        'results_year': 'Year',
        'results_analysis_date': 'Analysis Date',
        'results_report_types': 'Report Types',
        'results_status': 'Status',
        'results_analyzing': 'Analyzing Your Agricultural Reports',
        'results_processing': 'Our AI system is processing your soil and leaf analysis data. This may take a few moments...',
        'results_important': 'Important',
        'results_keep_page_open': 'Please keep this page open during analysis. The process may take 2-5 minutes.',
        'results_raw_data_title': 'Raw Extracted Data',
        'results_raw_data_desc': 'Your original laboratory test results extracted from uploaded reports',
        'results_raw_data_what': "What you'll see here:",
        'results_soil_params_desc': 'pH, organic matter, nutrients in your soil',
        'results_leaf_params_desc': 'Nutrient levels in your oil palm leaves',
        'results_sample_numbers': 'Sample Numbers',
        'results_sample_numbers_desc': 'Individual test results from different samples',
        'results_units': 'Units',
        'results_units_desc': 'Measurements in standard scientific units',
        'results_raw_data_note': 'These are the raw numbers from your lab reports that our AI extracted automatically.',
        'results_no_results_found': 'No analysis results found.',
        'results_upload_to_see': 'Upload and analyze your agricultural reports to see results here.',
        'results_analyze_files': 'Analyze Files',
        'results_dashboard': 'Dashboard',
        'results_back_home': 'Back to Home',
        'results_download_report': 'Download Report',
        'results_soil_analysis_data': 'Soil Analysis Data',
        'results_leaf_analysis_data': 'Leaf Analysis Data',
        'results_summary_statistics': 'Summary Statistics',
        'results_parameter_statistics': 'Parameter Statistics',
        'results_individual_sample_data': 'Individual Sample Data',
        'results_baseline_yield': 'Baseline Yield',
        'results_projected_yield': 'Projected Yield',
        
        # Executive Summary
        'executive_summary': 'Executive Summary',
        'executive_summary_desc': 'Key insights from your soil and leaf analysis in simple terms',
        'executive_summary_intro': 'This comprehensive agronomic analysis evaluates key nutritional parameters from both soil and leaf tissue samples to assess the current fertility status and plant health of the oil palm plantation.',
        
        # Nutrient Status Tables
        'soil_nutrient_status_title': 'Soil Nutrient Status (Average vs. MPOB Standard)',
        'leaf_nutrient_status_title': 'Leaf Nutrient Status (Average vs. MPOB Standard)',
        'nutrient_gap_analysis_title': 'Nutrient Gap Analysis',
        'parameter_analysis_matrix': 'Parameter Analysis Matrix',
        'data_quality_assessment': 'Data Quality Assessment',
        
        # Table Column Headers
        'table_parameter': 'Parameter',
        'table_average': 'Average',
        'table_mpob_standard': 'MPOB Standard',
        'table_status': 'Status',
        'table_gap': 'Gap',
        'table_severity': 'Severity',
        'table_source': 'Source',
        'table_min': 'Min',
        'table_max': 'Max',
        'table_std_dev': 'Std Dev',
        'table_optimal_range': 'Optimal Range',
        'table_unit': 'Unit',
        'table_value': 'Value',
        
        # Economic Forecast Labels
        'economic_forecast': 'Economic Forecast',
        'economic_forecast_source': 'Economic Forecast Source',
        'land_size_hectares': 'Land Size (Hectares)',
        'current_yield_tonnes_ha': 'Current Yield (Tonnes/Ha)',
        'palm_density_per_hectare': 'Palm Density Per Hectare',
        'total_palms': 'Total Palms',
        'oil_palm_price_range': 'Oil Palm Price Range (RM/Tonne)',
        'investment_scenario': 'Investment Scenario',
        'yield_improvement_tha': 'Yield Improvement (t/ha)',
        'revenue_rm_ha': 'Revenue (RM/ha)',
        'input_cost_rm_ha': 'Input Cost (RM/ha)',
        'net_profit_rm_ha': 'Net Profit (RM/ha)',
        'cumulative_net_profit': 'Cumulative Net Profit (RM/ha)',
        'roi_percent': 'ROI (%)',
        'payback_period': 'Payback Period',
        'months': 'months',
        
        # Step Titles
        'step_1_title': 'Step 1: Data Analysis',
        'step_2_title': 'Step 2: Issue Diagnosis',
        'step_3_title': 'Step 3: Solution Recommendations',
        'step_4_title': 'Step 4: Regenerative Agriculture',
        'step_5_title': 'Step 5: Economic Impact Forecast',
        'step_6_title': 'Step 6: Yield Forecast & Projections',
        
        # Yield Forecast
        'yield_forecast_title': 'Yield Forecast & Projections',
        'baseline_yield': 'Baseline Yield',
        'projected_yield': 'Projected Yield',
        'year_1': 'Year 1',
        'year_2': 'Year 2',
        'year_3': 'Year 3',
        'year_4': 'Year 4',
        'year_5': 'Year 5',
        'high_investment': 'High Investment',
        'medium_investment': 'Medium Investment',
        'low_investment': 'Low Investment',
        'tonnes_per_hectare': 'tonnes/hectare',
        'hectares': 'hectares',
        
        # Status Labels
        'status_optimal': 'Optimal',
        'status_deficient': 'Deficient',
        'status_excess': 'Excess',
        'status_critical': 'Critical',
        'status_low': 'Low',
        'status_balanced': 'Balanced',
        'status_adequate': 'Adequate',
        
        # Admin
        'admin_title': 'Admin Panel',
        'admin_restricted': 'Admin access only',
        'admin_enter_code': 'Please enter the admin code to access the admin panel.',
        'admin_no_codes': 'No admin codes configured. Please configure admin codes in Hugging Face Spaces secrets. Go to Settings > Variables and secrets, and add a secret with key `admin_codes` (alphanumeric only, no dots) and value as a JSON array like `["YOUR_ADMIN_CODE_HERE"]`.',
        'admin_access': 'Admin Access',
        'admin_code_input': 'Enter Admin Code',
        'admin_code_help': 'Enter the admin code to access the admin panel',
        'admin_access_button': 'Access Admin Panel',
        'admin_access_granted': 'Admin access granted!',
        'admin_access_denied': 'Invalid admin code. Please try again.',
        'admin_logged_in': 'Admin access granted. Using code: {}',
        'admin_logout': 'Logout',
        'admin_tab_users': 'User Management',
        'admin_tab_ai': 'AI Configuration',
        'admin_tab_feedback': 'Feedback Analytics',
        'admin_tab_config': 'System Configuration',
        'admin_error_reading': 'Error reading admin codes from secrets: {}',
        
        # Common actions
        'btn_view': 'View',
        'btn_download': 'Download',
        'btn_delete': 'Delete',
        'btn_edit': 'Edit',
        'btn_save': 'Save',
        'btn_cancel': 'Cancel',
        'btn_submit': 'Submit',
        'btn_back': 'Back',
        
        # Status messages
        'status_success': 'Success',
        'status_error': 'Error',
        'status_warning': 'Warning',
        'status_info': 'Info',
        'status_loading': 'Loading...',
        
        # Time
        'time_created': 'Created: {}',
        'time_updated': 'Updated: {}',
        
        # Reports
        'report_type': 'Type: {} Analysis',
        'report_status': 'Status: AI Analysis Complete',
        
        # System
        'system_ready': 'All Systems Operational',
        'system_status': 'System Status',
        
        # Footer
        'footer_copyright': '© 2025 CropDrive™ AI Assistant | Advanced Oil Palm Cultivation Analysis System',
        
        # PDF Report
        'pdf_title': 'Agricultural Analysis Report',
        'pdf_report_type_multiple': 'Report Type: {} Analysis',
        'pdf_report_type_single': 'Report Type: {} Analysis',
        'pdf_report_type_comprehensive': 'Report Type: Comprehensive Agricultural Analysis',
        'pdf_lab_number': 'Lab Number:',
        'pdf_sample_date': 'Sample Date:',
        'pdf_farm_name': 'Farm Name:',
        'pdf_oil_palm_plantation': 'Oil Palm Plantation',
        'pdf_report_generated': 'Report Generated:',
        'pdf_generated_by': 'Generated by CropDrive™ AI Analysis System',
        'pdf_platform_name': 'Advanced Agricultural Intelligence Platform',
        'pdf_executive_summary': 'Executive Summary',
        'pdf_executive_summary_text': 'This report provides a comprehensive analysis of your agricultural data, including soil and leaf nutrient levels, deficiency identification, and actionable recommendations for optimal crop management.',
        'pdf_parameters_analysis': 'Parameters Analysis',
        'pdf_parameters_summary': 'The following sections detail the analyzed parameters from your soil and leaf samples, compared against optimal standards for oil palm cultivation.',
        'pdf_soil_analysis': 'Soil Analysis',
        'pdf_leaf_analysis': 'Leaf Analysis',
        'pdf_recommendations': 'Recommendations',
        'pdf_step_by_step_analysis': 'Step-by-Step Analysis',
        'pdf_key_findings': 'Key Findings',
        'pdf_parameter': 'Parameter',
        'pdf_value': 'Value',
        'pdf_unit': 'Unit',
        'pdf_status': 'Status',
        'pdf_optimal_range': 'Optimal Range',
        'pdf_action_required': 'Action Required',
        'pdf_priority': 'Priority',
        'pdf_high': 'High',
        'pdf_medium': 'Medium',
        'pdf_low': 'Low',
        'pdf_deficient': 'Deficient',
        'pdf_adequate': 'Adequate',
        'pdf_excess': 'Excess',
        'pdf_download_report': 'Download PDF Report',
        'pdf_generating': 'Generating PDF report...',
        'pdf_generated_success': 'PDF report generated successfully!',
        'pdf_generated_error': 'Failed to generate PDF report',
        'pdf_no_soil_data': 'No soil nutrient data available',
        'pdf_no_leaf_data': 'No leaf nutrient data available',
        'pdf_raw_soil_data': 'Raw Soil Sample Data',
        'pdf_raw_leaf_data': 'Raw Leaf Sample Data',
    },
    'ms': {
        # Common
        'app_title': 'Pembantu AI CropDrive™',
        'app_subtitle': 'Sistem Analisis Penanaman Kelapa Sawit Lanjutan',
        'language': 'Bahasa',
        'english': 'Bahasa Inggeris',
        'malay': 'Bahasa Malaysia',
        'toggle_language': 'Tukar Bahasa',
        
        # Navigation
        'nav_home': 'Laman Utama',
        'nav_analyze': 'Analisa Fail',
        'nav_admin': 'Panel Admin',
        'nav_help_improve': '💬 Bantu Kami Meningkatkan',
        'nav_upload': 'Muat Naik',
        
        # Home Page
        'home_title': 'Selamat Datang ke Pembantu AI CropDrive™',
        'home_what_title': 'Apa yang alat ini lakukan:',
        'home_what_1': 'Membaca laporan ujian tanah dan daun anda',
        'home_what_2': 'Menganalisis data menggunakan AI',
        'home_what_3': 'Memberi cadangan pertanian kepada anda',
        'home_what_4': 'Menunjukkan ramalan hasil untuk ladang anda',
        'home_how_title': 'Cara menggunakannya:',
        'home_how_1': 'Muat naik laporan ujian anda',
        'home_how_2': 'Masukkan butiran ladang anda',
        'home_how_3': 'Dapatkan hasil analisis anda',
        'home_how_4': 'Semak cadangan dan pandangan',
        'home_ready': 'Bersedia untuk bermula?',
        'home_ready_desc': 'Muat naik laporan ujian kelapa sawit anda dan dapatkan nasihat pertanian yang berguna.',
        'home_start': 'Mula Analisis',
        'home_upload_files': 'Muat Naik Fail Anda',
        
        # Dashboard
        'dashboard_title': 'Papan Pemuka',
        'dashboard_welcome': 'Selamat Datang Semula, {}!',
        'dashboard_welcome_msg': 'Bersedia untuk merevolusikan penanaman kelapa sawit anda dengan pandangan yang dikuasakan oleh AI?',
        'dashboard_tab_1': '📊 Papan Pemuka',
        'dashboard_tab_2': '💬 Bantu Kami Meningkatkan',
        'dashboard_reports': 'Laporan Terdahulu',
        'dashboard_no_reports': 'Tiada Laporan Dijumpai',
        'dashboard_no_reports_msg': 'Muat naik laporan pertanian kelapa sawit pertama anda untuk bermula dengan analisis AI!',
        'dashboard_no_reports_desc': 'AI kami akan menganalisis data tanah dan daun anda untuk memberikan pandangan yang bijak.',
        'dashboard_actions': 'Tindakan Pertanian AI',
        'dashboard_action_analyze': 'Analisa Laporan Pertanian',
        'dashboard_action_analyze_desc': 'Muat naik laporan ujian tanah dan daun kelapa sawit anda untuk analisis AI',
        'dashboard_action_results': 'Lihat Hasil Terkini',
        'dashboard_action_results_desc': 'Buka ringkasan analisis AI terkini anda',
        'dashboard_start_analysis': '📤 Mula Analisis AI',
        'dashboard_view_results': '📊 Lihat Hasil',
        'dashboard_profile': 'Profil Pertanian AI',
        'dashboard_status': 'Status Sistem Pertanian AI',
        'dashboard_status_operational': 'Sistem Pertanian AI Beroperasi',
        'dashboard_status_analysis': 'Analisis AI: Sedia',
        'dashboard_status_database': 'Pangkalan Data: Bersambung',
        'dashboard_status_ocr': 'OCR: Aktif',
        'dashboard_status_online': 'Analisis: Dalam Talian',
        'dashboard_member_since': 'Ahli Pertanian AI sejak {}',
        'dashboard_logout': '🚪 Log Keluar',
        'dashboard_help_title': '💬 Bantu Kami Meningkatkan Pertanian AI',
        'dashboard_help_desc': 'Maklum balas anda membantu kami menjadikan platform analisis pertanian yang dikuasakan oleh AI lebih baik untuk penanaman kelapa sawit!',
        
        # Upload
        'upload_title': 'Muat Naik Laporan SP LAB',
        'upload_desc': 'Muat naik laporan analisis tanah dan daun anda untuk analisis komprehensif yang dikuasakan oleh AI',
        'upload_select_files': 'Pilih fail',
        'upload_file_types': 'Disokong: PDF, JPG, PNG',
        'upload_analyzing': 'Menganalisis...',
        'upload_success': 'Analisis selesai!',
        'upload_error': 'Ralat semasa analisis',
        'upload_section_title': 'Muat Naik Laporan Ujian SP LAB',
        'upload_tip': 'Tip: Muat naik kedua-dua laporan analisis tanah dan daun untuk analisis komprehensif.',
        'upload_soil_title': 'Analisis Tanah',
        'upload_soil_desc': 'Muat naik **laporan ujian tanah** untuk analisis nutrien',
        'upload_soil_expected': 'Dijangka: Analisis tanah dengan pH, karbon organik, P tersedia, kation boleh tukar, dll.',
        'upload_soil_file': 'Pilih fail analisis tanah',
        'upload_soil_help': 'Muat naik laporan analisis tanah SP LAB',
        'upload_leaf_title': 'Analisis Daun',
        'upload_leaf_desc': 'Muat naik **laporan ujian daun** untuk analisis kekurangan nutrien',
        'upload_leaf_expected': 'Dijangka: Analisis daun dengan kandungan nutrien, bahan kering, dll.',
        'upload_leaf_file': 'Pilih fail analisis daun',
        'upload_leaf_help': 'Muat naik laporan analisis daun SP LAB',
        'upload_land_size': 'Saiz Tanah',
        'upload_land_unit': 'Unit',
        'upload_current_yield': 'Hasil Semasa',
        'upload_yield_unit': 'Unit Hasil',
        'upload_palm_density': 'Ketumpatan Kelapa Sawit (per hektar)',
        'upload_start_analysis': 'Mula Analisis Komprehensif',
        'upload_requirements': 'Keperluan untuk Analisis:',
        'upload_need_soil': 'Muat naik laporan analisis tanah',
        'upload_need_leaf': 'Muat naik laporan analisis daun',
        'upload_need_yield': 'Berikan data saiz tanah dan hasil semasa',
        'upload_uploaded_soil': 'Laporan Tanah Dimuat Naik',
        'upload_uploaded_leaf': 'Laporan Daun Dimuat Naik',
        'upload_restored_soil': 'Laporan Tanah Dimuat Naik (Dipulihkan)',
        'upload_restored_leaf': 'Laporan Daun Dimuat Naik (Dipulihkan)',
        'upload_file': 'Fail',
        'upload_size': 'Saiz',
        'upload_format': 'Format',
        'upload_type': 'Jenis',
        'upload_ocr_preview': 'Pemprosesan & Pratonton Data OCR',
        'upload_refresh_ocr': 'Segar Semula OCR',
        'upload_soil_data_summary': 'Ringkasan Analisis Tanah',
        'upload_leaf_data_summary': 'Ringkasan Analisis Daun',
        'upload_raw_text': 'Data Teks Mentah yang Diekstrak',
        'upload_structured_data': 'Data OCR Berstruktur (Format JSON)',
        'upload_extracted_samples': 'Diekstrak {} sampel terus dari fail {}',
        'upload_raw_ocr': 'Teks OCR Mentah (Rujukan Sahaja)',
        
        # Results
        'results_title': 'Hasil Analisis',
        'results_no_results': 'Tiada hasil tersedia',
        'results_header': 'Hasil Analisis Pertanian',
        'results_summary': 'Ringkasan',
        'results_step_analysis': 'Analisis Langkah Demi Langkah',
        'results_data_analysis': 'Analisis Data',
        'results_issue_diagnosis': 'Diagnosis Isu',
        'results_solution_recommendations': 'Cadangan Penyelesaian',
        'results_regenerative_agriculture': 'Pertanian Regeneratif',
        'results_economic_impact': 'Ramalan Kesan Ekonomi',
        'results_yield_forecast': 'Ramalan & Unjuran Hasil',
        'results_soil_parameters': 'Parameter Tanah',
        'results_leaf_parameters': 'Parameter Daun',
        'results_key_findings': 'Penemuan Utama',
        'results_detailed_analysis': 'Analisis Terperinci',
        'results_recommendations': 'Cadangan',
        'results_download_pdf': 'Muat Turun Laporan PDF',
        'results_print_results': 'Cetak Hasil',
        'results_nutrient_status': 'Status Nutrien',
        'results_deficiency_detected': 'Kekurangan Dikesan',
        'results_optimal_level': 'Tahap Optimum',
        'results_current_value': 'Nilai Semasa',
        'results_mpob_standard': 'Standard MPOB',
        'results_gap_analysis': 'Analisis Jurang',
        'results_percent_gap': 'Peratusan Jurang',
        'results_severity': 'Keterukan',
        'results_critical': 'Kritikal',
        'results_low': 'Rendah',
        'results_balanced': 'Seimbang',
        'results_parameter': 'Parameter',
        'results_average': 'Purata',
        'results_min': 'Minimum',
        'results_max': 'Maksimum',
        'results_std_dev': 'Sisihan Piawai',
        'results_sample_count': 'Bilangan Sampel',
        'results_action_required': 'Tindakan Diperlukan',
        'results_priority': 'Keutamaan',
        'results_timeline': 'Jangka Masa',
        'results_expected_impact': 'Kesan Dijangka',
        'results_cost_estimate': 'Anggaran Kos',
        'results_investment_scenario': 'Senario Pelaburan',
        'results_high_investment': 'Pelaburan Tinggi',
        'results_medium_investment': 'Pelaburan Sederhana',
        'results_low_investment': 'Pelaburan Rendah',
        'results_yield_improvement': 'Peningkatan Hasil',
        'results_revenue': 'Pendapatan',
        'results_net_profit': 'Untung Bersih',
        'results_roi': 'Pulangan Pelaburan (ROI)',
        'results_year': 'Tahun',
        'results_baseline_yield': 'Hasil Asas',
        'results_analysis_date': 'Tarikh Analisis',
        'results_report_types': 'Jenis Laporan',
        'results_status': 'Status',
        'results_analyzing': 'Menganalisis Laporan Pertanian Anda',
        'results_processing': 'Sistem AI kami sedang memproses data analisis tanah dan daun anda. Ini mungkin mengambil masa beberapa minit...',
        'results_important': 'Penting',
        'results_keep_page_open': 'Sila biarkan halaman ini terbuka semasa analisis. Proses ini mungkin mengambil masa 2-5 minit.',
        'results_raw_data_title': 'Data Mentah yang Diekstrak',
        'results_raw_data_desc': 'Keputusan ujian makmal asal anda yang diekstrak dari laporan yang dimuat naik',
        'results_raw_data_what': 'Apa yang anda akan lihat di sini:',
        'results_soil_params_desc': 'pH, bahan organik, nutrien dalam tanah anda',
        'results_leaf_params_desc': 'Tahap nutrien dalam daun kelapa sawit anda',
        'results_sample_numbers': 'Nombor Sampel',
        'results_sample_numbers_desc': 'Keputusan ujian individu dari sampel yang berbeza',
        'results_units': 'Unit',
        'results_units_desc': 'Ukuran dalam unit saintifik standard',
        'results_raw_data_note': 'Ini adalah nombor mentah dari laporan makmal anda yang diekstrak oleh AI kami secara automatik.',
        'results_no_results_found': 'Tiada hasil analisis ditemui.',
        'results_upload_to_see': 'Muat naik dan analisis laporan pertanian anda untuk melihat hasil di sini.',
        'results_analyze_files': 'Analisis Fail',
        'results_dashboard': 'Papan Pemuka',
        'results_back_home': 'Kembali ke Laman Utama',
        'results_download_report': 'Muat Turun Laporan',
        'results_soil_analysis_data': 'Data Analisis Tanah',
        'results_leaf_analysis_data': 'Data Analisis Daun',
        'results_summary_statistics': 'Statistik Ringkasan',
        'results_parameter_statistics': 'Statistik Parameter',
        'results_individual_sample_data': 'Data Sampel Individu',
        'results_data_format_error': 'Ralat format data hasil analisis',
        'results_projected_yield': 'Hasil Unjuran',
        
        # Executive Summary (Malaysian)
        'executive_summary': 'Ringkasan Eksekutif',
        'executive_summary_desc': 'Pandangan utama daripada analisis tanah dan daun anda dalam istilah mudah',
        'executive_summary_intro': 'Analisis agronomi komprehensif ini menilai parameter pemakanan utama daripada sampel tisu tanah dan daun untuk menilai status kesuburan semasa dan kesihatan tumbuhan ladang kelapa sawit.',
        
        # Nutrient Status Tables (Malaysian)
        'soil_nutrient_status_title': 'Status Nutrien Tanah (Purata vs. Standard MPOB)',
        'leaf_nutrient_status_title': 'Status Nutrien Daun (Purata vs. Standard MPOB)',
        'nutrient_gap_analysis_title': 'Analisis Jurang Nutrien',
        'parameter_analysis_matrix': 'Matriks Analisis Parameter',
        'data_quality_assessment': 'Penilaian Kualiti Data',
        
        # Table Column Headers (Malaysian)
        'table_parameter': 'Parameter',
        'table_average': 'Purata',
        'table_mpob_standard': 'Standard MPOB',
        'table_status': 'Status',
        'table_gap': 'Jurang',
        'table_severity': 'Keterukan',
        'table_source': 'Sumber',
        'table_min': 'Min',
        'table_max': 'Maks',
        'table_std_dev': 'Sisihan Piawai',
        'table_optimal_range': 'Julat Optimum',
        'table_unit': 'Unit',
        'table_value': 'Nilai',
        
        # Economic Forecast Labels (Malaysian)
        'economic_forecast': 'Ramalan Ekonomi',
        'economic_forecast_source': 'Sumber Ramalan Ekonomi',
        'land_size_hectares': 'Saiz Tanah (Hektar)',
        'current_yield_tonnes_ha': 'Hasil Semasa (Tan/Ha)',
        'palm_density_per_hectare': 'Kepadatan Sawit Per Hektar',
        'total_palms': 'Jumlah Pokok Sawit',
        'oil_palm_price_range': 'Julat Harga Kelapa Sawit (RM/Tan)',
        'investment_scenario': 'Senario Pelaburan',
        'yield_improvement_tha': 'Peningkatan Hasil (t/ha)',
        'revenue_rm_ha': 'Pendapatan (RM/ha)',
        'input_cost_rm_ha': 'Kos Input (RM/ha)',
        'net_profit_rm_ha': 'Untung Bersih (RM/ha)',
        'cumulative_net_profit': 'Untung Bersih Kumulatif (RM/ha)',
        'roi_percent': 'ROI (%)',
        'payback_period': 'Tempoh Pulangan Modal',
        'months': 'bulan',
        
        # Step Titles (Malaysian)
        'step_1_title': 'Langkah 1: Analisis Data',
        'step_2_title': 'Langkah 2: Diagnosis Isu',
        'step_3_title': 'Langkah 3: Cadangan Penyelesaian',
        'step_4_title': 'Langkah 4: Pertanian Regeneratif',
        'step_5_title': 'Langkah 5: Ramalan Kesan Ekonomi',
        'step_6_title': 'Langkah 6: Ramalan & Unjuran Hasil',
        
        # Yield Forecast (Malaysian)
        'yield_forecast_title': 'Ramalan & Unjuran Hasil',
        'baseline_yield': 'Hasil Asas',
        'projected_yield': 'Hasil Unjuran',
        'year_1': 'Tahun 1',
        'year_2': 'Tahun 2',
        'year_3': 'Tahun 3',
        'year_4': 'Tahun 4',
        'year_5': 'Tahun 5',
        'high_investment': 'Pelaburan Tinggi',
        'medium_investment': 'Pelaburan Sederhana',
        'low_investment': 'Pelaburan Rendah',
        'tonnes_per_hectare': 'tan/hektar',
        'hectares': 'hektar',
        
        # Status Labels (Malaysian)
        'status_optimal': 'Optimum',
        'status_deficient': 'Kekurangan',
        'status_excess': 'Berlebihan',
        'status_critical': 'Kritikal',
        'status_low': 'Rendah',
        'status_balanced': 'Seimbang',
        'status_adequate': 'Mencukupi',
        
        # Admin
        'admin_title': 'Panel Admin',
        'admin_restricted': 'Akses admin sahaja',
        'admin_enter_code': 'Sila masukkan kod admin untuk mengakses panel admin.',
        'admin_no_codes': 'Tiada kod admin dikonfigurasi. Sila konfigurasi kod admin dalam Hugging Face Spaces secrets. Pergi ke Settings > Variables and secrets, dan tambah secret dengan kunci `admin_codes` (hanya alfanumerik, tiada titik) dan nilai sebagai array JSON seperti `["YOUR_ADMIN_CODE_HERE"]`.',
        'admin_access': 'Akses Admin',
        'admin_code_input': 'Masukkan Kod Admin',
        'admin_code_help': 'Masukkan kod admin untuk mengakses panel admin',
        'admin_access_button': 'Akses Panel Admin',
        'admin_access_granted': 'Akses admin diberikan!',
        'admin_access_denied': 'Kod admin tidak sah. Sila cuba lagi.',
        'admin_logged_in': 'Akses admin diberikan. Menggunakan kod: {}',
        'admin_logout': 'Log Keluar',
        'admin_tab_users': 'Pengurusan Pengguna',
        'admin_tab_ai': 'Konfigurasi AI',
        'admin_tab_feedback': 'Analitik Maklum Balas',
        'admin_tab_config': 'Konfigurasi Sistem',
        'admin_error_reading': 'Ralat membaca kod admin dari rahsia: {}',
        
        # Common actions
        'btn_view': 'Lihat',
        'btn_download': 'Muat Turun',
        'btn_delete': 'Padam',
        'btn_edit': 'Edit',
        'btn_save': 'Simpan',
        'btn_cancel': 'Batal',
        'btn_submit': 'Hantar',
        'btn_back': 'Kembali',
        
        # Status messages
        'status_success': 'Berjaya',
        'status_error': 'Ralat',
        'status_warning': 'Amaran',
        'status_info': 'Maklumat',
        'status_loading': 'Memuatkan...',
        
        # Time
        'time_created': 'Dicipta: {}',
        'time_updated': 'Dikemaskini: {}',
        
        # Reports
        'report_type': 'Jenis: Analisis {}',
        'report_status': 'Status: Analisis AI Selesai',
        
        # System
        'system_ready': 'Semua Sistem Beroperasi',
        'system_status': 'Status Sistem',
        
        # Footer
        'footer_copyright': '© 2025 Pembantu AI CropDrive™ | Sistem Analisis Penanaman Kelapa Sawit Lanjutan',
        
        # PDF Report (Malaysian)
        'pdf_title': 'Laporan Analisis Pertanian',
        'pdf_report_type_multiple': 'Jenis Laporan: Analisis {}',
        'pdf_report_type_single': 'Jenis Laporan: Analisis {}',
        'pdf_report_type_comprehensive': 'Jenis Laporan: Analisis Pertanian Komprehensif',
        'pdf_lab_number': 'Nombor Makmal:',
        'pdf_sample_date': 'Tarikh Sampel:',
        'pdf_farm_name': 'Nama Ladang:',
        'pdf_oil_palm_plantation': 'Ladang Kelapa Sawit',
        'pdf_report_generated': 'Laporan Dijana:',
        'pdf_generated_by': 'Dijana oleh Sistem Analisis AI CropDrive™',
        'pdf_platform_name': 'Platform Kecerdasan Pertanian Lanjutan',
        'pdf_executive_summary': 'Ringkasan Eksekutif',
        'pdf_executive_summary_text': 'Laporan ini menyediakan analisis komprehensif data pertanian anda, termasuk tahap nutrien tanah dan daun, pengenalan kekurangan, dan cadangan yang boleh diambil untuk pengurusan tanaman yang optimum.',
        'pdf_parameters_analysis': 'Analisis Parameter',
        'pdf_parameters_summary': 'Bahagian berikut memperincikan parameter yang dianalisis daripada sampel tanah dan daun anda, dibandingkan dengan piawaian optimum untuk penanaman kelapa sawit.',
        'pdf_soil_analysis': 'Analisis Tanah',
        'pdf_leaf_analysis': 'Analisis Daun',
        'pdf_recommendations': 'Cadangan',
        'pdf_step_by_step_analysis': 'Analisis Langkah Demi Langkah',
        'pdf_key_findings': 'Penemuan Utama',
        'pdf_parameter': 'Parameter',
        'pdf_value': 'Nilai',
        'pdf_unit': 'Unit',
        'pdf_status': 'Status',
        'pdf_optimal_range': 'Julat Optimum',
        'pdf_action_required': 'Tindakan Diperlukan',
        'pdf_priority': 'Keutamaan',
        'pdf_high': 'Tinggi',
        'pdf_medium': 'Sederhana',
        'pdf_low': 'Rendah',
        'pdf_deficient': 'Kekurangan',
        'pdf_adequate': 'Mencukupi',
        'pdf_excess': 'Berlebihan',
        'pdf_download_report': 'Muat Turun Laporan PDF',
        'pdf_generating': 'Menjana laporan PDF...',
        'pdf_generated_success': 'Laporan PDF berjaya dijana!',
        'pdf_generated_error': 'Gagal menjana laporan PDF',
        'pdf_no_soil_data': 'Tiada data nutrien tanah tersedia',
        'pdf_no_leaf_data': 'Tiada data nutrien daun tersedia',
        'pdf_raw_soil_data': 'Data Sampel Tanah Mentah',
        'pdf_raw_leaf_data': 'Data Sampel Daun Mentah',
        'pdf_executive_summary_error': 'Ringkasan eksekutif tidak dapat dihasilkan dari data analisis.',
        'pdf_data_analysis_results': 'Hasil Analisis Data',
        'pdf_soil_leaf_test_results': 'Keputusan Ujian Tanah dan Daun Anda',
        'pdf_table_summary_desc': 'Jadual ini meringkaskan nilai purata dari semua sampel yang dikumpulkan.',
        'pdf_nutrient_gap_analysis_title': 'Analisis Jurang Nutrien: Diperhatikan vs. Ambang Minimum Malaysia',
        'pdf_nutrient_gap_desc': 'Jadual ini mengutamakan kekurangan nutrien mengikut magnitud jurang mereka berbanding standard MPOB, menyerlahkan kawasan yang paling kritikal untuk campur tangan.',
        'pdf_nutrient_ratio_analysis': 'Analisis Nisbah Nutrien',
        'pdf_nutrient_ratio_desc': 'Nisbah nutrien adalah penting kerana ia menunjukkan keseimbangan dan persaingan yang berpotensi antara nutrien untuk pengambilan oleh pokok kelapa sawit.',
        'pdf_parameter_quick_guide': 'Panduan Pantas Parameter untuk Nutrien yang Kekurangan',
        'pdf_interpretations': 'Tafsiran',
        'pdf_interpretation': 'Tafsiran',
        'pdf_exec_critical_soil_ph': 'ANALISIS pH TANAH KRITIKAL',
        'pdf_exec_soil_nutrient_status': 'PENILAIAN STATUS NUTRIEN TANAH',
        'pdf_exec_leaf_tissue_analysis': 'ANALISIS NUTRIEN TISU DAUN',
        'pdf_exec_yield_economic': 'ANALISIS KESAN HASIL & EKONOMI',
        'pdf_exec_recommendations_monitoring': 'CADANGAN & PEMANTAUAN',
        'pdf_exec_top_limiting_factors': '3 FAKTOR PEMBATASAN TERATAS',
        'pdf_exec_recommended_solutions': 'CADANGAN & CAMPUR TANGAN YANG DISYORKAN',
        'pdf_exec_economic_roi': 'ANALISIS KESAN EKONOMI & ROI',
        'pdf_exec_5_year_yield': 'UNJURAN HASIL 5 TAHUN',
        'pdf_exec_regenerative': 'PERTANIAN REGENERATIF & KELESTARIAN',
        'pdf_exec_conclusion': 'KESIMPULAN & PELAN PELAKSANAAN',
        'table_mpob_optimal': 'Optimum MPOB',
        'table_unit': 'Unit',
        'table_average_value': 'Nilai Purata',
        'table_type': 'Jenis',
        'table_nutrient': 'Nutrien',
        'table_mpob_standard_min': 'Standard MPOB (Min)',
        'table_absolute_gap': 'Jurang Mutlak',
        'table_percent_gap': 'Peratus Jurang',
        'table_ratio': 'Nisbah',
        'table_optimal_range': 'Julat Optimum',
        'table_function': 'Fungsi',
        'table_correction_action': 'Tindakan Pembetulan',
    }
}

def get_language() -> str:
    """Get current language from session state, default to 'en' (English)"""
    # Always check URL params first (for dynamic updates)
    try:
        query_params = st.query_params
        url_lang = query_params.get('lang', '')
        if url_lang in ['en', 'ms']:
            st.session_state.language = url_lang
            return url_lang
    except Exception:
        pass
    
    # Fallback to session state
    if 'language' not in st.session_state:
        st.session_state.language = 'en'  # Default to English for clarity
    return st.session_state.language

def set_language(lang: str):
    """Set current language and notify parent window if embedded"""
    if lang in ['en', 'ms']:
        st.session_state.language = lang
        # Try to notify parent window if CropDrive integration is available
        try:
            from utils.cropdrive_integration import send_language_change
            send_language_change(lang)
        except (ImportError, Exception):
            # If integration not available, just update session state
            pass

def toggle_language():
    """Toggle between English and Malaysian"""
    current = get_language()
    new_lang = 'en' if current == 'ms' else 'ms'
    set_language(new_lang)
    # Language change will be handled by set_language() which calls send_language_change()
    
    # If CropDrive integration is available, notify parent window
    try:
        from utils.cropdrive_integration import send_language_change
        send_language_change(new_lang)
    except (ImportError, AttributeError):
        # CropDrive integration not available or function doesn't exist
        pass

def translate(key: str, default: str = None, **kwargs) -> str:
    """
    Translate a key to the current language
    
    Args:
        key: Translation key
        default: Default value if key not found
        **kwargs: Format parameters for string formatting
        
    Returns:
        Translated string
    """
    lang = get_language()
    translations = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    text = translations.get(key, default or key)
    
    # Format string if kwargs provided
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, ValueError):
            pass  # Return as-is if formatting fails
    
    return text

def t(key: str, default: str = None, **kwargs) -> str:
    """Short alias for translate"""
    return translate(key, default, **kwargs)
