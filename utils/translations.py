"""
Translation system for AGS AI Assistant
Supports English and Bahasa Malaysia (Malaysian)
"""

import streamlit as st
from typing import Dict, Any

# Translation dictionaries
TRANSLATIONS = {
    'en': {
        # Common
        'app_title': 'AGS AI Assistant',
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
        'home_title': 'Welcome to AGS AI Assistant',
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
        'upload_soil_data_summary': 'Soil Analysis Summary',
        'upload_leaf_data_summary': 'Leaf Analysis Summary',
        'upload_raw_text': 'Raw Extracted Text Data',
        'upload_structured_data': 'Structured OCR Data (JSON Format)',
        'upload_extracted_samples': 'Extracted {} samples directly from {} file',
        'upload_raw_ocr': 'Raw OCR Text (Reference Only)',
        
        # Results
        'results_title': 'Analysis Results',
        'results_no_results': 'No results available',
        
        
        # Admin
        'admin_title': 'Admin Panel',
        'admin_restricted': 'Admin access only',
        'admin_enter_code': 'Please enter the admin code to access the admin panel.',
        'admin_no_codes': 'No admin codes configured. Please configure admin codes in `.streamlit/secrets.toml` under `[admin]` section.',
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
        'footer_copyright': '© 2025 AGS AI Assistant | Advanced Oil Palm Cultivation Analysis System',
    },
    'ms': {
        # Common
        'app_title': 'Pembantu AI AGS',
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
        'home_title': 'Selamat Datang ke Pembantu AI AGS',
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
        
        
        # Admin
        'admin_title': 'Panel Admin',
        'admin_restricted': 'Akses admin sahaja',
        'admin_enter_code': 'Sila masukkan kod admin untuk mengakses panel admin.',
        'admin_no_codes': 'Tiada kod admin dikonfigurasi. Sila konfigurasi kod admin dalam `.streamlit/secrets.toml` di bawah bahagian `[admin]`.',
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
        'footer_copyright': '© 2025 Pembantu AI AGS | Sistem Analisis Penanaman Kelapa Sawit Lanjutan',
    }
}

def get_language() -> str:
    """Get current language from session state, default to 'en' (English)"""
    if 'language' not in st.session_state:
        st.session_state.language = 'en'  # Default to English for clarity
    return st.session_state.language

def set_language(lang: str):
    """Set current language"""
    if lang in ['en', 'ms']:
        st.session_state.language = lang

def toggle_language():
    """Toggle between English and Malaysian"""
    current = get_language()
    new_lang = 'en' if current == 'ms' else 'ms'
    set_language(new_lang)
    
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

