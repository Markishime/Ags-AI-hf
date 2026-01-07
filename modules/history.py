import streamlit as st
import sys
import os
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add utils to path
sys.path.append(os.path.join(
    os.path.dirname(os.path.dirname(__file__)), 'utils'))

# Import utilities
from utils.translations import t, get_language
from utils.cropdrive_integration import inject_parent_communication

# Import results display functions
try:
    from modules.results import (
        display_results_header,
        display_raw_data_section,
        display_summary_section,
        display_step_by_step_results,
        get_analysis_results_from_data,
        display_comprehensive_data_tables,
        generate_results_pdf,
        add_responsive_css
    )
except ImportError as e:
    logger.error(f"Failed to import results display functions: {e}")
    # Fallback functions
    def display_results_header(*args, **kwargs):
        pass
    def display_raw_data_section(*args, **kwargs):
        pass
    def display_summary_section(*args, **kwargs):
        pass
    def display_step_by_step_results(*args, **kwargs):
        pass
    def get_analysis_results_from_data(*args, **kwargs):
        return {}
    def display_comprehensive_data_tables(*args, **kwargs):
        pass
    def generate_results_pdf(*args, **kwargs):
        return b""
    def add_responsive_css():
        pass


def inject_load_analysis_listener():
    """
    Inject JavaScript to listen for LOAD_ANALYSIS messages from parent window (CropDrive website)
    Stores analysisId and analysisData in sessionStorage, then updates URL and reloads
    """
    js_code = """
    <script>
    (function() {
        // Listen for LOAD_ANALYSIS messages from parent window
        window.addEventListener('message', function(event) {
            // Verify origin for security
            const allowedOrigins = [
                'https://cropdrive.ai',
                'https://www.cropdrive.ai',
                'https://cropdrive-f5exleg55-mark-lloyd-cuizons-projects.vercel.app',
                'http://localhost:3000'  // For local development
            ];
            
            // Check origin (allow same origin always)
            if (event.origin !== window.location.origin && !allowedOrigins.includes(event.origin)) {
                console.warn('⚠️ Message from unauthorized origin:', event.origin);
                return;
            }
            
            const data = event.data;
            
            if (data && data.type === 'LOAD_ANALYSIS') {
                console.log('📥 Received LOAD_ANALYSIS message:', data);
                
                const analysisId = data.analysisId;
                const analysisData = data.analysisData;
                
                if (!analysisId && !analysisData) {
                    console.error('❌ LOAD_ANALYSIS message missing both analysisId and analysisData');
                    return;
                }
                
                try {
                    // Store analysisId in sessionStorage
                    if (analysisId) {
                        sessionStorage.setItem('load_analysis_id', analysisId);
                        console.log('✅ Stored analysisId in sessionStorage:', analysisId);
                    }
                    
                    // Store full analysisData in sessionStorage
                    if (analysisData) {
                        sessionStorage.setItem('load_analysis_data', JSON.stringify(analysisData));
                        console.log('✅ Stored analysisData in sessionStorage');
                    }
                    
                    // Update URL with analysisId parameter
                    const url = new URL(window.location.href);
                    const finalAnalysisId = analysisId || (analysisData && analysisData.id);
                    if (finalAnalysisId) {
                        url.searchParams.set('analysisId', finalAnalysisId);
                    }
                    
                    // If analysisData is provided, encode it as base64 JSON in URL
                    // This allows Streamlit to read it directly
                    if (analysisData) {
                        try {
                            const encodedData = btoa(JSON.stringify(analysisData));
                            url.searchParams.set('analysisData', encodedData);
                            console.log('✅ Encoded analysisData in URL');
                        } catch (encodeError) {
                            console.warn('⚠️ Could not encode analysisData:', encodeError);
                        }
                    }
                    
                    window.history.replaceState({}, '', url);
                    
                    // Trigger Streamlit rerun to load the analysis
                    console.log('🔄 Reloading page to display analysis...');
                    window.location.reload();
                } catch (e) {
                    console.error('❌ Error handling LOAD_ANALYSIS message:', e);
                }
            }
        });
        
        // Also check for analysisId in URL on page load
        window.addEventListener('load', function() {
            const urlParams = new URLSearchParams(window.location.search);
            const analysisId = urlParams.get('analysisId');
            if (analysisId) {
                console.log('📋 Found analysisId in URL:', analysisId);
                // Store in sessionStorage for consistency
                sessionStorage.setItem('load_analysis_id', analysisId);
            }
        });
    })();
    </script>
    """
    
    st.components.v1.html(js_code, height=0)


def load_analysis_from_url():
    """
    Load analysis data from URL query parameters
    Returns the analysis data dict or None
    """
    query_params = st.query_params
    analysis_id = query_params.get('analysisId', None)
    analysis_data_encoded = query_params.get('analysisData', None)
    
    # If analysisData is encoded in URL, decode it
    if analysis_data_encoded:
        try:
            import base64
            analysis_data_json = base64.b64decode(analysis_data_encoded).decode('utf-8')
            analysis_data = json.loads(analysis_data_json)
            logger.info("✅ Loaded analysisData from URL")
            return analysis_data
        except Exception as e:
            logger.error(f"❌ Error decoding analysisData from URL: {e}")
    
    return None


def fetch_analysis_from_firestore(analysis_id):
    """
    Fetch analysis data from Firestore by document ID
    This is a fallback if analysisData wasn't provided via postMessage
    """
    try:
        from utils.firebase_config import get_firestore_client, COLLECTIONS
        
        db = get_firestore_client()
        if not db:
            logger.error("Firestore client not available")
            return None
        
        # Fetch document from analysis_results collection
        doc_ref = db.collection(COLLECTIONS['analysis_results']).document(analysis_id)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            data['id'] = doc.id
            data['success'] = True
            logger.info(f"✅ Successfully fetched analysis {analysis_id} from Firestore")
            return data
        else:
            logger.warning(f"⚠️ Analysis {analysis_id} not found in Firestore")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching analysis {analysis_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def show_history_page():
    """
    Main history page that displays analysis results loaded from CropDrive website
    Handles both postMessage (LOAD_ANALYSIS) and URL-based (analysisId) loading
    """
    # Inject parent communication and LOAD_ANALYSIS listener
    inject_parent_communication()
    inject_load_analysis_listener()
    
    # Add responsive CSS styling
    add_responsive_css()
    
    # Page header
    st.markdown(f'<h1 class="main-title" style="text-align: center;">📋 {t("results_title", "Analysis Results")}</h1>', unsafe_allow_html=True)
    
    # Get analysisId from query parameters
    query_params = st.query_params
    analysis_id = query_params.get('analysisId', None)
    
    # Check if analysis data was stored in session state from previous load
    if 'loaded_analysis_data' in st.session_state and st.session_state.loaded_analysis_data:
        analysis_data = st.session_state.loaded_analysis_data
    else:
        # Try to load from URL (encoded by JavaScript from postMessage)
        analysis_data = load_analysis_from_url()
        
        # If not in URL, try to fetch from Firestore using analysisId
        if not analysis_data and analysis_id:
            with st.spinner(f"🔄 {t('results_loading', 'Loading analysis...')}"):
                analysis_data = fetch_analysis_from_firestore(analysis_id)
        
        # Store in session state for future use
        if analysis_data:
            st.session_state.loaded_analysis_data = analysis_data
    
    # If still no data, check if we need to wait for postMessage
    if not analysis_data and not analysis_id:
        st.info("📋 Waiting for analysis data from CropDrive website...")
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 2rem; border-radius: 10px; margin: 2rem 0; border-left: 4px solid #2E8B57;">
            <h3 style="color: #2E8B57; margin-top: 0;">How to view an analysis:</h3>
            <ol style="color: #333; line-height: 1.8;">
                <li>Go to the CropDrive website</li>
                <li>Navigate to your analysis history</li>
                <li>Click "Open Recent Analysis" on any report</li>
                <li>The analysis will load automatically here</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
        
        # Show button to go back to home
        if st.button("🏠 Back to Home", type="primary"):
            st.session_state.current_page = 'home'
            st.rerun()
        return
    
    if not analysis_data:
        st.error(f"❌ {t('results_not_found', 'Analysis not found. The analysis ID may be invalid or the analysis may have been deleted.')}")
        st.info(f"💡 {t('results_check_id', 'Please verify the analysis ID and try again.')}")
        
        # Show button to go back to home
        if st.button("🏠 Back to Home", type="primary"):
            st.session_state.current_page = 'home'
            st.rerun()
        return
    
    # Store in session state for display functions
    st.session_state.current_analysis = analysis_data
    st.session_state.loaded_analysis_data = analysis_data
    
    # Ensure success flag is set
    if 'success' not in analysis_data:
        analysis_data['success'] = True
    
    # Display results using the same functions as results page
    # This ensures identical formatting to the original reports page
    st.markdown('<div class="print-show">', unsafe_allow_html=True)
    
    # Display header with metadata
    display_results_header(analysis_data)
    
    # Display raw data section
    display_raw_data_section(analysis_data)
    
    # Display comprehensive data tables
    analysis_results = get_analysis_results_from_data(analysis_data)
    if analysis_results:
        # Try multiple data sources for soil and leaf parameters
        soil_params = None
        leaf_params = None
        
        # Check raw_data for soil_parameters and leaf_parameters
        if 'raw_data' in analysis_results:
            soil_params = analysis_results['raw_data'].get('soil_parameters')
            leaf_params = analysis_results['raw_data'].get('leaf_parameters')
        
        # Check analysis_results directly
        if not soil_params and 'soil_parameters' in analysis_results:
            soil_params = analysis_results['soil_parameters']
        if not leaf_params and 'leaf_parameters' in analysis_results:
            leaf_params = analysis_results['leaf_parameters']
        
        # Check if we have structured OCR data that needs conversion
        if not soil_params and 'raw_ocr_data' in analysis_results:
            raw_ocr_data = analysis_results['raw_ocr_data']
            if 'soil_data' in raw_ocr_data and 'structured_ocr_data' in raw_ocr_data['soil_data']:
                # Convert structured OCR data to analysis format
                from utils.analysis_engine import AnalysisEngine
                engine = AnalysisEngine()
                structured_soil_data = raw_ocr_data['soil_data']['structured_ocr_data']
                soil_params = engine._convert_structured_to_analysis_format(structured_soil_data, 'soil')
            
            if 'leaf_data' in raw_ocr_data and 'structured_ocr_data' in raw_ocr_data['leaf_data']:
                # Convert structured OCR data to analysis format
                structured_leaf_data = raw_ocr_data['leaf_data']['structured_ocr_data']
                leaf_params = engine._convert_structured_to_analysis_format(structured_leaf_data, 'leaf')
        
        # Display comprehensive data tables if we have data
        if soil_params or leaf_params:
            display_comprehensive_data_tables(soil_params, leaf_params)
    
    # Display Executive Summary
    display_summary_section(analysis_data)
    
    # Display Step-by-Step Analysis
    display_step_by_step_results(analysis_data)
    
    # PDF Download section
    st.markdown("---")
    st.markdown(f"## 📄 {t('results_download_report', 'Download Report')}")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button(f"📥 {t('pdf_download_report', 'Download PDF Report')}", type="primary", use_container_width=True):
            try:
                # Generate PDF
                with st.spinner(f"🔄 {t('pdf_generating', 'Generating PDF report...')}"):
                    pdf_bytes = generate_results_pdf(analysis_data)
                    
                # Get language-specific filename
                current_lang = get_language()
                filename = "laporan_analisis_pertanian.pdf" if current_lang == 'ms' else "agricultural_analysis_report.pdf"
                
                # Download the PDF
                st.download_button(
                    label=f"💾 {t('pdf_download_report', 'Download PDF')}",
                    data=pdf_bytes,
                    file_name=filename,
                    mime="application/pdf",
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"❌ {t('pdf_generated_error', 'Failed to generate PDF')}: {str(e)}")
                st.info("Please try again or contact support if the issue persists.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Add back button
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🏠 Back to Home", use_container_width=True):
            st.session_state.current_page = 'home'
            st.rerun()

