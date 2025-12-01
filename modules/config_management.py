import streamlit as st
import sys
import os
from datetime import datetime
import json

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))

# Import utilities
from utils.config_manager import config_manager, AIConfig, MPOBStandards, EconomicConfig, OCRConfig, UIConfig, MPOBStandard
from utils.firebase_config import get_firestore_client

def show_config_management():
    """Configuration management page for administrators"""
    
    # Authentication is handled by admin panel - no need to check here
    st.title("⚙️ System Configuration Management")
    st.markdown("Manage system-wide configurations including AI models, MPOB standards, economic parameters, OCR settings, and UI preferences.")
    
    # Create tabs for simplified configuration
    tab1, tab2 = st.tabs([
        "🤖 AI Configuration", 
        "📊 MPOB Standards"
    ])
    
    with tab1:
        show_ai_configuration()
    
    with tab2:
        show_mpob_standards_configuration()

def show_ai_configuration():
    """AI configuration management"""
    st.markdown("### 🤖 AI Model Configuration")
    
    # Get current AI configuration
    current_config = config_manager.get_ai_config()
    
    with st.form("ai_config_form"):
        st.markdown("#### Model Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Safe model selection with proper error handling
            available_models = ["gemini-2.5-pro", "gemini-1.5-pro", "gemini-1.5-flash"]
            current_model = getattr(current_config, 'model', 'gemini-2.5-pro')
            model_index = available_models.index(current_model) if current_model in available_models else 0
            
            model = st.selectbox(
                "AI Model",
                available_models,
                index=model_index
            )
            
            temperature = st.slider(
                "Temperature",
                min_value=0.0,
                max_value=2.0,
                value=current_config.temperature,
                step=0.1,
                help="Controls randomness. 0.0 = deterministic, 2.0 = very random"
            )
            
            # Clamp current value within allowed bounds to avoid StreamlitValueAboveMaxError
            current_max_tokens = current_config.max_tokens
            if current_max_tokens > 65536:
                current_max_tokens = 65536
            if current_max_tokens < 1000:
                current_max_tokens = 1000
            max_tokens = st.number_input(
                "Max Tokens",
                min_value=1000,
                max_value=65536,
                value=current_max_tokens,
                step=1000,
                help="Maximum number of tokens to generate"
            )
        
        with col2:
            top_p = st.slider(
                "Top P",
                min_value=0.0,
                max_value=1.0,
                value=current_config.top_p,
                step=0.1,
                help="Controls diversity via nucleus sampling"
            )
            
            frequency_penalty = st.slider(
                "Frequency Penalty",
                min_value=-2.0,
                max_value=2.0,
                value=current_config.frequency_penalty,
                step=0.1,
                help="Reduces likelihood of repeating the same line"
            )
            
            presence_penalty = st.slider(
                "Presence Penalty",
                min_value=-2.0,
                max_value=2.0,
                value=current_config.presence_penalty,
                step=0.1,
                help="Increases likelihood of talking about new topics"
            )
        
        st.markdown("#### Advanced Settings")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Safe embedding model selection
            available_embeddings = ["text-embedding-004"]
            current_embedding = getattr(current_config, 'embedding_model', 'text-embedding-004')
            embedding_index = available_embeddings.index(current_embedding) if current_embedding in available_embeddings else 0
            
            embedding_model = st.selectbox(
                "Embedding Model",
                available_embeddings,
                index=embedding_index
            )
            
            enable_rag = st.checkbox(
                "Enable RAG (Retrieval Augmented Generation)",
                value=current_config.enable_rag,
                help="Use reference materials to enhance analysis"
            )
        
        with col4:
            enable_caching = st.checkbox(
                "Enable Response Caching",
                value=current_config.enable_caching,
                help="Cache AI responses for faster processing"
            )
            
            retry_attempts = st.number_input(
                "Retry Attempts",
                min_value=1,
                max_value=10,
                value=current_config.retry_attempts,
                help="Number of retry attempts for failed requests"
            )
        
        timeout_seconds = st.number_input(
            "Timeout (seconds)",
            min_value=10,
            max_value=300,
            value=current_config.timeout_seconds,
            help="Request timeout in seconds"
        )
        
        confidence_threshold = st.slider(
            "Confidence Threshold",
            min_value=0.0,
            max_value=1.0,
            value=current_config.confidence_threshold,
            step=0.1,
            help="Minimum confidence for AI responses"
        )
        
        # Form submission
        col_submit1, col_submit2, col_submit3 = st.columns([1, 1, 1])
        
        with col_submit1:
            if st.form_submit_button("💾 Save Configuration", type="primary"):
                save_ai_configuration(
                    model, temperature, max_tokens, top_p, frequency_penalty, 
                    presence_penalty, embedding_model, enable_rag, enable_caching,
                    retry_attempts, timeout_seconds, confidence_threshold
                )
        
        with col_submit2:
            if st.form_submit_button("🔄 Reset to Defaults"):
                reset_ai_configuration()
        
        with col_submit3:
            if st.form_submit_button("🧪 Test Configuration"):
                test_ai_configuration()

def show_mpob_standards_configuration():
    """MPOB standards configuration management"""
    st.markdown("### 📊 MPOB Standards Configuration")
    
    # Get current MPOB standards
    current_standards = config_manager.get_mpob_standards()
    
    # Create tabs for soil and leaf standards
    soil_tab, leaf_tab = st.tabs(["🌱 Soil Standards", "🍃 Leaf Standards"])
    
    with soil_tab:
        st.markdown("#### Soil Analysis Standards")
        
        if current_standards.soil_standards:
            for param_name, standard in current_standards.soil_standards.items():
                with st.expander(f"📋 {param_name}"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        min_val = st.number_input(
                            f"Minimum {param_name}",
                            value=standard.min_value,
                            key=f"soil_{param_name}_min"
                        )
                    
                    with col2:
                        max_val = st.number_input(
                            f"Maximum {param_name}",
                            value=standard.max_value,
                            key=f"soil_{param_name}_max"
                        )
                    
                    with col3:
                        optimal_val = st.number_input(
                            f"Optimal {param_name}",
                            value=standard.optimal_value if standard.optimal_value is not None else (standard.min_value + standard.max_value) / 2,
                            key=f"soil_{param_name}_optimal"
                        )
                    
                    unit = st.text_input(
                        f"Unit for {param_name}",
                        value=standard.unit,
                        key=f"soil_{param_name}_unit"
                    )
                    
                    description = st.text_area(
                        f"Description for {param_name}",
                        value=standard.description,
                        key=f"soil_{param_name}_desc"
                    )
                    
                    critical = st.checkbox(
                        f"Critical Parameter",
                        value=standard.critical,
                        key=f"soil_{param_name}_critical"
                    )
    
    with leaf_tab:
        st.markdown("#### Leaf Analysis Standards")
        
        if current_standards.leaf_standards:
            for param_name, standard in current_standards.leaf_standards.items():
                with st.expander(f"📋 {param_name}"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        min_val = st.number_input(
                            f"Minimum {param_name}",
                            value=standard.min_value,
                            key=f"leaf_{param_name}_min"
                        )
                    
                    with col2:
                        max_val = st.number_input(
                            f"Maximum {param_name}",
                            value=standard.max_value,
                            key=f"leaf_{param_name}_max"
                        )
                    
                    with col3:
                        optimal_val = st.number_input(
                            f"Optimal {param_name}",
                            value=standard.optimal_value if standard.optimal_value is not None else (standard.min_value + standard.max_value) / 2,
                            key=f"leaf_{param_name}_optimal"
                        )
                    
                    unit = st.text_input(
                        f"Unit for {param_name}",
                        value=standard.unit,
                        key=f"leaf_{param_name}_unit"
                    )
                    
                    description = st.text_area(
                        f"Description for {param_name}",
                        value=standard.description,
                        key=f"leaf_{param_name}_desc"
                    )
                    
                    critical = st.checkbox(
                        f"Critical Parameter",
                        value=standard.critical,
                        key=f"leaf_{param_name}_critical"
                    )
    
    # Save button
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("💾 Save MPOB Standards", type="primary"):
            save_mpob_standards()
    
    with col2:
        if st.button("🔄 Reset to Defaults"):
            reset_mpob_standards()

def show_economic_configuration():
    """Economic configuration management"""
    st.markdown("### 💰 Economic Configuration")
    
    # Get current economic configuration
    current_config = config_manager.get_economic_config()
    
    with st.form("economic_config_form"):
        st.markdown("#### Basic Economic Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Safe currency selection
            available_currencies = ["RM", "USD", "EUR", "GBP", "SGD"]
            current_currency = getattr(current_config, 'currency', 'RM')
            currency_index = available_currencies.index(current_currency) if current_currency in available_currencies else 0
            
            currency = st.selectbox(
                "Currency",
                available_currencies,
                index=currency_index
            )
            
            yield_price = st.number_input(
                "Yield Price per Ton",
                min_value=0.0,
                value=current_config.yield_price_per_ton,
                step=100.0,
                help="Price per ton of oil palm yield"
            )
            
            inflation_rate = st.slider(
                "Inflation Rate",
                min_value=0.0,
                max_value=0.2,
                value=current_config.inflation_rate,
                step=0.01,
                format="%.2%",
                help="Annual inflation rate"
            )
        
        with col2:
            region = st.text_input(
                "Region",
                value=current_config.region,
                help="Geographic region for economic calculations"
            )
            
            discount_rate = st.slider(
                "Discount Rate",
                min_value=0.0,
                max_value=0.2,
                value=current_config.discount_rate,
                step=0.01,
                format="%.2%",
                help="Discount rate for future value calculations"
            )
        
        st.markdown("#### Cost Configurations")
        
        # Fertilizer costs
        st.markdown("##### Fertilizer Costs (per kg)")
        fertilizer_cols = st.columns(3)
        
        fertilizer_costs = {}
        for i, (fertilizer, cost) in enumerate(current_config.fertilizer_costs.items()):
            with fertilizer_cols[i % 3]:
                fertilizer_costs[fertilizer] = st.number_input(
                    f"{fertilizer.title()}",
                    min_value=0.0,
                    value=cost,
                    step=0.1,
                    key=f"fertilizer_{fertilizer}"
                )
        
        # Application costs
        st.markdown("##### Application Costs (per hectare)")
        application_cols = st.columns(3)
        
        application_costs = {}
        for i, (method, cost) in enumerate(current_config.application_costs.items()):
            with application_cols[i % 3]:
                application_costs[method] = st.number_input(
                    f"{method.title()}",
                    min_value=0.0,
                    value=cost,
                    step=10.0,
                    key=f"application_{method}"
                )
        
        # Labor costs
        st.markdown("##### Labor Costs (per hour)")
        labor_cols = st.columns(3)
        
        labor_costs = {}
        for i, (type, cost) in enumerate(current_config.labor_costs.items()):
            with labor_cols[i % 3]:
                labor_costs[type] = st.number_input(
                    f"{type.title()}",
                    min_value=0.0,
                    value=cost,
                    step=1.0,
                    key=f"labor_{type}"
                )
        
        # Equipment costs
        st.markdown("##### Equipment Costs (per day)")
        equipment_cols = st.columns(3)
        
        equipment_costs = {}
        for i, (equipment, cost) in enumerate(current_config.equipment_costs.items()):
            with equipment_cols[i % 3]:
                equipment_costs[equipment] = st.number_input(
                    f"{equipment.title()}",
                    min_value=0.0,
                    value=cost,
                    step=10.0,
                    key=f"equipment_{equipment}"
                )
        
        # Form submission
        col_submit1, col_submit2 = st.columns([1, 1])
        
        with col_submit1:
            if st.form_submit_button("💾 Save Economic Configuration", type="primary"):
                save_economic_configuration(
                    currency, yield_price, inflation_rate, discount_rate, region,
                    fertilizer_costs, application_costs, labor_costs, equipment_costs
                )
        
        with col_submit2:
            if st.form_submit_button("🔄 Reset to Defaults"):
                reset_economic_configuration()

def show_ocr_configuration():
    """OCR configuration management"""
    st.markdown("### 🔍 OCR Configuration")
    
    # Get current OCR configuration
    current_config = config_manager.get_ocr_config()
    
    with st.form("ocr_config_form"):
        st.markdown("#### OCR Processing Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            psm_modes = st.multiselect(
                "PSM Modes",
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                default=current_config.psm_modes,
                help="Page segmentation modes to try"
            )
            
            character_whitelist = st.text_input(
                "Character Whitelist",
                value=current_config.character_whitelist,
                help="Characters allowed in OCR recognition"
            )
            
            scale_factor_min = st.number_input(
                "Min Scale Factor",
                min_value=0.5,
                max_value=2.0,
                value=current_config.scale_factor_min,
                step=0.1,
                help="Minimum image scale factor"
            )
            
            scale_factor_max = st.number_input(
                "Max Scale Factor",
                min_value=1.0,
                max_value=5.0,
                value=current_config.scale_factor_max,
                step=0.1,
                help="Maximum image scale factor"
            )
        
        with col2:
            contrast_enhancement = st.slider(
                "Contrast Enhancement",
                min_value=0.5,
                max_value=3.0,
                value=current_config.contrast_enhancement,
                step=0.1,
                help="Image contrast enhancement factor"
            )
            
            sharpness_enhancement = st.slider(
                "Sharpness Enhancement",
                min_value=0.5,
                max_value=3.0,
                value=current_config.sharpness_enhancement,
                step=0.1,
                help="Image sharpness enhancement factor"
            )
            
            confidence_threshold = st.slider(
                "Confidence Threshold",
                min_value=0.0,
                max_value=1.0,
                value=current_config.confidence_threshold,
                step=0.1,
                help="Minimum confidence for OCR results"
            )
        
        st.markdown("#### Advanced Image Processing")
        
        col3, col4 = st.columns(2)
        
        with col3:
            bilateral_filter_d = st.number_input(
                "Bilateral Filter D",
                min_value=1,
                max_value=20,
                value=current_config.bilateral_filter_d,
                help="Bilateral filter diameter"
            )
            
            bilateral_filter_sigma_color = st.number_input(
                "Bilateral Filter Sigma Color",
                min_value=10.0,
                max_value=200.0,
                value=current_config.bilateral_filter_sigma_color,
                step=10.0,
                help="Bilateral filter sigma color"
            )
        
        with col4:
            bilateral_filter_sigma_space = st.number_input(
                "Bilateral Filter Sigma Space",
                min_value=10.0,
                max_value=200.0,
                value=current_config.bilateral_filter_sigma_space,
                step=10.0,
                help="Bilateral filter sigma space"
            )
            
            adaptive_threshold_block_size = st.number_input(
                "Adaptive Threshold Block Size",
                min_value=3,
                max_value=50,
                value=current_config.adaptive_threshold_block_size,
                step=2,
                help="Adaptive threshold block size (must be odd)"
            )
        
        adaptive_threshold_c = st.number_input(
            "Adaptive Threshold C",
            min_value=0,
            max_value=20,
            value=current_config.adaptive_threshold_c,
            help="Adaptive threshold constant"
        )
        
        # Form submission
        col_submit1, col_submit2 = st.columns([1, 1])
        
        with col_submit1:
            if st.form_submit_button("💾 Save OCR Configuration", type="primary"):
                save_ocr_configuration(
                    psm_modes, character_whitelist, scale_factor_min, scale_factor_max,
                    contrast_enhancement, sharpness_enhancement, confidence_threshold,
                    bilateral_filter_d, bilateral_filter_sigma_color, bilateral_filter_sigma_space,
                    adaptive_threshold_block_size, adaptive_threshold_c
                )
        
        with col_submit2:
            if st.form_submit_button("🔄 Reset to Defaults"):
                reset_ocr_configuration()

def show_ui_configuration():
    """UI configuration management"""
    st.markdown("### 🎨 UI/UX Configuration")
    
    # Get current UI configuration
    current_config = config_manager.get_ui_config()
    
    with st.form("ui_config_form"):
        st.markdown("#### Theme and Appearance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Safe theme selection
            available_themes = ["light", "dark", "auto"]
            current_theme = getattr(current_config, 'theme', 'light')
            theme_index = available_themes.index(current_theme) if current_theme in available_themes else 0
            
            theme = st.selectbox(
                "Theme",
                available_themes,
                index=theme_index
            )
            
            primary_color = st.color_picker(
                "Primary Color",
                value=current_config.primary_color
            )
            
            secondary_color = st.color_picker(
                "Secondary Color",
                value=current_config.secondary_color
            )
        
        with col2:
            accent_color = st.color_picker(
                "Accent Color",
                value=current_config.accent_color
            )
            
            # Safe language selection
            available_languages = ["English", "Malay", "Chinese", "Tamil"]
            current_language = getattr(current_config, 'language', 'English')
            language_index = available_languages.index(current_language) if current_language in available_languages else 0
            
            language = st.selectbox(
                "Language",
                available_languages,
                index=language_index
            )
        
        st.markdown("#### Formatting and Units")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Safe date format selection
            available_date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"]
            current_date_format = getattr(current_config, 'date_format', '%Y-%m-%d')
            date_format_index = available_date_formats.index(current_date_format) if current_date_format in available_date_formats else 0
            
            date_format = st.selectbox(
                "Date Format",
                available_date_formats,
                index=date_format_index
            )
            
            # Safe number format selection
            available_number_formats = ["en_US", "en_GB", "ms_MY", "zh_CN"]
            current_number_format = getattr(current_config, 'number_format', 'en_US')
            number_format_index = available_number_formats.index(current_number_format) if current_number_format in available_number_formats else 0
            
            number_format = st.selectbox(
                "Number Format",
                available_number_formats,
                index=number_format_index
            )
        
        with col4:
            # Land size units
            land_size_units = st.multiselect(
                "Land Size Units",
                ["hectares", "acres", "square_meters", "square_feet"],
                default=current_config.units.get("land_size", ["hectares", "acres"]),
                key="land_size_units"
            )
            
            # Yield units
            yield_units = st.multiselect(
                "Yield Units",
                ["tonnes/hectare", "kg/hectare", "tonnes/acre", "kg/acre"],
                default=current_config.units.get("yield", ["tonnes/hectare", "kg/hectare"]),
                key="yield_units"
            )
        
        st.markdown("#### Display Preferences")
        
        col5, col6 = st.columns(2)
        
        with col5:
            show_icons = st.checkbox(
                "Show Icons",
                value=current_config.display_preferences.get("show_icons", True)
            )
            
            show_colors = st.checkbox(
                "Show Colors",
                value=current_config.display_preferences.get("show_colors", True)
            )
        
        with col6:
            compact_mode = st.checkbox(
                "Compact Mode",
                value=current_config.display_preferences.get("compact_mode", False)
            )
            
            auto_refresh = st.checkbox(
                "Auto Refresh",
                value=current_config.display_preferences.get("auto_refresh", True)
            )
        
        # Safe default chart type selection
        available_chart_types = ["line", "bar", "scatter", "area"]
        current_chart_type = current_config.display_preferences.get("default_chart_type", "line") if hasattr(current_config, 'display_preferences') and current_config.display_preferences else "line"
        chart_type_index = available_chart_types.index(current_chart_type) if current_chart_type in available_chart_types else 0
        
        default_chart_type = st.selectbox(
            "Default Chart Type",
            available_chart_types,
            index=chart_type_index
        )
        
        # Form submission
        col_submit1, col_submit2 = st.columns([1, 1])
        
        with col_submit1:
            if st.form_submit_button("💾 Save UI Configuration", type="primary"):
                save_ui_configuration(
                    theme, primary_color, secondary_color, accent_color, language,
                    date_format, number_format, land_size_units, yield_units,
                    show_icons, show_colors, compact_mode, auto_refresh, default_chart_type
                )
        
        with col_submit2:
            if st.form_submit_button("🔄 Reset to Defaults"):
                reset_ui_configuration()

def show_system_overview():
    """System configuration overview (simplified)"""
    st.markdown("### 📋 System Configuration Overview")
    
    # Get relevant configurations only
    all_configs = config_manager.get_all_configs()
    ai_config = all_configs.get('ai_config')
    mpob = all_configs.get('mpob_standards')
    
    # Fetch reference documents summary
    ref_count = 0
    latest_refs = []
    try:
        db = get_firestore_client()
        if db:
            docs_ref = db.collection('reference_documents')
            # Count (may be approximate if large; for simplicity stream)
            docs = list(docs_ref.stream())
            ref_count = len(docs)
            # Build list with created_at and name
            def _doc_info(d):
                data = d.to_dict() if hasattr(d, 'to_dict') else d
                created = data.get('created_at')
                ts = created.timestamp() if hasattr(created, 'timestamp') else 0
                return (
                    ts,
                    data.get('name', 'Unnamed'),
                    data.get('storage_url'),
                    data.get('file_name') or data.get('name', 'Unnamed')
                )
            info_list = [_doc_info(d) for d in docs]
            latest_refs = sorted(info_list, key=lambda x: x[0], reverse=True)[:5]
    except Exception:
        pass
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("AI Config", "✅ Active" if ai_config else "❌ Missing")
    with col2:
        st.metric("MPOB Standards", "✅ Active" if mpob else "❌ Missing")
    with col3:
        st.metric("Reference Docs", ref_count)
    
    colx, coly = st.columns(2)
    with colx:
        st.metric("Cache Status", "✅ Active" if config_manager._cache else "❌ Empty")
    with coly:
        if latest_refs:
            st.metric("Latest Uploaded", latest_refs[0][3])
    
    st.markdown("#### Quick Details")
    
    if ai_config:
        with st.expander("🤖 AI Configuration"):
            st.write(f"Model: {ai_config.model}")
            st.write(f"Temperature: {ai_config.temperature}")
            st.write(f"Max Tokens: {ai_config.max_tokens}")
            st.write(f"Embedding Model: {ai_config.embedding_model}")
    else:
        st.warning("AI Configuration is missing.")
    
    if mpob:
        with st.expander("📊 MPOB Standards"):
            soil_count = len(mpob.soil_standards) if mpob.soil_standards else 0
            leaf_count = len(mpob.leaf_standards) if mpob.leaf_standards else 0
            st.write(f"Soil parameters: {soil_count}")
            st.write(f"Leaf parameters: {leaf_count}")
    else:
        st.warning("MPOB Standards are missing.")
    
    # Reference documents detail
    with st.expander("📚 Reference Documents"):
        st.write(f"Total documents: {ref_count}")
        if latest_refs:
            st.write("Recent uploads:")
            for _, _, url, fname in latest_refs:
                if url:
                    st.markdown(f"- [{fname}]({url})")
                else:
                    st.markdown(f"- {fname}")
        else:
            st.write("No reference documents found.")
    
    # Cache management
    st.markdown("#### Cache Management")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Refresh All Configs"):
            config_manager.clear_cache()
            st.success("All configurations refreshed from database")
            st.rerun()
    with col2:
        if st.button("🧹 Clear Cache"):
            config_manager.clear_cache()
            st.success("Cache cleared")
            st.rerun()

# Configuration save functions
def save_ai_configuration(model, temperature, max_tokens, top_p, frequency_penalty, 
                         presence_penalty, embedding_model, enable_rag, enable_caching,
                         retry_attempts, timeout_seconds, confidence_threshold):
    """Save AI configuration"""
    try:
        config_data = {
            'model': model,
            'temperature': temperature,
            'max_tokens': max_tokens,
            'top_p': top_p,
            'frequency_penalty': frequency_penalty,
            'presence_penalty': presence_penalty,
            'embedding_model': embedding_model,
            'enable_rag': enable_rag,
            'enable_caching': enable_caching,
            'retry_attempts': retry_attempts,
            'timeout_seconds': timeout_seconds,
            'confidence_threshold': confidence_threshold
        }
        
        if config_manager.save_config('ai_config', config_data):
            st.success("✅ AI configuration saved successfully!")
        else:
            st.error("❌ Failed to save AI configuration")
    except Exception as e:
        st.error(f"❌ Error saving AI configuration: {str(e)}")

def save_mpob_standards():
    """Save MPOB standards configuration"""
    try:
        # This would need to collect all the form data
        # For now, just show a placeholder
        st.info("🔄 MPOB standards save functionality needs to be implemented")
    except Exception as e:
        st.error(f"❌ Error saving MPOB standards: {str(e)}")

def save_economic_configuration(currency, yield_price, inflation_rate, discount_rate, region,
                               fertilizer_costs, application_costs, labor_costs, equipment_costs):
    """Save economic configuration"""
    try:
        config_data = {
            'currency': currency,
            'yield_price_per_ton': yield_price,
            'inflation_rate': inflation_rate,
            'discount_rate': discount_rate,
            'region': region,
            'fertilizer_costs': fertilizer_costs,
            'application_costs': application_costs,
            'labor_costs': labor_costs,
            'equipment_costs': equipment_costs,
            'updated_at': datetime.now()
        }
        
        if config_manager.save_config('economic_config', config_data):
            st.success("✅ Economic configuration saved successfully!")
        else:
            st.error("❌ Failed to save economic configuration")
    except Exception as e:
        st.error(f"❌ Error saving economic configuration: {str(e)}")

def save_ocr_configuration(psm_modes, character_whitelist, scale_factor_min, scale_factor_max,
                          contrast_enhancement, sharpness_enhancement, confidence_threshold,
                          bilateral_filter_d, bilateral_filter_sigma_color, bilateral_filter_sigma_space,
                          adaptive_threshold_block_size, adaptive_threshold_c):
    """Save OCR configuration"""
    try:
        config_data = {
            'psm_modes': psm_modes,
            'character_whitelist': character_whitelist,
            'scale_factor_min': scale_factor_min,
            'scale_factor_max': scale_factor_max,
            'contrast_enhancement': contrast_enhancement,
            'sharpness_enhancement': sharpness_enhancement,
            'confidence_threshold': confidence_threshold,
            'bilateral_filter_d': bilateral_filter_d,
            'bilateral_filter_sigma_color': bilateral_filter_sigma_color,
            'bilateral_filter_sigma_space': bilateral_filter_sigma_space,
            'adaptive_threshold_block_size': adaptive_threshold_block_size,
            'adaptive_threshold_c': adaptive_threshold_c
        }
        
        if config_manager.save_config('ocr_config', config_data):
            st.success("✅ OCR configuration saved successfully!")
        else:
            st.error("❌ Failed to save OCR configuration")
    except Exception as e:
        st.error(f"❌ Error saving OCR configuration: {str(e)}")

def save_ui_configuration(theme, primary_color, secondary_color, accent_color, language,
                         date_format, number_format, land_size_units, yield_units,
                         show_icons, show_colors, compact_mode, auto_refresh, default_chart_type):
    """Save UI configuration"""
    try:
        config_data = {
            'theme': theme,
            'primary_color': primary_color,
            'secondary_color': secondary_color,
            'accent_color': accent_color,
            'language': language,
            'date_format': date_format,
            'number_format': number_format,
            'units': {
                'land_size': land_size_units,
                'yield': yield_units
            },
            'display_preferences': {
                'show_icons': show_icons,
                'show_colors': show_colors,
                'compact_mode': compact_mode,
                'auto_refresh': auto_refresh,
                'default_chart_type': default_chart_type
            }
        }
        
        if config_manager.save_config('ui_config', config_data):
            st.success("✅ UI configuration saved successfully!")
        else:
            st.error("❌ Failed to save UI configuration")
    except Exception as e:
        st.error(f"❌ Error saving UI configuration: {str(e)}")

# Reset functions
def reset_ai_configuration():
    """Reset AI configuration to defaults"""
    try:
        if config_manager.reset_to_defaults('ai_config'):
            st.success("✅ AI configuration reset to defaults!")
            st.rerun()
        else:
            st.error("❌ Failed to reset AI configuration")
    except Exception as e:
        st.error(f"❌ Error resetting AI configuration: {str(e)}")

def reset_mpob_standards():
    """Reset MPOB standards to defaults"""
    try:
        if config_manager.reset_to_defaults('mpob_standards'):
            st.success("✅ MPOB standards reset to defaults!")
            st.rerun()
        else:
            st.error("❌ Failed to reset MPOB standards")
    except Exception as e:
        st.error(f"❌ Error resetting MPOB standards: {str(e)}")

def reset_economic_configuration():
    """Reset economic configuration to defaults"""
    try:
        if config_manager.reset_to_defaults('economic_config'):
            st.success("✅ Economic configuration reset to defaults!")
            st.rerun()
        else:
            st.error("❌ Failed to reset economic configuration")
    except Exception as e:
        st.error(f"❌ Error resetting economic configuration: {str(e)}")

def reset_ocr_configuration():
    """Reset OCR configuration to defaults"""
    try:
        if config_manager.reset_to_defaults('ocr_config'):
            st.success("✅ OCR configuration reset to defaults!")
            st.rerun()
        else:
            st.error("❌ Failed to reset OCR configuration")
    except Exception as e:
        st.error(f"❌ Error resetting OCR configuration: {str(e)}")

def reset_ui_configuration():
    """Reset UI configuration to defaults"""
    try:
        if config_manager.reset_to_defaults('ui_config'):
            st.success("✅ UI configuration reset to defaults!")
            st.rerun()
        else:
            st.error("❌ Failed to reset UI configuration")
    except Exception as e:
        st.error(f"❌ Error resetting UI configuration: {str(e)}")

def test_ai_configuration():
    """Test AI configuration"""
    try:
        st.info("🧪 Testing AI configuration...")
        
        # Get current AI config
        ai_config = config_manager.get_ai_config()
        
        # Test basic configuration
        test_results = {
            "Model": ai_config.model,
            "Temperature": ai_config.temperature,
            "Max Tokens": ai_config.max_tokens,
            "Embedding Model": ai_config.embedding_model,
            "RAG Enabled": ai_config.enable_rag,
            "Caching Enabled": ai_config.enable_caching
        }
        
        st.json(test_results)
        st.success("✅ AI configuration test completed!")
        
    except Exception as e:
        st.error(f"❌ Error testing AI configuration: {str(e)}")
