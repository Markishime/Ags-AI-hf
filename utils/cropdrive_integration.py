"""
CropDrive Website Integration Module
Handles communication with parent window when embedded in CropDrive website
"""

import streamlit as st
from streamlit.components.v1 import html
import json
from datetime import datetime

# ============================================================================
# STEP 1: JavaScript for parent window communication
# ============================================================================

def inject_parent_communication():
    """Inject JavaScript to communicate with parent window (CropDrive website)"""
    js_code = """
    <script>
    (function() {
        let currentLanguage = 'en';
        let userConfig = {};
        
        // Listen for messages from parent window
        window.addEventListener('message', function(event) {
            // IMPORTANT: Update these with your actual website domains
            const allowedOrigins = [
                'https://cropdrive.ai',
                'https://cropdrive-f5exleg55-mark-lloyd-cuizons-projects.vercel.app',
                'http://localhost:3000'  // For local development
            ];
            
            if (!allowedOrigins.includes(event.origin)) {
                console.warn('Message from unauthorized origin:', event.origin);
                return;
            }
            
            const data = event.data;
            console.log('📨 Received message from parent:', data);
            
            // Handle initial configuration
            if (data.type === 'CONFIG') {
                userConfig = {
                    language: data.language || 'en',
                    userId: data.userId,
                    plan: data.plan || 'none',
                    userEmail: data.userEmail
                };
                currentLanguage = userConfig.language;
                
                // Store in localStorage
                localStorage.setItem('cropdrive_language', currentLanguage);
                localStorage.setItem('cropdrive_userId', userConfig.userId || '');
                localStorage.setItem('cropdrive_plan', userConfig.plan || 'none');
                
                // Update URL parameters
                const url = new URL(window.location.href);
                url.searchParams.set('lang', currentLanguage);
                url.searchParams.set('userId', userConfig.userId || '');
                url.searchParams.set('plan', userConfig.plan || 'none');
                window.history.replaceState({}, '', url);
                
                // Reload to apply changes
                window.location.reload();
            }
            
            // Handle language change
            if (data.type === 'LANGUAGE_CHANGE') {
                currentLanguage = data.language;
                localStorage.setItem('cropdrive_language', currentLanguage);
                
                // Update URL
                const url = new URL(window.location.href);
                url.searchParams.set('lang', currentLanguage);
                window.history.replaceState({}, '', url);
                
                // Reload to apply language change
                window.location.reload();
            }
        });
        
        // Check URL parameters on page load
        window.addEventListener('load', function() {
            const urlParams = new URLSearchParams(window.location.search);
            const langParam = urlParams.get('lang');
            if (langParam && (langParam === 'en' || langParam === 'ms')) {
                currentLanguage = langParam;
                localStorage.setItem('cropdrive_language', currentLanguage);
            }
        });
    })();
    </script>
    """
    html(js_code, height=0)

# ============================================================================
# STEP 2: Initialize integration and get language
# ============================================================================

def initialize_integration():
    """Initialize integration and get current language"""
    # Inject communication JavaScript
    inject_parent_communication()
    
    # Get language from URL parameters
    query_params = st.query_params
    current_lang = query_params.get('lang', 'en')
    
    # Validate language
    if current_lang not in ['en', 'ms']:
        current_lang = 'en'
    
    # Store in session state
    if 'language' not in st.session_state:
        st.session_state.language = current_lang
    
    # Get user config from URL
    user_id = query_params.get('userId', '')
    user_plan = query_params.get('plan', 'none')
    features_str = query_params.get('features', '')
    features = features_str.split(',') if features_str else []
    
    if 'user_config' not in st.session_state:
        st.session_state.user_config = {
            'userId': user_id,
            'plan': user_plan,
            'features': features
        }
    
    return current_lang, user_plan, features

# ============================================================================
# STEP 3: Send analysis results to website
# ============================================================================

def send_analysis_complete(
    title: str,
    analysis_type: str,  # 'soil' or 'leaf'
    summary: str = '',
    recommendations_count: int = 0,
    file_url: str = None,
    analysis_data: dict = None
):
    """
    Send analysis completion message to parent window (CropDrive website)
    
    Args:
        title: Report title (required)
        analysis_type: 'soil' or 'leaf' (required)
        summary: Analysis summary text (optional)
        recommendations_count: Number of recommendations (optional)
        file_url: URL to analysis file/image (optional)
        analysis_data: Additional analysis data dict (optional)
    """
    message = {
        'type': 'ANALYSIS_COMPLETE',
        'title': title,
        'analysisType': analysis_type,
        'summary': summary or '',
        'recommendationsCount': recommendations_count,
        'fileUrl': file_url,
        'analysisData': analysis_data or {}
    }
    
    # IMPORTANT: Update this with your actual website domain
    parent_origin = 'https://cropdrive-f5exleg55-mark-lloyd-cuizons-projects.vercel.app'
    
    send_js = f"""
    <script>
    window.parent.postMessage({json.dumps(message)}, '{parent_origin}');
    console.log('📤 Sent analysis complete message:', {json.dumps(message)});
    </script>
    """
    
    html(send_js, height=0)

# ============================================================================
# STEP 4: Handle plan-based features
# ============================================================================

def is_feature_available(feature_name: str, user_plan: str) -> bool:
    """
    Check if a feature is available for the user's plan
    
    Args:
        feature_name: Name of the feature ('basic', 'priority', 'premium', 'comparative', 'early_access')
        user_plan: User's plan ('start', 'smart', 'precision', 'none')
    
    Returns:
        True if feature is available, False otherwise
    """
    plan_features = {
        'start': ['basic'],
        'smart': ['basic', 'priority'],
        'precision': ['basic', 'priority', 'premium', 'comparative', 'early_access'],
        'none': []  # No features for unsubscribed users
    }
    
    available_features = plan_features.get(user_plan, ['basic'])
    return feature_name in available_features

def send_feature_restricted(required_plan: str, feature_name: str):
    """
    Notify parent window that user tried to use a restricted feature
    
    Args:
        required_plan: Plan required to access the feature
        feature_name: Name of the restricted feature
    """
    message = {
        'type': 'FEATURE_RESTRICTED',
        'requiredPlan': required_plan,
        'featureName': feature_name
    }
    
    parent_origin = 'https://cropdrive-f5exleg55-mark-lloyd-cuizons-projects.vercel.app'
    
    send_js = f"""
    <script>
    window.parent.postMessage({json.dumps(message)}, '{parent_origin}');
    </script>
    """
    
    html(send_js, height=0)

def get_user_plan():
    """Get current user plan from session state"""
    return st.session_state.get('user_config', {}).get('plan', 'none')

def get_user_id():
    """Get current user ID from session state"""
    return st.session_state.get('user_config', {}).get('userId', '')

def send_language_change(new_language: str):
    """Send language change notification to parent window"""
    message = {
        'type': 'LANGUAGE_CHANGE',
        'language': new_language
    }
    
    parent_origin = 'https://cropdrive-f5exleg55-mark-lloyd-cuizons-projects.vercel.app'
    
    send_js = f"""
    <script>
    window.parent.postMessage({json.dumps(message)}, '{parent_origin}');
    console.log('📤 Sent language change message:', {json.dumps(message)});
    </script>
    """
    
    html(send_js, height=0)

