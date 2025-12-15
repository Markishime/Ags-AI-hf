"""
CropDrive Website Integration Module
Handles communication with parent window when embedded in CropDrive website
"""

import streamlit as st
from streamlit.components.v1 import html
import json

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
                    userEmail: data.userEmail || '',
                    userName: data.userName || ''
                };
                currentLanguage = userConfig.language;
                if (currentLanguage !== 'en' && currentLanguage !== 'ms') {
                    currentLanguage = 'en';
                }
                
                // Store in localStorage
                localStorage.setItem('cropdrive_language', currentLanguage);
                localStorage.setItem('cropdrive_userId', userConfig.userId || '');
                localStorage.setItem('cropdrive_userEmail', userConfig.userEmail || '');
                localStorage.setItem('cropdrive_userName', userConfig.userName || '');
                localStorage.setItem('cropdrive_plan', userConfig.plan || 'none');
                
                // Update URL parameters
                const url = new URL(window.location.href);
                const currentLangParam = url.searchParams.get('lang');
                if (currentLangParam !== currentLanguage) {
                    url.searchParams.set('lang', currentLanguage);
                }
                url.searchParams.set('userId', userConfig.userId || '');
                url.searchParams.set('userEmail', userConfig.userEmail || '');
                url.searchParams.set('userName', userConfig.userName || '');
                url.searchParams.set('plan', userConfig.plan || 'none');
                window.history.replaceState({}, '', url);
                
                // Notify parent that language was set
                window.parent.postMessage({
                    type: 'STREAMLIT_LANGUAGE_UPDATE',
                    language: currentLanguage
                }, '*');
                
                // Reload to apply changes
                window.location.reload();
            }
            
            // Handle language change
            if (data.type === 'LANGUAGE_CHANGE') {
                currentLanguage = data.language || 'en';
                if (currentLanguage !== 'en' && currentLanguage !== 'ms') {
                    currentLanguage = 'en';
                }
                localStorage.setItem('cropdrive_language', currentLanguage);
                
                // Update URL
                const url = new URL(window.location.href);
                url.searchParams.set('lang', currentLanguage);
                window.history.replaceState({}, '', url);
                
                // Notify parent that language was updated
                window.parent.postMessage({
                    type: 'STREAMLIT_LANGUAGE_UPDATE',
                    language: currentLanguage
                }, '*');
                
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
            // Also check for user info in URL params
            const userIdParam = urlParams.get('userId');
            const userEmailParam = urlParams.get('userEmail');
            const userNameParam = urlParams.get('userName');
            if (userIdParam) {
                localStorage.setItem('cropdrive_userId', userIdParam);
            }
            if (userEmailParam) {
                localStorage.setItem('cropdrive_userEmail', userEmailParam);
            }
            if (userNameParam) {
                localStorage.setItem('cropdrive_userName', userNameParam);
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
    
    # Get language from URL parameters (always check on each run)
    query_params = st.query_params
    current_lang = query_params.get('lang', 'en')
    
    # Validate language
    if current_lang not in ['en', 'ms']:
        current_lang = 'en'
    
    # Always update language from URL params (handles dynamic changes)
    st.session_state.language = current_lang
    
    # Get user config from URL
    user_id = query_params.get('userId', '')
    user_email = query_params.get('userEmail', '')
    user_name = query_params.get('userName', '')
    user_plan = query_params.get('plan', 'none')
    features_str = query_params.get('features', '')
    features = features_str.split(',') if features_str else []
    
    # Store user config in session state
    if 'user_config' not in st.session_state:
        st.session_state.user_config = {
            'userId': user_id,
            'plan': user_plan,
            'features': features
        }
    
    # Also store user info in session state for Firebase storage (for backward compatibility)
    if user_id:
        st.session_state.user_id = user_id
    if user_email:
        st.session_state.user_email = user_email
    if user_name:
        st.session_state.user_name = user_name
    
    # Update user_config with email and name
    st.session_state.user_config['userEmail'] = user_email
    st.session_state.user_config['userName'] = user_name
    
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
    # Try multiple sources for user ID
    user_id = st.session_state.get('user_id', '')
    if not user_id:
        user_id = st.session_state.get('user_config', {}).get('userId', '')
    return user_id

def get_user_email():
    """Get current user email from session state"""
    user_email = st.session_state.get('user_email', '')
    if not user_email:
        user_email = st.session_state.get('user_config', {}).get('userEmail', '')
    return user_email

def get_user_name():
    """Get current user name from session state"""
    user_name = st.session_state.get('user_name', '')
    if not user_name:
        user_name = st.session_state.get('user_config', {}).get('userName', '')
    return user_name

def send_language_change(new_language: str):
    """Send language change notification to parent window when language changes within Streamlit"""
    # Validate language
    if new_language not in ['en', 'ms']:
        new_language = 'en'
    
    # Update session state
    st.session_state.language = new_language
    
    # Update URL parameter
    query_params = st.query_params
    query_params['lang'] = new_language
    
    # Send message to parent window
    message = {
        'type': 'LANGUAGE_CHANGE_REQUEST',
        'language': new_language
    }
    
    # Get allowed origins from the JavaScript (should match)
    allowed_origins = [
        'https://cropdrive.ai',
        'https://cropdrive-f5exleg55-mark-lloyd-cuizons-projects.vercel.app',
        'http://localhost:3000'
    ]
    
    send_js = f"""
    <script>
    const allowedOrigins = {json.dumps(allowed_origins)};
    allowedOrigins.forEach(origin => {{
        window.parent.postMessage({json.dumps(message)}, origin);
    }});
    console.log('📤 Sent language change request:', {json.dumps(message)});
    
    // Also update URL
    const url = new URL(window.location.href);
    url.searchParams.set('lang', '{new_language}');
    window.history.replaceState({{}}, '', url);
    </script>
    """
    
    html(send_js, height=0)
    
    # Trigger rerun to apply language change
    st.rerun()

