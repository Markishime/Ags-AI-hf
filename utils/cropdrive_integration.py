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
                    userName: data.userName || '',
                    uploadsUsed: data.uploadsUsed || 0,
                    uploadsLimit: data.uploadsLimit || 0,
                    uploadLimitExceeded: data.uploadLimitExceeded || false,
                    uploadsRemaining: data.uploadsRemaining !== undefined ? data.uploadsRemaining : (data.uploadsLimit || 0) - (data.uploadsUsed || 0)
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
                localStorage.setItem('cropdrive_uploadsUsed', String(userConfig.uploadsUsed));
                localStorage.setItem('cropdrive_uploadsLimit', String(userConfig.uploadsLimit));
                localStorage.setItem('cropdrive_uploadLimitExceeded', String(userConfig.uploadLimitExceeded));
                localStorage.setItem('cropdrive_uploadsRemaining', String(userConfig.uploadsRemaining));
                
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
                url.searchParams.set('uploadsUsed', String(userConfig.uploadsUsed));
                url.searchParams.set('uploadsLimit', String(userConfig.uploadsLimit));
                url.searchParams.set('uploadLimitExceeded', String(userConfig.uploadLimitExceeded));
                url.searchParams.set('uploadsRemaining', String(userConfig.uploadsRemaining));
                window.history.replaceState({}, '', url);
                
                // Notify parent that config was received
                window.parent.postMessage({
                    type: 'CONFIG_RECEIVED',
                    userId: userConfig.userId
                }, '*');
                
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
            const uploadsUsedParam = urlParams.get('uploadsUsed');
            const uploadsLimitParam = urlParams.get('uploadsLimit');
            const uploadLimitExceededParam = urlParams.get('uploadLimitExceeded');
            const uploadsRemainingParam = urlParams.get('uploadsRemaining');
            
            if (userIdParam) {
                localStorage.setItem('cropdrive_userId', userIdParam);
            }
            if (userEmailParam) {
                localStorage.setItem('cropdrive_userEmail', userEmailParam);
            }
            if (userNameParam) {
                localStorage.setItem('cropdrive_userName', userNameParam);
            }
            if (uploadsUsedParam !== null) {
                localStorage.setItem('cropdrive_uploadsUsed', uploadsUsedParam);
            }
            if (uploadsLimitParam !== null) {
                localStorage.setItem('cropdrive_uploadsLimit', uploadsLimitParam);
            }
            if (uploadLimitExceededParam !== null) {
                localStorage.setItem('cropdrive_uploadLimitExceeded', uploadLimitExceededParam);
            }
            if (uploadsRemainingParam !== null) {
                localStorage.setItem('cropdrive_uploadsRemaining', uploadsRemainingParam);
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
    
    # Get upload limits from URL parameters
    uploads_used = query_params.get('uploadsUsed', '0')
    uploads_limit = query_params.get('uploadsLimit', '0')
    upload_limit_exceeded = query_params.get('uploadLimitExceeded', 'false')
    uploads_remaining_param = query_params.get('uploadsRemaining', '')
    
    # Convert to appropriate types
    try:
        uploads_used = int(uploads_used) if uploads_used else 0
    except (ValueError, TypeError):
        uploads_used = 0
    
    try:
        uploads_limit = int(uploads_limit) if uploads_limit else 0
    except (ValueError, TypeError):
        uploads_limit = 0
    
    try:
        upload_limit_exceeded = upload_limit_exceeded.lower() == 'true' if upload_limit_exceeded else False
    except (AttributeError, ValueError):
        upload_limit_exceeded = False
    
    # Calculate uploads_remaining ourselves: limit - used
    # This ensures correct calculation regardless of what parent sends
    if uploads_limit == 0 or uploads_limit == -1:
        # No limit or unlimited plan
        uploads_remaining = float('inf')
    else:
        # Calculate remaining: limit - used
        uploads_remaining = max(0, uploads_limit - uploads_used)
    
    # Override with parent's value only if it indicates unlimited (-1 or infinity)
    if uploads_remaining_param:
        try:
            uploads_remaining_str = str(uploads_remaining_param).lower()
            if uploads_remaining_str == 'infinity' or uploads_remaining_str == '-1' or uploads_remaining_str == 'inf':
                uploads_remaining = float('inf')
        except (ValueError, TypeError):
            pass  # Use our calculated value
    
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
    
    # Store upload limits in session state
    st.session_state.uploads_used = uploads_used
    st.session_state.uploads_limit = uploads_limit
    st.session_state.upload_limit_exceeded = upload_limit_exceeded
    st.session_state.uploads_remaining = uploads_remaining
    
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
    from datetime import datetime
    
    # Include user ID in the analysis complete message
    user_id = get_user_id()
    user_email = get_user_email()
    
    # Ensure analysis_data includes timestamp if not already present
    if analysis_data is None:
        analysis_data = {}
    
    # Add timestamp if not already in analysis_data
    if 'timestamp' not in analysis_data:
        analysis_data['timestamp'] = datetime.now().isoformat()
    
    message = {
        'type': 'ANALYSIS_COMPLETE',
        'userId': user_id,
        'title': title,
        'analysisType': analysis_type,
        'summary': summary or '',
        'recommendationsCount': recommendations_count,
        'fileUrl': file_url,
        'analysisData': analysis_data or {},
        'timestamp': datetime.now().isoformat()
    }
    
    # Send to all allowed origins
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

def can_start_analysis():
    """
    Check if user can start a new analysis based on upload limits
    
    Returns:
        True if analysis can start, False otherwise
    """
    # Get current values
    uploads_used = st.session_state.get('uploads_used', 0)
    uploads_limit = st.session_state.get('uploads_limit', 0)
    upload_limit_exceeded = st.session_state.get('upload_limit_exceeded', False)
    uploads_remaining = st.session_state.get('uploads_remaining', 0)
    
    # If upload_limit_exceeded flag is set, block analysis
    if upload_limit_exceeded:
        return False
    
    # If no limit set (0 or -1), allow unlimited analyses
    if uploads_limit == 0 or uploads_limit == -1:
        return True
    
    # Recalculate remaining to ensure accuracy: limit - used
    calculated_remaining = max(0, uploads_limit - uploads_used)
    
    # If calculated remaining is infinity, allow
    if calculated_remaining == float('inf'):
        return True
    
    # If calculated remaining is greater than 0, allow analysis
    if calculated_remaining > 0:
        return True
    
    # Otherwise, block (remaining is 0 or less)
    return False

def get_upload_limit_info():
    """
    Get upload limit information for display
    
    Returns:
        Dictionary with upload limit information
    """
    uploads_used = st.session_state.get('uploads_used', 0)
    uploads_limit = st.session_state.get('uploads_limit', 0)
    upload_limit_exceeded = st.session_state.get('upload_limit_exceeded', False)
    
    # Calculate remaining: limit - used (ensure accuracy)
    if uploads_limit == 0 or uploads_limit == -1:
        uploads_remaining = float('inf')
    else:
        uploads_remaining = max(0, uploads_limit - uploads_used)
    
    return {
        'uploads_used': uploads_used,
        'uploads_limit': uploads_limit,
        'upload_limit_exceeded': upload_limit_exceeded,
        'uploads_remaining': uploads_remaining
    }

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

