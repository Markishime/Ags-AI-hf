"""
CropDrive Website Integration Module
Handles communication with parent window when embedded in CropDrive website
"""

import streamlit as st
from streamlit.components.v1 import html
import json
import logging

# Configure logger
logger = logging.getLogger(__name__)

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
                console.log('📥 Received CONFIG from website:', data);

                // Store user information for later use
                window.userConfig = {
                    userId: data.userId,
                    userEmail: data.userEmail,
                    userName: data.userName,
                    plan: data.plan,
                    uploadsUsed: data.uploadsUsed,
                    uploadsLimit: data.uploadsLimit,
                    uploadLimitExceeded: data.uploadLimitExceeded,
                    uploadsRemaining: data.uploadsRemaining,
                    language: data.language
                };

                userConfig = window.userConfig;
                currentLanguage = userConfig.language;
                if (currentLanguage !== 'en' && currentLanguage !== 'ms') {
                    currentLanguage = 'en';
                }

                console.log('✅ Stored user config:', window.userConfig);
                
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
                
                // Log upload count update for debugging
                console.log(`📊 Upload count updated via CONFIG: ${{userConfig.uploadsUsed}}/${{userConfig.uploadsLimit}} used (${{userConfig.uploadsRemaining}} remaining)`);
                
                // CRITICAL: Update session state upload counts from CONFIG message
                // This ensures Python code has the latest values
                try {{
                    // Update URL params which will be read by initialize_integration()
                    const url = new URL(window.location.href);
                    url.searchParams.set('uploadsUsed', String(userConfig.uploadsUsed));
                    url.searchParams.set('uploadsLimit', String(userConfig.uploadsLimit));
                    url.searchParams.set('uploadsRemaining', String(userConfig.uploadsRemaining));
                    url.searchParams.set('uploadLimitExceeded', String(userConfig.uploadLimitExceeded));
                    window.history.replaceState({{}}, '', url);
                    console.log('✅ Updated URL params with CONFIG upload counts');
                }} catch (urlError) {{
                    console.warn('⚠️ Could not update URL params:', urlError);
                }}
                
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
            
            // Handle CONFIG updates without reload (for upload count updates)
            if (data.type === 'CONFIG_UPDATE' || (data.type === 'CONFIG' && !data.fullReload)) {
                console.log('📥 Received CONFIG_UPDATE from website:', data);

                // Update upload counts without full reload
                if (data.uploadsUsed !== undefined || data.uploadsLimit !== undefined) {
                    const newUsed = data.uploadsUsed !== undefined ? data.uploadsUsed : parseInt(localStorage.getItem('cropdrive_uploadsUsed') || '0');
                    const newLimit = data.uploadsLimit !== undefined ? data.uploadsLimit : parseInt(localStorage.getItem('cropdrive_uploadsLimit') || '0');
                    const newRemaining = data.uploadsRemaining !== undefined ? data.uploadsRemaining : Math.max(0, newLimit - newUsed);

                    // Update window.userConfig if it exists
                    if (window.userConfig) {
                        window.userConfig.uploadsUsed = newUsed;
                        window.userConfig.uploadsLimit = newLimit;
                        window.userConfig.uploadLimitExceeded = data.uploadLimitExceeded || false;
                        window.userConfig.uploadsRemaining = newRemaining;
                    }

                    localStorage.setItem('cropdrive_uploadsUsed', String(newUsed));
                    localStorage.setItem('cropdrive_uploadsLimit', String(newLimit));
                    localStorage.setItem('cropdrive_uploadsRemaining', String(newRemaining));

                    // Update URL params
                    const url = new URL(window.location.href);
                    url.searchParams.set('uploadsUsed', String(newUsed));
                    url.searchParams.set('uploadsLimit', String(newLimit));
                    url.searchParams.set('uploadsRemaining', String(newRemaining));
                    window.history.replaceState({}, '', url);

                    console.log(`📊 Upload count updated: ${{newUsed}}/${{newLimit}} used (${{newRemaining}} remaining)`);
                    console.log('✅ Updated window.userConfig:', window.userConfig);

                    // Trigger Streamlit rerun to update UI
                    if (window.parent !== window) {{
                        window.parent.postMessage({{
                            type: 'STREAMLIT_RERUN'
                        }}, '*');
                    }}
                }
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
        
        // Helper function for safe postMessage
        function safePostMessage(message, targetOrigin = '*') {
            try {
                window.parent.postMessage(message, targetOrigin);
                console.log(`✅ Message sent to ${targetOrigin}:`, message.type);
                return true;
            } catch (error) {
                console.error(`❌ Failed to send message to ${targetOrigin}:`, error);
                return false;
            }
        }

        // Helper function to send ANALYSIS_COMPLETE message
        function sendAnalysisComplete(analysisResults) {
            console.log('📊 Sending ANALYSIS_COMPLETE message:', analysisResults);

            const message = {
                type: 'ANALYSIS_COMPLETE',
                userId: window.userConfig?.userId, // Must be included
                title: `Analysis Report - ${new Date().toLocaleDateString()}`,
                analysisType: analysisResults.analysisType || 'soil', // 'soil', 'leaf', or 'both'
                summary: analysisResults.summary || '',
                recommendationsCount: analysisResults.recommendationsCount || 0,
                fileUrl: analysisResults.fileUrl || null,
                analysisData: analysisResults.analysisData || null,
                timestamp: new Date().toISOString()
            };

            // CRITICAL: Use '*' as target origin
            try {
                window.parent.postMessage(message, '*');
                console.log('✅ ANALYSIS_COMPLETE message sent successfully');

                // Store as backup in case message fails
                try {
                    sessionStorage.setItem('analysis_results', JSON.stringify(message));
                    console.log('💾 Stored analysis results in sessionStorage');
                } catch (e) {
                    console.log('⚠️ Could not store in sessionStorage');
                }
            } catch (error) {
                console.error('❌ Error sending ANALYSIS_COMPLETE:', error);

                // Fallback: Try with specific origin
                try {
                    window.parent.postMessage(message, 'https://www.cropdrive.ai');
                    console.log('✅ ANALYSIS_COMPLETE sent with specific origin');
                } catch (e2) {
                    console.error('❌ Failed with specific origin too');
                }
            }
        }

        // Helper function to request config update
        function requestConfigUpdate() {
            console.log('📤 Requesting updated CONFIG from parent...');

            const message = {
                type: 'REQUEST_CONFIG_UPDATE',
                userId: window.userConfig?.userId,
                timestamp: Date.now()
            };

            try {
                window.parent.postMessage(message, '*');
                console.log('✅ REQUEST_CONFIG_UPDATE sent');
            } catch (error) {
                console.error('❌ Error sending REQUEST_CONFIG_UPDATE:', error);
            }
        }

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
    # CRITICAL: Always update from URL params to get latest values (parent may have updated them)
    # This ensures upload counts stay in sync with parent website
    st.session_state.uploads_used = uploads_used
    st.session_state.uploads_limit = uploads_limit
    st.session_state.upload_limit_exceeded = upload_limit_exceeded
    st.session_state.uploads_remaining = uploads_remaining
    
    # Log upload count for debugging
    logger.info(f"📊 Upload count from URL: {uploads_used}/{uploads_limit} used ({uploads_remaining} remaining)")
    
    return current_lang, user_plan, features

# ============================================================================
# STEP 3: Send analysis results to website
# ============================================================================

def request_config_update():
    """
    Request updated CONFIG from parent window after analysis
    This should be called after analysis completes to get updated upload counts
    """
    from streamlit.components.v1 import html
    import json

    request_js = f"""
    <script>
    (function() {{
        console.log('📤 Requesting updated CONFIG from parent...');

        const message = {{
            type: 'REQUEST_CONFIG_UPDATE',
            userId: window.userConfig?.userId || '',
            timestamp: Date.now()
        }};

        try {{
            window.parent.postMessage(message, '*');
            console.log('✅ REQUEST_CONFIG_UPDATE sent');
        }} catch (error) {{
            console.error('❌ Error sending REQUEST_CONFIG_UPDATE:', error);
        }}
    }})();
    </script>
    """

    html(request_js, height=0)

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
    CRITICAL: Send analysis completion message to parent window (CropDrive website)
    This message MUST be sent after every analysis completes to save results to Firestore.
    
    Args:
        title: Report title (required)
        analysis_type: 'soil' or 'leaf' (required)
        summary: Analysis summary text (optional)
        recommendations_count: Number of recommendations (optional)
        file_url: URL to analysis file/image (optional)
        analysis_data: Additional analysis data dict (optional)
    """
    from datetime import datetime
    
    try:
        # Include user ID in the analysis complete message
        user_id = get_user_id()
        user_email = get_user_email()
        
        # Log user info for debugging
        logger.info(f"🔍 DEBUG send_analysis_complete - user_id: {user_id}, user_email: {user_email}")
        
        # Warn if user_id is missing (critical for parent page to save analysis)
        if not user_id:
            logger.warning("⚠️ WARNING: user_id is empty when sending ANALYSIS_COMPLETE message!")
            logger.warning(f"⚠️ Session state user_id: {st.session_state.get('user_id', 'NOT SET')}")
            logger.warning(f"⚠️ Session state user_config: {st.session_state.get('user_config', {})}")
            # Try to get from URL params as fallback
            try:
                query_params = st.query_params
                user_id = query_params.get('userId', '')
                if user_id:
                    logger.info(f"✅ Retrieved user_id from URL params: {user_id}")
            except Exception as e:
                logger.error(f"❌ Failed to get user_id from URL params: {e}")
        
        # Ensure analysis_data includes timestamp if not already present
        if analysis_data is None:
            analysis_data = {}
        
        # Add timestamp if not already in analysis_data
        if 'timestamp' not in analysis_data:
            analysis_data['timestamp'] = datetime.now().isoformat()
        
        # Ensure userId is in analysis_data as well (for redundancy)
        if 'userId' not in analysis_data:
            analysis_data['userId'] = user_id
        
        # CRITICAL: Build message with exact format expected by parent page
        message = {
            'type': 'ANALYSIS_COMPLETE',  # MUST be exactly this string
            'userId': user_id or '',  # CRITICAL: Must match current authenticated user
            'title': title or f'Analysis Report - {datetime.now().strftime("%Y-%m-%d")}',
            'analysisType': analysis_type or 'soil',  # 'soil', 'leaf', or 'both'
            'summary': summary or '',
            'recommendationsCount': int(recommendations_count) if recommendations_count else 0,
            'fileUrl': file_url or None,
            'analysisData': analysis_data or {},
            'timestamp': datetime.now().isoformat()
        }

        # Store as backup in case message fails
        try:
            sessionStorage.setItem('analysis_results', json.dumps(message))
            logger.info('💾 Stored analysis results in sessionStorage')
        except Exception as e:
            logger.warning(f'⚠️ Could not store in sessionStorage: {e}')
        
        # Log the complete message for debugging
        logger.info(f"📤 Sending ANALYSIS_COMPLETE message: userId={user_id}, title={title}, type={analysis_type}")
        logger.info(f"📤 Message payload: {json.dumps(message, indent=2)}")
        
        # CRITICAL: Use '*' as target origin to avoid origin mismatch errors
        # The parent page will verify the message origin on its side
        send_js = f"""
        <script>
        (function() {{
            try {{
                const message = {json.dumps(message)};
                console.log('📊 Sending ANALYSIS_COMPLETE message:', message);

                // CRITICAL: Use '*' as target origin
                try {{
                    window.parent.postMessage(message, '*');
                    console.log('✅ ANALYSIS_COMPLETE message sent successfully');

                    // Store as backup in case message fails
                    try {{
                        sessionStorage.setItem('analysis_results', JSON.stringify(message));
                        console.log('💾 Stored analysis results in sessionStorage');
                    }} catch (e) {{
                        console.log('⚠️ Could not store in sessionStorage');
                    }}
                }} catch (error) {{
                    console.error('❌ Error sending ANALYSIS_COMPLETE:', error);

                    // Fallback: Try with specific origin
                    try {{
                        window.parent.postMessage(message, 'https://www.cropdrive.ai');
                        console.log('✅ ANALYSIS_COMPLETE sent with specific origin');
                    }} catch (e2) {{
                        console.error('❌ Failed with specific origin too');
                    }}
                }}
            }} catch (error) {{
                console.error('❌ Critical error in ANALYSIS_COMPLETE sending:', error);
            }}
        }})();
        </script>
        """
        
        # Inject the JavaScript
        html(send_js, height=0)
        
        # Also log in Python
        logger.info(f"✅ ANALYSIS_COMPLETE message HTML injected for user_id: {user_id}")
        
        # Show success message to user
        st.success("✅ Analysis completed! Results are being saved...")
        
    except Exception as e:
        # CRITICAL: Log error but don't fail silently
        logger.error(f"❌ CRITICAL ERROR in send_analysis_complete: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        
        # Try to send a minimal message even if there's an error
        try:
            minimal_message = {
                'type': 'ANALYSIS_COMPLETE',
                'userId': get_user_id() or '',
                'title': title or 'Analysis Report',
                'analysisType': analysis_type or 'soil',
                'summary': summary or '',
                'recommendationsCount': 0,
                'fileUrl': None,
                'analysisData': {},
                'timestamp': datetime.now().isoformat()
            }
            
            fallback_js = f"""
            <script>
            try {{
                const message = {json.dumps(minimal_message)};
                console.error('⚠️ Sending minimal ANALYSIS_COMPLETE message due to error');
                if (window.parent && window.parent !== window) {{
                    window.parent.postMessage(message, '*');
                    console.log('✅ Minimal ANALYSIS_COMPLETE message sent');
                }}
            }} catch (e) {{
                console.error('❌ Failed to send minimal ANALYSIS_COMPLETE:', e);
            }}
            </script>
            """
            html(fallback_js, height=0)
        except Exception as fallback_error:
            logger.error(f"❌ Failed to send fallback ANALYSIS_COMPLETE message: {fallback_error}")

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
    
    # IMPORTANT: Use '*' as target origin to avoid origin mismatch errors
    # The parent page will verify the message origin on its side
    send_js = f"""
    <script>
    window.parent.postMessage({json.dumps(message)}, '*');
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

def send_script_run_state_changed(script_run_state: str, progress: int = None, message: str = None) -> None:
    """
    Send analysis progress/status update to parent window
    This is useful for showing progress bars and status messages during long-running analyses

    Args:
        script_run_state: 'running' or 'notRunning'
        progress: Optional progress percentage (0-100)
        message: Optional status message
    """
    message_data = {
        'type': 'SCRIPT_RUN_STATE_CHANGED',
        'scriptRunState': script_run_state
    }

    if progress is not None:
        message_data['progress'] = progress
    if message is not None:
        message_data['message'] = message

    # IMPORTANT: Use '*' as target origin to avoid origin mismatch errors
    # The parent page will verify the message origin on its side
    send_js = f"""
    <script>
    window.parent.postMessage({json.dumps(message_data)}, '*');
    console.log('📤 Sent script run state changed:', {json.dumps(message_data)});
    </script>
    """

    html(send_js, height=0)

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

    # IMPORTANT: Use '*' as target origin to avoid origin mismatch errors
    # The parent page will verify the message origin on its side
    send_js = f"""
    <script>
    window.parent.postMessage({json.dumps(message)}, '*');
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

# ============================================================================
# STEP 5: Additional Message Types
# ============================================================================

def send_error_message(error_message: str) -> None:
    """
    Send error status message to parent window

    Args:
        error_message: Error message to display
    """
    send_script_run_state_changed(
        script_run_state='error',
        message=f"Error: {error_message}"
    )

def send_progress_update(current_step: int, total_steps: int, status_message: str = None) -> None:
    """
    Send progress update during analysis

    Args:
        current_step: Current step number
        total_steps: Total number of steps
        status_message: Optional status message
    """
    progress = int((current_step / total_steps) * 100)
    message = status_message or f"Processing step {current_step} of {total_steps}"

    send_script_run_state_changed(
        script_run_state='running',
        progress=progress,
        message=message
    )

# ============================================================================
# STEP 6: Safe postMessage utility
# ============================================================================

def safe_post_message(message, target_origin='*'):
    """
    Safely send postMessage with comprehensive error handling

    Args:
        message: Message object to send
        target_origin: Target origin for postMessage ('*' by default)
    """
    from streamlit.components.v1 import html
    import json

    safe_js = f"""
    <script>
    (function() {{
        const message = {json.dumps(message)};
        const targetOrigin = '{target_origin}';

        try {{
            window.parent.postMessage(message, targetOrigin);
            console.log(`✅ Message sent to ${{targetOrigin}}:`, message.type);
            return true;
        }} catch (error) {{
            console.error(`❌ Failed to send message to ${{targetOrigin}}:`, error);
            return false;
        }}
    }})();
    </script>
    """

    html(safe_js, height=0)

