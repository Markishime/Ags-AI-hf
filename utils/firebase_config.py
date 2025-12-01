import os
import json
import streamlit as st
from typing import Optional

# Optional Firebase Admin imports
try:
    import firebase_admin
    from firebase_admin import credentials, firestore, auth, storage
    from google.cloud.firestore import FieldFilter
    FIREBASE_AVAILABLE = True
except ImportError as e:
    firebase_admin = None
    credentials = None
    firestore = None
    auth = None
    storage = None
    FieldFilter = None
    FIREBASE_AVAILABLE = False
    print(f"Warning: Firebase Admin SDK not available: {e}")

# Do not load from .env in deployment; rely on Streamlit secrets only

# Avoid global monkey patching of Google Auth entirely

def initialize_firebase() -> bool:
    """Initialize Firebase Admin SDK
    
    Returns:
        bool: True if initialization successful, False otherwise
    """
    if not FIREBASE_AVAILABLE:
        error_msg = "Firebase Admin SDK is not installed. Please install it with: pip install firebase-admin"
        st.error(error_msg)
        print(error_msg)
        return False
    
    try:
        # Prefer project_id from Streamlit secrets
        project_id = None
        if hasattr(st, 'secrets') and 'firebase' in st.secrets:
            project_id = st.secrets.firebase.get('project_id')
        
        # Check if Firebase is already initialized
        if firebase_admin and firebase_admin._apps:
            print("Firebase already initialized")
            return True
        
        print("Initializing Firebase...")
        
        # Get Firebase credentials from environment or Streamlit secrets
        firebase_creds = get_firebase_credentials()
        
        if firebase_creds:
            print(f"Found Firebase credentials for project: {firebase_creds.get('project_id', 'unknown')}")
            
            if not credentials:
                print("Firebase credentials module not available")
                return False
            
            # Initialize Firebase with credentials
            cred = credentials.Certificate(firebase_creds)
            
            # Get storage bucket from Streamlit secrets
            storage_bucket = None
            if hasattr(st, 'secrets') and 'firebase' in st.secrets:
                storage_bucket = (
                    st.secrets.firebase.get('firebase_storage_bucket') or
                    st.secrets.firebase.get('storage_bucket')
                )
            if not storage_bucket and firebase_creds.get('project_id'):
                storage_bucket = f"{firebase_creds.get('project_id')}.firebasestorage.app"
            
            # Initialize Firebase with explicit configuration
            if firebase_admin:
                firebase_admin.initialize_app(cred, {
                    'storageBucket': storage_bucket,
                    'projectId': firebase_creds.get('project_id', project_id or 'agriai-cbd8b')
                })
            else:
                print("Firebase Admin module not available")
                return False
            
            # Do not modify global google.auth defaults; rely on initialized app
            
            print(f"Firebase initialized successfully with storage bucket: {storage_bucket}")
            return True
        else:
            # Provide more specific guidance about missing credentials
            if not hasattr(st, 'secrets'):
                error_msg = "Streamlit secrets not available. Please ensure you're running in a Streamlit environment."
            elif 'firebase' not in st.secrets:
                error_msg = "Firebase section not found in Streamlit secrets. Please add a [firebase] section to your secrets.toml file."
            else:
                firebase_secrets = st.secrets.firebase
                missing_fields = []
                # Check for required fields (matching the field names used in get_firebase_credentials)
                if not firebase_secrets.get('project_id'):
                    missing_fields.append('project_id')
                if not firebase_secrets.get('firebase_private_key'):
                    missing_fields.append('firebase_private_key')
                if not firebase_secrets.get('firebase_client_email'):
                    missing_fields.append('firebase_client_email')
                
                if missing_fields:
                    error_msg = f"Missing required Firebase credentials in Streamlit secrets: {', '.join(missing_fields)}. Please check your secrets.toml file."
                else:
                    error_msg = "Firebase credentials found but failed validation. Please check that your private_key, client_email, and project_id are correct."
            
            st.error(error_msg)
            print(error_msg)
            return False
            
    except Exception as e:
        error_type = type(e).__name__
        error_details = str(e)
        
        # Provide more specific error messages
        if "Certificate" in error_type or "private_key" in error_details.lower():
            error_msg = f"Firebase credential error: Invalid private key or certificate. Please check your Firebase service account credentials in Streamlit secrets."
        elif "project_id" in error_details.lower() or "project" in error_details.lower():
            error_msg = f"Firebase project error: {error_details}. Please verify your project_id in Streamlit secrets."
        elif "already exists" in error_details.lower() or "already initialized" in error_details.lower():
            # Firebase already initialized - this is actually OK
            print(f"Firebase already initialized: {error_details}")
            return True
        else:
            error_msg = f"Failed to initialize Firebase: {error_details}. Please check your configuration in Streamlit secrets."
        
        st.error(error_msg)
        print(f"Failed to initialize Firebase ({error_type}): {error_details}")
        return False

def get_firebase_credentials() -> Optional[dict]:
    """Get Firebase credentials from environment variables or Streamlit secrets
    
    Returns:
        dict: Firebase service account credentials or None
    """
    try:
        # Prefer Streamlit secrets exclusively in deployment
        try:
            if hasattr(st, 'secrets') and 'firebase' in st.secrets:
                print("Loading Firebase credentials from Streamlit secrets")
                firebase_secrets = st.secrets.firebase
                
                # Construct credentials from individual fields in secrets
                firebase_config = {
                    "type": firebase_secrets.get('firebase_type', 'service_account'),
                    "project_id": firebase_secrets.get('project_id'),
                    "private_key_id": firebase_secrets.get('firebase_private_key_id'),
                    "private_key": firebase_secrets.get('firebase_private_key', '').replace('\\n', '\n'),
                    "client_email": firebase_secrets.get('firebase_client_email'),
                    "client_id": firebase_secrets.get('firebase_client_id'),
                    "auth_uri": firebase_secrets.get('firebase_auth_uri', 'https://accounts.google.com/o/oauth2/auth'),
                    "token_uri": firebase_secrets.get('firebase_token_uri', 'https://oauth2.googleapis.com/token'),
                    "auth_provider_x509_cert_url": firebase_secrets.get('firebase_auth_provider_x509_cert_url', 'https://www.googleapis.com/oauth2/v1/certs'),
                    "client_x509_cert_url": firebase_secrets.get('firebase_client_x509_cert_url'),
                    "universe_domain": firebase_secrets.get('firebase_universe_domain', 'googleapis.com')
                }
                
                # Check if all required fields are present
                required_fields = ['project_id', 'private_key', 'client_email']
                missing_fields = [field for field in required_fields if not firebase_config.get(field)]
                
                if not missing_fields:
                    print("Successfully loaded Firebase credentials from Streamlit secrets")
                    return firebase_config
                else:
                    print(f"Missing required Firebase credentials in secrets: {missing_fields}")
                    
            elif hasattr(st, 'secrets') and 'FIREBASE_SERVICE_ACCOUNT_KEY' in st.secrets:
                return dict(st.secrets['FIREBASE_SERVICE_ACCOUNT_KEY'])
        except Exception as e:
            print(f"Error loading Firebase credentials from Streamlit secrets: {e}")
        # No environment fallback in deployment; return None to signal missing secrets
        return None
        
    except Exception as e:
        st.error(f"Error loading Firebase credentials: {str(e)}")
        print(f"Error loading Firebase credentials: {e}")
        return None

def get_firestore_client():
    """Get Firestore client instance
    
    Returns:
        firestore.Client: Firestore client instance
    """
    if not FIREBASE_AVAILABLE:
        return None
    try:
        # Ensure Firebase is initialized before creating the client
        if firebase_admin and not firebase_admin._apps:
            initialize_firebase()
        # Return Firestore client from the initialized Firebase app
        return firestore.client() if firestore else None
    except Exception as e:
        # Firebase initialization handled elsewhere, silently return None
        return None

def get_storage_bucket():
    """Get Firebase Storage bucket instance
    
    Returns:
        storage.Bucket: Storage bucket instance
    """
    if not FIREBASE_AVAILABLE:
        return None
    try:
        # Get credentials explicitly to avoid metadata service
        firebase_creds = get_firebase_credentials()
        if firebase_creds and credentials:
            cred = credentials.Certificate(firebase_creds)
            return storage.bucket(credentials=cred) if storage else None
        else:
            # Fallback to default bucket (should work if Firebase is initialized)
            return storage.bucket() if storage else None
    except Exception as e:
        st.error(f"Failed to get Storage bucket: {str(e)}")
        return None

def get_auth_client():
    """Get Firebase Auth client
    
    Returns:
        auth: Firebase Auth client
    """
    if not FIREBASE_AVAILABLE:
        return None
    try:
        return auth if auth else None
    except Exception as e:
        st.error(f"Failed to get Auth client: {str(e)}")
        return None

# Collection names
COLLECTIONS = {
    'users': 'users',
    'analyses': 'analyses',
    'analysis_results': 'analysis_results',
    'analysis_prompts': 'analysis_prompts',
    'feedback': 'feedback',
    'admin_requests': 'admin_requests',
    'admin_codes': 'admin_codes',
    'reference_documents': 'reference_documents',
    'ai_configuration': 'ai_configuration',
    'reference_materials': 'reference_materials',
    'output_formats': 'output_formats',
    'tagging_config': 'tagging_config',
    'prompt_templates': 'prompt_templates'
}

# Default MPOB standards - Accurate values for Malaysian Oil Palm cultivation (matching actual data format)
DEFAULT_MPOB_STANDARDS = {
    'leaf_standards': {
        'N (%)': {'min': 2.4, 'max': 2.8, 'unit': '%', 'optimal': 2.6},
        'P (%)': {'min': 0.14, 'max': 0.20, 'unit': '%', 'optimal': 0.17},
        'K (%)': {'min': 0.9, 'max': 1.3, 'unit': '%', 'optimal': 1.1},
        'Mg (%)': {'min': 0.25, 'max': 0.45, 'unit': '%', 'optimal': 0.35},
        'Ca (%)': {'min': 0.5, 'max': 0.9, 'unit': '%', 'optimal': 0.7},
        'B (mg/kg)': {'min': 18, 'max': 28, 'unit': 'mg/kg', 'optimal': 23},
        'Cu (mg/kg)': {'min': 8, 'max': 18, 'unit': 'mg/kg', 'optimal': 13},
        'Zn (mg/kg)': {'min': 18, 'max': 35, 'unit': 'mg/kg', 'optimal': 26}
    },
    'soil_standards': {
        'pH': {'min': 4.5, 'max': 6.0, 'unit': '', 'optimal': 5.25},
        'N (%)': {'min': 0.15, 'max': 0.25, 'unit': '%', 'optimal': 0.20},
        'Org. C (%)': {'min': 1.5, 'max': 2.5, 'unit': '%', 'optimal': 2.0},
        'Total P (mg/kg)': {'min': 15, 'max': 25, 'unit': 'mg/kg', 'optimal': 20},
        'Avail P (mg/kg)': {'min': 10, 'max': 20, 'unit': 'mg/kg', 'optimal': 15},
        'Exch. K (meq%)': {'min': 0.20, 'max': 0.40, 'unit': 'meq%', 'optimal': 0.30},
        'Exch. Ca (meq%)': {'min': 2.0, 'max': 4.0, 'unit': 'meq%', 'optimal': 3.0},
        'Exch. Mg (meq%)': {'min': 0.6, 'max': 1.2, 'unit': 'meq%', 'optimal': 0.9},
        'CEC (meq%)': {'min': 15, 'max': 25, 'unit': 'meq%', 'optimal': 20}
    }
}

def initialize_admin_codes():
    """Initialize default admin codes in Firestore"""
    import secrets
    import string
    from datetime import datetime, timedelta, timezone
    
    try:
        db = get_firestore_client()
        if not db:
            print("Failed to get Firestore client")
            return None
        
        admin_codes_ref = db.collection(COLLECTIONS['admin_codes'])
        
        # Check if default admin code already exists
        if FieldFilter:
            existing_codes = admin_codes_ref.where(filter=FieldFilter('is_default', '==', True)).limit(1).get()
        else:
            # Fallback if FieldFilter is not available
            existing_codes = admin_codes_ref.where('is_default', '==', True).limit(1).get()
        
        if existing_codes:
            # Return existing default code
            for doc in existing_codes:
                code_data = doc.to_dict()
                expires_at = code_data.get('expires_at')
                # Handle both timezone-aware and naive datetime objects
                if expires_at:
                    if hasattr(expires_at, 'tzinfo') and expires_at.tzinfo is not None:
                        current_time = datetime.now(timezone.utc)
                    else:
                        current_time = datetime.now()
                    
                    if expires_at > current_time:
                        print("Default admin code already exists")
                        return code_data['code']
        
        # Generate new default admin code
        alphabet = string.ascii_uppercase + string.digits
        default_code = ''.join(secrets.choice(alphabet) for _ in range(8))
        
        # Set expiration to 1 year from now (timezone-aware)
        current_time = datetime.now(timezone.utc)
        expires_at = current_time + timedelta(days=365)
        
        # Store in Firestore
        admin_code_data = {
            'code': default_code,
            'is_default': True,
            'created_at': current_time,
            'expires_at': expires_at,
            'used': False,
            'created_by': 'system',
            'description': 'Default admin registration code'
        }
        
        admin_codes_ref.add(admin_code_data)
        print("Default admin code created successfully")
        return default_code
        
    except Exception as e:
        print(f"Error initializing admin codes: {str(e)}")
        return None