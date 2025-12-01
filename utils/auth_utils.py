import streamlit as st
import hashlib
import secrets
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from firebase_admin import auth, firestore
from firebase_config import get_firestore_client, COLLECTIONS
from google.cloud.firestore import FieldFilter

class AuthManager:
    """Handle user authentication and management"""
    
    def __init__(self):
        pass  # Defer any Firestore operations until after Firebase is initialized
    
    def _get_db(self):
        """Get Firestore client with proper error handling"""
        db = get_firestore_client()
        if not db:
            raise Exception("Failed to get Firestore client. Firebase may not be initialized.")
        return db
    
    def _verify_admin_code(self, admin_code: str) -> bool:
        """Verify admin code from Firestore
        
        Args:
            admin_code: Admin access code to verify
            
        Returns:
            bool: True if code is valid and not expired, False otherwise
        """
        try:
            from datetime import timezone
            db = self._get_db()
            admin_codes_ref = db.collection(COLLECTIONS['admin_codes'])
            
            # Find matching admin code
            matching_codes = admin_codes_ref.where(filter=FieldFilter('code', '==', admin_code)).limit(1).get()
            
            for doc in matching_codes:
                code_data = doc.to_dict()
                
                # Check if code is expired
                expires_at = code_data.get('expires_at')
                if expires_at:
                    # Handle both timezone-aware and naive datetime objects
                    if hasattr(expires_at, 'tzinfo') and expires_at.tzinfo is not None:
                        current_time = datetime.now(timezone.utc)
                    else:
                        current_time = datetime.now()
                    
                    if expires_at <= current_time:
                        return False
                
                # Check if code is already used (optional - you can remove this if codes can be reused)
                if code_data.get('used', False):
                    return False
                
                return True
            
            return False  # Code not found
            
        except Exception as e:
            print(f"Error verifying admin code: {str(e)}")
            return False
    
    def login(self, email: str, password: str) -> Dict[str, Any]:
        """Authenticate user login
        
        Args:
            email: User email
            password: User password
            
        Returns:
            dict: Login result with success status and user info
        """
        try:
            # Get user from Firestore
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            user_query = users_ref.where(filter=FieldFilter('email', '==', email.lower())).limit(1)
            users = user_query.get()
            
            if not users:
                return {'success': False, 'message': 'User not found'}
            
            user_doc = users[0]
            user_data = user_doc.to_dict()
            
            # Verify password
            if self._verify_password(password, user_data.get('password_hash', '')):
                # Check if account is active
                if not user_data.get('is_active', True):
                    return {'success': False, 'message': 'Account is deactivated'}
                
                # Update last login
                user_doc.reference.update({
                    'last_login': datetime.now(),
                    'login_count': user_data.get('login_count', 0) + 1
                })
                
                # Return user info
                user_info = {
                    'uid': user_doc.id,
                    'email': user_data['email'],
                    'name': user_data['name'],
                    'role': user_data.get('role', 'user'),
                    'company': user_data.get('company', ''),
                    'created_at': user_data.get('created_at'),
                    'last_login': datetime.now()
                }
                
                return {'success': True, 'user_info': user_info, 'message': 'Login successful'}
            else:
                return {'success': False, 'message': 'Invalid password'}
                
        except Exception as e:
            return {'success': False, 'message': f'Login error: {str(e)}'}
    
    def signup(self, email: str, password: str, name: str, company: str = '') -> Dict[str, Any]:
        """Register new user
        
        Args:
            email: User email
            password: User password
            name: User full name
            company: User company (optional)
            
        Returns:
            dict: Signup result with success status
        """
        try:
            # Validate input
            if len(password) < 6:
                return {'success': False, 'message': 'Password must be at least 6 characters'}
            
            # Check if user already exists
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            existing_user = users_ref.where(filter=FieldFilter('email', '==', email.lower())).limit(1).get()
            
            if existing_user:
                return {'success': False, 'message': 'User already exists with this email'}
            
            # Create user document
            user_data = {
                'email': email.lower(),
                'name': name,
                'company': company,
                'password_hash': self._hash_password(password),
                'role': 'user',
                'is_active': True,
                'created_at': datetime.now(),
                'last_login': None,
                'login_count': 0,
                'analyses_count': 0
            }
            
            # Add user to Firestore
            doc_ref = users_ref.add(user_data)
            
            return {'success': True, 'message': 'Account created successfully'}
            
        except Exception as e:
            return {'success': False, 'message': f'Signup error: {str(e)}'}
    
    def admin_signup_with_code(self, email: str, password: str, name: str, organization: str, admin_code: str) -> Dict[str, Any]:
        """Register new admin user
        
        Args:
            email: Admin email
            password: Admin password
            name: Admin full name
            organization: Admin organization
            admin_code: Admin access code
            
        Returns:
            dict: Admin signup result
        """
        try:
            # Verify admin access code from Firestore
            if not self._verify_admin_code(admin_code):
                return {'success': False, 'message': 'Invalid or expired admin access code'}
            
            # Validate input
            if len(password) < 8:
                return {'success': False, 'message': 'Admin password must be at least 8 characters'}
            
            # Check if user already exists
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            existing_user = users_ref.where(filter=FieldFilter('email', '==', email.lower())).limit(1).get()
            
            if existing_user:
                return {'success': False, 'message': 'User already exists with this email'}
            
            # Create admin request document
            admin_request_data = {
                'email': email.lower(),
                'name': name,
                'organization': organization,
                'password_hash': self._hash_password(password),
                'status': 'approved',  # Auto-approve for demo
                'requested_at': datetime.now(),
                'approved_at': datetime.now(),
                'approved_by': 'system'
            }
            
            # Add to admin requests collection
            admin_requests_ref = db.collection(COLLECTIONS['admin_requests'])
            admin_requests_ref.add(admin_request_data)
            
            # Create admin user directly (for demo purposes)
            user_data = {
                'email': email.lower(),
                'name': name,
                'company': organization,
                'password_hash': self._hash_password(password),
                'role': 'admin',
                'is_active': True,
                'created_at': datetime.now(),
                'last_login': None,
                'login_count': 0,
                'analyses_count': 0
            }
            
            # Add admin user to Firestore
            users_ref.add(user_data)
            
            return {'success': True, 'message': 'Admin account created successfully'}
            
        except Exception as e:
            return {'success': False, 'message': f'Admin signup error: {str(e)}'}
    
    def admin_signup(self, email: str, password: str, name: str, organization: str = '') -> Dict[str, Any]:
        """Register new admin user without admin code requirement
        
        Args:
            email: Admin email
            password: Admin password
            name: Admin full name
            organization: Admin organization (optional)
            
        Returns:
            dict: Admin signup result
        """
        try:
            # Validate input
            if len(password) < 8:
                return {'success': False, 'message': 'Admin password must be at least 8 characters'}
            
            if not name.strip():
                return {'success': False, 'message': 'Name is required'}
            
            if not email.strip():
                return {'success': False, 'message': 'Email is required'}
            
            # Check if user already exists
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            existing_user = users_ref.where(filter=FieldFilter('email', '==', email.lower())).limit(1).get()
            
            if existing_user:
                return {'success': False, 'message': 'User already exists with this email'}
            
            # Create admin request document
            admin_request_data = {
                'email': email.lower(),
                'name': name,
                'organization': organization,
                'password_hash': self._hash_password(password),
                'status': 'approved',  # Auto-approve for demo
                'requested_at': datetime.now(),
                'approved_at': datetime.now(),
                'approved_by': 'system'
            }
            
            # Add to admin requests collection
            admin_requests_ref = db.collection(COLLECTIONS['admin_requests'])
            admin_requests_ref.add(admin_request_data)
            
            # Create admin user directly
            user_data = {
                'email': email.lower(),
                'name': name,
                'company': organization,
                'password_hash': self._hash_password(password),
                'role': 'admin',
                'is_active': True,
                'created_at': datetime.now(),
                'last_login': None,
                'login_count': 0,
                'analyses_count': 0
            }
            
            # Add admin user to Firestore
            users_ref.add(user_data)
            
            return {'success': True, 'message': 'Admin account created successfully'}
            
        except Exception as e:
            return {'success': False, 'message': f'Admin signup error: {str(e)}'}
    
    def reset_password(self, email: str) -> Dict[str, Any]:
        """Send password reset link
        
        Args:
            email: User email
            
        Returns:
            dict: Reset result
        """
        try:
            # Check if user exists
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            user_query = users_ref.where(filter=FieldFilter('email', '==', email.lower())).limit(1)
            users = user_query.get()
            
            if not users:
                return {'success': False, 'message': 'User not found'}
            
            # Generate reset token
            reset_token = secrets.token_urlsafe(32)
            reset_expires = datetime.now() + timedelta(hours=1)
            
            # Update user with reset token
            user_doc = users[0]
            user_doc.reference.update({
                'reset_token': reset_token,
                'reset_expires': reset_expires
            })

            # Attempt to send reset email if SMTP is configured
            reset_link = self._build_reset_link(reset_token)
            email_result = self._send_reset_email(email, reset_link)

            if email_result.get('success'):
                return {
                    'success': True,
                    'message': 'Password reset link sent to your email',
                    'reset_link': reset_link
                }
            else:
                # Enforce email sending requirement, but include link for manual use/debugging
                return {
                    'success': False,
                    'message': email_result.get('message', 'Failed to send reset email'),
                    'reset_link': reset_link
                }
            
        except Exception as e:
            return {'success': False, 'message': f'Reset error: {str(e)}'}
    
    def change_password(self, user_id: str, old_password: str, new_password: str) -> Dict[str, Any]:
        """Change user password
        
        Args:
            user_id: User document ID
            old_password: Current password
            new_password: New password
            
        Returns:
            dict: Change password result
        """
        try:
            # Get user document
            db = self._get_db()
            user_ref = db.collection(COLLECTIONS['users']).document(user_id)
            user_doc = user_ref.get()
            
            if not user_doc.exists:
                return {'success': False, 'message': 'User not found'}
            
            user_data = user_doc.to_dict()
            
            # Verify old password
            if not self._verify_password(old_password, user_data.get('password_hash', '')):
                return {'success': False, 'message': 'Current password is incorrect'}
            
            # Validate new password
            if len(new_password) < 6:
                return {'success': False, 'message': 'New password must be at least 6 characters'}
            
            # Update password
            user_ref.update({
                'password_hash': self._hash_password(new_password),
                'password_changed_at': datetime.now()
            })
            
            return {'success': True, 'message': 'Password changed successfully'}
            
        except Exception as e:
            return {'success': False, 'message': f'Password change error: {str(e)}'}

    def finalize_password_reset(self, token: str, new_password: str) -> Dict[str, Any]:
        """Finalize password reset using token and set a new password.
        Args:
            token: Reset token from email link
            new_password: New password to set
        Returns:
            dict with success and message
        """
        try:
            if not token or not new_password:
                return {'success': False, 'message': 'Invalid token or password'}

            if len(new_password) < 6:
                return {'success': False, 'message': 'Password must be at least 6 characters'}

            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            # Query for user with this reset token
            matching = users_ref.where(filter=FieldFilter('reset_token', '==', token)).limit(1).get()
            if not matching:
                return {'success': False, 'message': 'Invalid or used reset link'}

            user_doc = matching[0]
            user_data = user_doc.to_dict()
            expires = user_data.get('reset_expires')
            if not expires:
                return {'success': False, 'message': 'Reset link has expired'}
            # Normalize timezone for safe comparison
            try:
                if hasattr(expires, 'tzinfo') and expires.tzinfo is not None:
                    from datetime import timezone
                    now_cmp = datetime.now(expires.tzinfo)
                else:
                    now_cmp = datetime.now()
                if expires <= now_cmp:
                    return {'success': False, 'message': 'Reset link has expired'}
            except Exception:
                # Fallback to naive comparison
                if getattr(expires, 'replace', None):
                    expires_naive = expires.replace(tzinfo=None)
                    if expires_naive <= datetime.now():
                        return {'success': False, 'message': 'Reset link has expired'}
                else:
                    return {'success': False, 'message': 'Invalid expiration timestamp'}
                return {'success': False, 'message': 'Reset link has expired'}

            # Update password and clear reset fields
            user_doc.reference.update({
                'password_hash': self._hash_password(new_password),
                'password_changed_at': datetime.now(),
                'reset_token': None,
                'reset_expires': None
            })

            return {'success': True, 'message': 'Your password has been updated'}
        except Exception as e:
            return {'success': False, 'message': f'Reset finalize error: {str(e)}'}
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by document ID
        
        Args:
            user_id: User document ID
            
        Returns:
            dict: User data or None
        """
        try:
            db = self._get_db()
            user_ref = db.collection(COLLECTIONS['users']).document(user_id)
            user_doc = user_ref.get()
            
            if user_doc.exists:
                user_data = user_doc.to_dict()
                user_data['uid'] = user_doc.id
                return user_data
            
            return None
            
        except Exception as e:
            st.error(f"Error getting user: {str(e)}")
            return None
    
    def update_user_profile(self, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update user profile
        
        Args:
            user_id: User document ID
            updates: Fields to update
            
        Returns:
            dict: Update result
        """
        try:
            db = self._get_db()
            user_ref = db.collection(COLLECTIONS['users']).document(user_id)
            
            # Add timestamp
            updates['updated_at'] = datetime.now()
            
            # Update user document
            user_ref.update(updates)
            
            return {'success': True, 'message': 'Profile updated successfully'}
            
        except Exception as e:
            return {'success': False, 'message': f'Update error: {str(e)}'}

    def _build_reset_link(self, token: str) -> str:
        """Build a reset link pointing back to the app with token in query params"""
        try:
            import streamlit as st
            base_url = None
            # Prefer explicit base_url in secrets
            try:
                if hasattr(st, 'secrets') and 'app' in st.secrets:
                    base_url = st.secrets['app'].get('base_url')
            except Exception:
                base_url = None

            # Fallback to a generic localhost if base_url is not configured
            if not base_url:
                base_url = 'http://localhost:8501'

            return f"{base_url}?reset_token={token}"
        except Exception:
            return f"http://localhost:8501?reset_token={token}"

    def _send_reset_email(self, to_email: str, reset_link: str) -> Dict[str, Any]:
        """Send password reset email using SendGrid/SES/SMTP depending on configuration."""
        try:
            import streamlit as st
            # Prepare shared content
            try:
                app_name = st.secrets.get('app', {}).get('name', 'AGS AI Assistant') if hasattr(st, 'secrets') else 'AGS AI Assistant'
            except Exception:
                app_name = 'AGS AI Assistant'
            subject = f"Reset your password for {app_name}"
            text_body = f"""
Hello,\n\nFollow this link to reset your {app_name} password for your {to_email} account.\n\n{reset_link}\n\nIf you didn’t ask to reset your password, you can ignore this email.\n\nThanks,\n\nYour {app_name} team\n"""
            html_body = f"""
            <div style=\"font-family: Arial, sans-serif; background: #f6fff6; padding: 24px;\">
              <div style=\"max-width: 560px; margin: 0 auto; background: white; border: 1px solid #e6efe6; border-radius: 12px; overflow: hidden;\">
                <div style=\"background: linear-gradient(90deg, #2E8B57 0%, #228B22 100%); padding: 16px; text-align: center; color: white;\">
                  <h2 style=\"margin: 0;\">🌴 {app_name}</h2>
                  <p style=\"margin: 4px 0 0 0; opacity: 0.9;\">Reset your password</p>
                </div>
                <div style=\"padding: 24px; color: #244224;\">
                  <p>Hello,</p>
                  <p>Follow this link to reset your <b>{app_name}</b> password for your <b>{to_email}</b> account.</p>
                  <div style=\"text-align: center; margin: 24px 0;\">
                    <a href=\"{reset_link}\" style=\"background: #2E8B57; color: white; padding: 12px 20px; text-decoration: none; border-radius: 8px; display: inline-block;\">Reset Password</a>
                  </div>
                  <p style=\"font-size: 14px; color: #456b45;\">If you didn’t ask to reset your password, you can ignore this email.</p>
                  <p style=\"font-size: 14px; color: #456b45;\">If the button doesn’t work, copy and paste this link into your browser:</p>
                  <p style=\"word-break: break-all; font-size: 12px; color: #2E8B57;\">{reset_link}</p>
                  <p style=\"margin-top: 16px; font-size: 12px; color: #567a56;\">This link will expire in 1 hour.</p>
                </div>
                <div style=\"background: #f2fbf2; padding: 12px; text-align: center; color: #456b45; font-size: 12px;\">
                  © 2025 {app_name}
                </div>
              </div>
            </div>
            """

            # 1) SendGrid
            try:
                if hasattr(st, 'secrets') and 'sendgrid' in st.secrets:
                    sg_cfg = st.secrets['sendgrid']
                    api_key = sg_cfg.get('api_key')
                    sg_from = sg_cfg.get('from_email')
                    if api_key and sg_from:
                        import requests
                        payload = {
                            "personalizations": [{"to": [{"email": to_email}]}],
                            "from": {"email": sg_from, "name": app_name},
                            "subject": subject,
                            "content": [
                                {"type": "text/plain", "value": text_body},
                                {"type": "text/html", "value": html_body}
                            ]
                        }
                        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
                        resp = requests.post("https://api.sendgrid.com/v3/mail/send", json=payload, headers=headers, timeout=15)
                        if resp.status_code in (200, 202):
                            return {'success': True, 'message': 'Email sent via SendGrid'}
            except Exception:
                pass

            # 2) Amazon SES
            try:
                if hasattr(st, 'secrets') and 'ses' in st.secrets:
                    ses_cfg = st.secrets['ses']
                    aws_key = ses_cfg.get('aws_access_key_id')
                    aws_secret = ses_cfg.get('aws_secret_access_key')
                    region = ses_cfg.get('region') or 'us-east-1'
                    ses_from = ses_cfg.get('from_email')
                    if aws_key and aws_secret and ses_from:
                        try:
                            import boto3
                            ses_client = boto3.client('ses', region_name=region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
                            ses_client.send_email(
                                Source=ses_from,
                                Destination={'ToAddresses': [to_email]},
                                Message={
                                    'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                                    'Body': {
                                        'Text': {'Data': text_body, 'Charset': 'UTF-8'},
                                        'Html': {'Data': html_body, 'Charset': 'UTF-8'}
                                    }
                                }
                            )
                            return {'success': True, 'message': 'Email sent via Amazon SES'}
                        except Exception:
                            pass
            except Exception:
                pass

            # 3) SMTP fallback
            smtp_cfg = None
            try:
                if hasattr(st, 'secrets') and 'smtp' in st.secrets:
                    smtp_cfg = st.secrets['smtp']
            except Exception:
                smtp_cfg = None
            if not smtp_cfg:
                return {'success': False, 'message': 'No email provider configured'}

            host = smtp_cfg.get('host')
            port = int(smtp_cfg.get('port', 587))
            username = smtp_cfg.get('username')
            password = smtp_cfg.get('password')
            from_email = smtp_cfg.get('from_email', username)
            # For Gmail SMTP, force From to the authenticated account to avoid rewrites/SPF issues
            try:
                if isinstance(host, str) and 'gmail' in host.lower():
                    from_email = username
            except Exception:
                pass
            if not all([host, port, username, password, from_email]):
                return {'success': False, 'message': 'Incomplete SMTP configuration'}

            message = MIMEMultipart('alternative')
            message['From'] = from_email
            message['To'] = to_email
            message['Subject'] = subject
            message.attach(MIMEText(text_body, 'plain'))
            message.attach(MIMEText(html_body, 'html'))

            use_ssl = bool(smtp_cfg.get('ssl', False)) or int(port) == 465
            if use_ssl:
                with smtplib.SMTP_SSL(host, port) as server:
                    server.login(username, password)
                    server.sendmail(username, to_email, message.as_string())
            else:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(username, to_email, message.as_string())

            return {'success': True, 'message': 'Email sent via SMTP'}
        except Exception as e:
            return {'success': False, 'message': f'Email error: {str(e)}'}
    
    def _hash_password(self, password: str) -> str:
        """Hash password using SHA-256 with salt
        
        Args:
            password: Plain text password
            
        Returns:
            str: Hashed password
        """
        salt = secrets.token_hex(16)
        password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
        return f"{salt}:{password_hash}"
    
    def _verify_password(self, password: str, stored_hash: str) -> bool:
        """Verify password against stored hash
        
        Args:
            password: Plain text password
            stored_hash: Stored password hash
            
        Returns:
            bool: True if password matches
        """
        try:
            if ':' not in stored_hash:
                return False
            
            salt, hash_part = stored_hash.split(':', 1)
            password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
            return password_hash == hash_part
            
        except Exception:
            return False
    
    def is_admin(self, user_info: Dict[str, Any]) -> bool:
        """Check if user is admin
        
        Args:
            user_info: User information dictionary
            
        Returns:
            bool: True if user is admin
        """
        return user_info.get('role') == 'admin'
    
    def _ensure_default_admin(self) -> None:
        """Ensure default admin user exists"""
        try:
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            
            # Check if default admin exists
            admin_query = users_ref.where(filter=FieldFilter('email', '==', 'agsadmin@ags.ai')).limit(1)
            existing_admin = admin_query.get()
            
            if not existing_admin:
                # Create default admin user
                admin_data = {
                    'email': 'agsadmin@ags.ai',
                    'name': 'AGS Admin',
                    'company': 'AGS AI',
                    'password_hash': self._hash_password('agsai123'),
                    'role': 'admin',
                    'is_active': True,
                    'created_at': datetime.now(),
                    'last_login': None,
                    'login_count': 0,
                    'analyses_count': 0
                }
                
                users_ref.add(admin_data)
                print("Default admin user created successfully")
                
        except Exception as e:
            print(f"Error creating default admin: {str(e)}")
    
    def logout_user(self) -> None:
        """Logout current user by clearing session state"""
        # Clear all authentication-related session state
        if hasattr(st, 'session_state'):
            keys_to_clear = [
                'authenticated', 'user_id', 'user_email', 'user_name', 
                'user_role', 'user_info', 'current_analysis', 'auth_token'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
    
    def is_logged_in(self) -> bool:
        """Check if user is logged in"""
        return getattr(st.session_state, 'authenticated', False)
    
    def get_all_users(self) -> list:
        """Get all users (admin only)
        
        Returns:
            list: List of all users
        """
        try:
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            users = users_ref.get()
            
            user_list = []
            for user_doc in users:
                user_data = user_doc.to_dict()
                user_data['uid'] = user_doc.id
                user_list.append(user_data)
            
            return user_list
            
        except Exception as e:
            st.error(f"Error getting users: {str(e)}")
            return []
    
    def register_user(self, email: str, password: str, full_name: str, farm_name: str = "", location: str = "") -> bool:
        """Register new user with farm details
        
        Args:
            email: User email
            password: User password
            full_name: User full name
            farm_name: Farm name (optional)
            location: Farm location (optional)
            
        Returns:
            bool: True if registration successful, False otherwise
        """
        try:
            # Validate input
            if len(password) < 6:
                st.error('Password must be at least 6 characters')
                return False
            
            # Check if user already exists
            db = self._get_db()
            users_ref = db.collection(COLLECTIONS['users'])
            existing_user = users_ref.where(filter=FieldFilter('email', '==', email.lower())).limit(1).get()
            
            if existing_user:
                st.error('User already exists with this email')
                return False
            
            # Create user document
            user_data = {
                'email': email.lower(),
                'name': full_name,
                'company': farm_name,
                'location': location,
                'password_hash': self._hash_password(password),
                'role': 'user',
                'is_active': True,
                'created_at': datetime.now(),
                'last_login': None,
                'login_count': 0,
                'analyses_count': 0
            }
            
            # Add user to Firestore
            doc_ref = users_ref.add(user_data)
            
            return True
            
        except Exception as e:
            st.error(f'Registration error: {str(e)}')
            return False


# Global auth manager instance
auth_manager = AuthManager()

# Standalone functions for backward compatibility
def login_user(email: str, password: str) -> Dict[str, Any]:
    """Login user"""
    return auth_manager.login(email, password)

def register_user(email: str, password: str, full_name: str, farm_name: str = "", location: str = "") -> bool:
    """Register new user"""
    return auth_manager.register_user(email, password, full_name, farm_name, location)

def reset_password(email: str) -> bool:
    """Reset user password"""
    return auth_manager.reset_password(email)

def finalize_password_reset(token: str, new_password: str) -> Dict[str, Any]:
    """Finalize password reset with token"""
    return auth_manager.finalize_password_reset(token, new_password)

def logout_user() -> None:
    """Logout current user"""
    auth_manager.logout_user()

def is_logged_in() -> bool:
    """Check if user is logged in"""
    return auth_manager.is_logged_in()

def is_admin(user_id: str) -> bool:
    """Check if user is admin"""
    user_info = auth_manager.get_user_by_id(user_id)
    if not user_info:
        return False
    return auth_manager.is_admin(user_info)

def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """Get user by ID"""
    return auth_manager.get_user_by_id(user_id)

def update_user_profile(user_id: str, profile_data: Dict[str, Any]) -> bool:
    """Update user profile"""
    return auth_manager.update_user_profile(user_id, profile_data)

def get_all_users() -> list:
    """Get all users (admin only)"""
    return auth_manager.get_all_users()

def admin_signup(email: str, password: str, name: str, organization: str = '') -> Dict[str, Any]:
    """Register new admin user"""
    return auth_manager.admin_signup(email, password, name, organization)

def admin_signup_with_code(email: str, password: str, name: str, organization: str, admin_code: str) -> Dict[str, Any]:
    """Register new admin user with admin code verification"""
    return auth_manager.admin_signup_with_code(email, password, name, organization, admin_code)