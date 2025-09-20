import streamlit as st
import os
import email
from email import policy
from email.parser import BytesParser
import base64
from datetime import datetime
import re
import html
import pandas as pd
from email.utils import parsedate_tz, mktime_tz
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import concurrent.futures
import threading
from concurrent.futures import ThreadPoolExecutor
import hashlib

# Load environment variables for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, which is fine for cloud deployment

def check_authentication():
    """Check if user is authenticated"""
    return st.session_state.get('authenticated', False)

def authenticate_user(username, password):
    """Authenticate user with username and password"""
    try:
        # Try Streamlit secrets first (cloud), then environment variables (local)
        if hasattr(st, 'secrets') and 'AUTH_USERNAME' in st.secrets:
            stored_username = st.secrets["AUTH_USERNAME"]
            stored_password = st.secrets["AUTH_PASSWORD"]
        else:
            stored_username = os.getenv("AUTH_USERNAME")
            stored_password = os.getenv("AUTH_PASSWORD")
        
        if not stored_username or not stored_password:
            st.error("Authentication not configured. Please set AUTH_USERNAME and AUTH_PASSWORD.")
            return False
        
        # Simple authentication check
        if username == stored_username and password == stored_password:
            st.session_state['authenticated'] = True
            return True
        else:
            return False
    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
        return False

def login_form():
    """Display login form"""
    st.title("🔐 EML File Viewer - Login")
    st.markdown("---")
    
    with st.form("login_form"):
        st.markdown("### Please enter your credentials")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit_button = st.form_submit_button("Login")
        
        if submit_button:
            if authenticate_user(username, password):
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid username or password")

def logout():
    """Logout user"""
    st.session_state['authenticated'] = False
    st.rerun()

def parse_and_format_date(date_string):
    """Parse email date and format it as DD/MM/YYYY HH:MM:SS"""
    try:
        # Parse the email date
        parsed_date = parsedate_tz(date_string)
        if parsed_date:
            # Convert to timestamp and then to datetime
            timestamp = mktime_tz(parsed_date)
            dt = datetime.fromtimestamp(timestamp)
            # Format as DD/MM/YYYY HH:MM:SS
            return dt.strftime('%d/%m/%Y %H:%M:%S'), dt
        else:
            return date_string, None
    except:
        return date_string, None

def get_s3_client():
    """Initialize S3 client using Streamlit secrets or environment variables"""
    try:
        # Try Streamlit secrets first (for cloud deployment)
        if hasattr(st, 'secrets') and 'AWS_ACCESS_KEY_ID' in st.secrets:
            return boto3.client(
                's3',
                aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
                region_name=st.secrets.get("AWS_REGION", "us-east-1")
            )
        # Fall back to environment variables (for local development)
        else:
            return boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "us-east-1")
            )
    except Exception as e:
        st.error(f"Error connecting to S3: {str(e)}")
        return None

def list_eml_files_from_s3(bucket_name, folder_prefix=""):
    """List all EML files from S3 bucket"""
    s3_client = get_s3_client()
    if not s3_client:
        return []
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_prefix
        )
        
        eml_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].lower().endswith('.eml'):
                    eml_files.append(obj['Key'])
        
        return eml_files
    except ClientError as e:
        st.error(f"Error listing files from S3: {str(e)}")
        return []

def download_eml_from_s3(bucket_name, file_key):
    """Download EML file content from S3"""
    s3_client = get_s3_client()
    if not s3_client:
        return None
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        return response['Body'].read()
    except ClientError as e:
        st.error(f"Error downloading {file_key} from S3: {str(e)}")
        return None

def parse_s3_eml(file_content, filename):
    """Parse an EML file from S3 and extract its content."""
    try:
        msg = BytesParser(policy=policy.default).parsebytes(file_content)
        
        # Extract basic information
        subject = msg.get('Subject', 'No Subject')
        sender = msg.get('From', 'Unknown Sender')
        recipient = msg.get('To', 'Unknown Recipient')
        date_raw = msg.get('Date', 'Unknown Date')
        
        # Parse and format the date
        date_formatted, date_obj = parse_and_format_date(date_raw)
        
        # Extract body content
        body_text = ""
        body_html = ""
        attachments = []
        
        # Walk through all parts of the email
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get('Content-Disposition', ''))
            
            # Extract text and HTML body
            if content_type == 'text/plain' and 'attachment' not in content_disposition:
                body_text = part.get_content()
            elif content_type == 'text/html' and 'attachment' not in content_disposition:
                body_html = part.get_content()
            
            # Extract attachments
            elif 'attachment' in content_disposition or part.get_filename():
                attach_filename = part.get_filename()
                if attach_filename:
                    try:
                        content = part.get_content()
                        if isinstance(content, str):
                            content = content.encode()
                        attachments.append({
                            'filename': attach_filename,
                            'content': content,
                            'content_type': content_type
                        })
                    except Exception as e:
                        # Use a placeholder for failed attachments instead of showing error
                        pass
        
        return {
            'subject': subject,
            'sender': sender,
            'recipient': recipient,
            'date': date_formatted,
            'date_obj': date_obj,  # Keep datetime object for sorting
            'body_text': body_text,
            'body_html': body_html,
            'attachments': attachments
        }
    
    except Exception as e:
        return None

def process_single_email(bucket_name, file_key):
    """Process a single email file - for parallel processing"""
    try:
        file_content = download_eml_from_s3(bucket_name, file_key)
        if file_content:
            return parse_s3_eml(file_content, file_key)
        return None
    except Exception as e:
        return None

def process_emails_parallel(bucket_name, eml_files, max_workers=5):
    """Process emails in parallel using ThreadPoolExecutor"""
    all_emails_data = []
    max_attachments = 0
    
    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_single_email, bucket_name, file_key): file_key 
            for file_key in eml_files
        }
        
        completed = 0
        total = len(eml_files)
        
        # Process completed tasks
        for future in concurrent.futures.as_completed(future_to_file):
            file_key = future_to_file[future]
            completed += 1
            
            # Update progress
            progress_bar.progress(completed / total)
            status_text.text(f"Processed {completed}/{total} files...")
            
            try:
                email_data = future.result()
                if email_data is not None:
                    all_emails_data.append(email_data)
                    # Track maximum number of attachments for table columns
                    if len(email_data['attachments']) > max_attachments:
                        max_attachments = len(email_data['attachments'])
            except Exception as e:
                # Silently skip failed emails
                pass
    
    progress_bar.empty()
    status_text.empty()
    
    return all_emails_data, max_attachments

def clean_html(html_content):
    """Clean HTML content for safe display."""
    if not html_content:
        return ""
    
    # Remove script and style tags
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove potentially dangerous attributes
    html_content = re.sub(r'on\w+="[^"]*"', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r"on\w+='[^']*'", '', html_content, flags=re.IGNORECASE)
    
    return html_content

def create_download_link(content, filename, content_type):
    """Create a download link for attachments."""
    if isinstance(content, str):
        content = content.encode()
    
    b64_content = base64.b64encode(content).decode()
    
    return f'''<a href="data:{content_type};base64,{b64_content}" download="{filename}" style="display: inline-block; padding: 4px 8px; margin: 2px; background-color: #0066cc; color: white; text-decoration: none; border-radius: 4px; font-size: 12px; white-space: nowrap;">📎 {filename}</a>'''

def main():
    st.set_page_config(
        page_title="EML File Viewer", 
        page_icon="📧", 
        layout="wide"
    )
    
    # Check authentication
    if not check_authentication():
        login_form()
        return
    
    # Main app header with logout button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📧 EML File Viewer")
    with col2:
        if st.button("🚪 Logout", type="secondary"):
            logout()
    
    st.markdown("---")
    
    # S3 Configuration
    try:
        # Try Streamlit secrets first (cloud), then environment variables (local)
        if hasattr(st, 'secrets') and 'S3_BUCKET_NAME' in st.secrets:
            bucket_name = st.secrets["S3_BUCKET_NAME"]
            folder_prefix = st.secrets.get("S3_FOLDER_PREFIX", "")
            max_workers = st.secrets.get("MAX_WORKERS", 5)
        else:
            bucket_name = os.getenv("S3_BUCKET_NAME")
            folder_prefix = os.getenv("S3_FOLDER_PREFIX", "")
            max_workers = int(os.getenv("MAX_WORKERS", 5))
            
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME not found")
            
    except Exception as e:
        st.error(f"Missing S3 configuration: {str(e)}")
        st.info("""
        **For Streamlit Cloud:** Configure secrets in app settings  
        **For Local Development:** Set environment variables or use .env file
        """)
        return
    
    # Get EML files from S3
    with st.spinner("Loading EML files from S3..."):
        eml_files = list_eml_files_from_s3(bucket_name, folder_prefix)
    
    if not eml_files:
        st.warning("No EML files found in S3 bucket.")
        st.info(f"Please upload EML files to s3://{bucket_name}/{folder_prefix}")
        return
    
    st.success(f"Found {len(eml_files)} EML files in S3")
    
    # Add processing info
    st.info(f"🚀 Using parallel processing with {max_workers} workers")
    
    # Process emails in parallel
    with st.spinner("Processing emails in parallel..."):
        all_emails_data, max_attachments = process_emails_parallel(bucket_name, eml_files, max_workers)
    
    if not all_emails_data:
        st.error("Could not parse any EML files.")
        return
    
    # Sort emails by date (latest first) - handle None dates
    all_emails_data.sort(key=lambda x: x['date_obj'] if x['date_obj'] else datetime.min, reverse=True)
    
    # Display summary statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📧 Total Emails", len(all_emails_data))
    with col2:
        total_attachments = sum(len(email['attachments']) for email in all_emails_data)
        st.metric("📎 Total Attachments", total_attachments)
    with col3:
        st.metric("📁 Max Attachments per Email", max_attachments)
    
    # Create table data
    table_data = []
    
    for i, email_data in enumerate(all_emails_data):
        row = {
            'From': email_data['sender'],
            'Date': email_data['date'],
            'Title': email_data['subject']
        }
        
        # Add attachment columns with download links
        for j in range(max_attachments):
            if j < len(email_data['attachments']):
                attachment = email_data['attachments'][j]
                download_link = create_download_link(
                    attachment['content'],
                    attachment['filename'],
                    attachment['content_type']
                )
                row[f'Attachment_{j+1}'] = download_link
            else:
                row[f'Attachment_{j+1}'] = ''
        
        table_data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(table_data)
    
    # Display the table with HTML rendering for download links
    st.markdown("### Email Overview")
    
    # Create custom HTML table for better control
    table_html = "<table style='width:100%; border-collapse: collapse; table-layout: fixed;'>"
    
    # Header
    table_html += "<thead><tr style='background-color: #f0f2f6; border: 1px solid #ddd;'>"
    for col in df.columns:
        table_html += f"<th style='padding: 8px; text-align: left; border: 1px solid #ddd; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>{col}</th>"
    table_html += "</tr></thead>"
    
    # Body
    table_html += "<tbody>"
    for _, row in df.iterrows():
        table_html += "<tr style='border: 1px solid #ddd;'>"
        for col in df.columns:
            cell_value = row[col] if pd.notna(row[col]) else ""
            if col.startswith('Attachment_') and cell_value:
                # This is an attachment cell with HTML link
                table_html += f"<td style='padding: 8px; border: 1px solid #ddd; white-space: nowrap; overflow: hidden;'>{cell_value}</td>"
            else:
                # Regular text cell
                table_html += f"<td style='padding: 8px; border: 1px solid #ddd; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='{html.escape(str(cell_value))}'>{html.escape(str(cell_value))}</td>"
        table_html += "</tr>"
    table_html += "</tbody></table>"
    
    # Display the HTML table
    st.markdown(table_html, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("**Note:** Click on the attachment links in the table to download files directly.")

if __name__ == "__main__":
    main()
