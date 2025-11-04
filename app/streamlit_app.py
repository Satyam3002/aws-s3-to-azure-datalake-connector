"""
AWS S3 to Azure Data Lake Connector - Streamlit UI
This is the main UI entry point for the application.
"""

import streamlit as st
import sys
from pathlib import Path

# Add src directory to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent / "src"))

from aws_connector.s3_client import S3Connector
from azure_connector.adls_client import ADLSConnector
from utils.file_converter import convert_to_parquet, get_file_extension
from utils.checksum import md5_file
from utils.file_manager import TempFileManager
import os

# Set page configuration
st.set_page_config(
    page_title="S3 to ADLS Connector",
    page_icon="🔄",
    layout="wide"
)

# Dark + compact styling (applies regardless of Streamlit theme)
st.markdown(
    """
    <style>
      html, body, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
        background-color: #0e1117 !important;
        color: #e6e6e6 !important;
      }
      .block-container { padding-top: 0.8rem; padding-bottom: 0.8rem; }
      [data-testid="stSidebar"] { background-color: #0b0e14 !important; }
      .stTextInput > div > div > input,
      .stPassword > div > div > input,
      .stSelectbox > div > div > select { height: 2.1rem; }
      .stButton > button { padding: 0.35rem 0.8rem; border-radius: 6px; }
      .stDataFrame { border: 1px solid #2a2f3a; border-radius: 6px; }
      .stMetric { background: #121621; padding: 0.6rem; border-radius: 8px; border: 1px solid #1b2030; }
      .st-expander { background: #0b0f18 !important; border: 1px solid #1b2030; border-radius: 8px; }
      .stMarkdown, .stHeader, .stSubheader { margin-bottom: 0.5rem; }
      hr { border-color: #1b2030; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title
st.title("🔄 AWS S3 → Azure Data Lake Connector")
st.markdown("Easily browse S3 files and upload them to ADLS Gen2.")
st.divider()

# Sidebar for AWS credentials
st.sidebar.header("🔐 AWS Configuration")
aws_access_key_id = st.sidebar.text_input(
    "AWS Access Key ID",
    type="password",
    help="Enter your AWS Access Key ID",
    key="aws_access_key_id"
)
aws_secret_access_key = st.sidebar.text_input(
    "AWS Secret Access Key",
    type="password",
    help="Enter your AWS Secret Access Key",
    key="aws_secret_access_key"
)
aws_region = st.sidebar.text_input(
    "AWS Region",
    value="us-east-1",
    help="Enter AWS region (e.g., us-east-1, eu-west-1)",
    key="aws_region"
)
s3_bucket_name = st.sidebar.text_input(
    "S3 Bucket Name",
    help="Enter the name of your S3 bucket",
    key="s3_bucket_name"
)

# Sidebar for Azure configuration
st.sidebar.header("🔐 Azure Configuration")
azure_storage_account_name = st.sidebar.text_input(
    "Azure Storage Account Name",
    help="Enter your Azure Storage Account name",
    key="azure_storage_account_name"
)
azure_account_key = st.sidebar.text_input(
    "Azure Account Key",
    type="password",
    help="Enter your Azure Storage Account Key",
    key="azure_account_key"
)
azure_container_name = st.sidebar.text_input(
    "Azure Container Name",
    help="Enter the target container name in ADLS",
    key="azure_container_name"
)

# Initialize session state for storing file list
if 's3_files' not in st.session_state:
    st.session_state.s3_files = []
if 'file_list_message' not in st.session_state:
    st.session_state.file_list_message = ""
if 's3_connector' not in st.session_state:
    st.session_state.s3_connector = None
if 'temp_file_manager' not in st.session_state:
    st.session_state.temp_file_manager = None

# Main content area
st.header("✨ File Transfer")

# Button to list files
col1, col2 = st.columns(2)

with col1:
    list_files_btn = st.button(
        "📋 List Files",
        type="primary",
        use_container_width=True,
        help="Fetch and display files from S3 bucket"
    )

# Handle List Files button click
if list_files_btn:
    # Validate inputs
    if not aws_access_key_id or not aws_secret_access_key or not aws_region or not s3_bucket_name:
        st.error("❌ Please fill in all AWS credentials in the sidebar!")
    else:
        with st.spinner("Connecting to AWS S3 and listing files..."):
            try:
                # Create S3 connector
                s3_connector = S3Connector(
                    access_key_id=aws_access_key_id,
                    secret_access_key=aws_secret_access_key,
                    region=aws_region
                )
                
                # Connect
                success, message = s3_connector.connect()
                if not success:
                    st.error(f"❌ Connection failed: {message}")
                    st.session_state.s3_files = []
                else:
                    # Test bucket access
                    test_success, test_message = s3_connector.test_connection(s3_bucket_name)
                    if not test_success:
                        st.error(f"❌ {test_message}")
                        st.session_state.s3_files = []
                    else:
                        st.success(f"✅ {test_message}")
                        
                        # List files (CSV, JSON, Parquet)
                        list_success, files, list_message = s3_connector.list_files(
                            bucket_name=s3_bucket_name,
                            file_extensions=['csv', 'json', 'parquet']
                        )
                        
                        if list_success:
                            st.session_state.s3_files = files
                            st.session_state.file_list_message = list_message
                            st.session_state.s3_connector = s3_connector  # Store connector for later use
                            if files:
                                st.success(f"✅ {list_message}")
                            else:
                                st.warning(f"⚠️ {list_message}")
                        else:
                            st.error(f"❌ {list_message}")
                            st.session_state.s3_files = []
                            
            except Exception as e:
                st.error(f"❌ Unexpected error: {str(e)}")
                st.session_state.s3_files = []

# Display file list if available
if st.session_state.s3_files:
    st.subheader("📁 Available Files in S3 Bucket")
    
    # Summary metrics
    total_files = len(st.session_state.s3_files)
    total_size_mb = round(sum(f['size_mb'] for f in st.session_state.s3_files), 2)
    c1, c2 = st.columns(2)
    c1.metric("Files", f"{total_files}")
    c2.metric("Total Size", f"{total_size_mb} MB")

    # Create a table to display files with sizes
    file_data = {
        "File Name": [f['name'] for f in st.session_state.s3_files],
        "Size (MB)": [f['size_mb'] for f in st.session_state.s3_files]
    }
    st.dataframe(file_data, use_container_width=True, height=260)
    
    # File selection area
    st.subheader("Selected Files")
    file_options = [f['name'] for f in st.session_state.s3_files]
    selected_files = st.multiselect(
        "Select files to transfer",
        options=file_options,
        help="Choose one or more files to upload to Azure",
        key="selected_files"
    )
else:
    # File selection area (empty if no files listed)
    st.subheader("Selected Files")
    selected_files = st.multiselect(
        "Select files to transfer",
        options=[],
        help="Click 'List Files' first to see available files",
        key="selected_files"
    )

# Optional: Transfer options
st.subheader("⚙️ Transfer Options")
cols_opts = st.columns(2)
with cols_opts[0]:
    convert_to_parquet_opt = st.checkbox(
        "Convert CSV/JSON to Parquet",
        value=False,
        help="Convert CSV or JSON files to Parquet before upload",
        key="convert_to_parquet_opt"
    )
with cols_opts[1]:
    verify_checksum_opt = st.checkbox(
        "Verify checksum (MD5)",
        value=False,
        help="After upload, compute and compare MD5 to ensure integrity",
        key="verify_checksum_opt"
    )

# Button to upload
with col2:
    upload_btn = st.button(
        "🚀 Upload to ADLS",
        type="primary",
        use_container_width=True,
        help="Upload selected files to Azure Data Lake"
    )

# Handle Upload button click
if upload_btn:
    # Validate inputs
    if not selected_files:
        st.warning("⚠️ Please select at least one file to upload!")
    elif not st.session_state.s3_connector:
        st.error("❌ Please click 'List Files' first to establish connection!")
    elif not azure_storage_account_name or not azure_account_key or not azure_container_name:
        st.error("❌ Please fill in all Azure credentials in the sidebar!")
    else:
        # Always start with a fresh temp directory for each upload run
        st.session_state.temp_file_manager = TempFileManager()
        
        temp_manager = st.session_state.temp_file_manager
        s3_connector = st.session_state.s3_connector
        
        downloaded_files = []
        converted_files = []
        errors = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_files = len(selected_files)
        
        for idx, file_key in enumerate(selected_files):
            status_text.text(f"Processing {idx + 1}/{total_files}: {file_key}")
            
            try:
                # Step 1: Download file from S3
                filename = os.path.basename(file_key)
                local_temp_path = temp_manager.create_temp_file(filename)
                
                download_success, download_msg = s3_connector.download_file(
                    bucket_name=s3_bucket_name,
                    file_key=file_key,
                    local_path=local_temp_path
                )
                
                if not download_success:
                    errors.append(f"{file_key}: {download_msg}")
                    continue
                
                # Step 2: Convert to Parquet if needed
                file_ext = get_file_extension(filename)
                final_path = local_temp_path
                final_filename = filename
                
                if convert_to_parquet_opt and file_ext in ['csv', 'json']:
                    # Convert to Parquet
                    parquet_filename = filename.rsplit('.', 1)[0] + '.parquet'
                    parquet_path = temp_manager.create_temp_file(parquet_filename)
                    
                    convert_success, convert_msg = convert_to_parquet(local_temp_path, parquet_path)
                    
                    if convert_success:
                        final_path = parquet_path
                        final_filename = parquet_filename
                        converted_files.append({
                            'original': filename,
                            'converted': parquet_filename
                        })
                        # Remove original file after conversion
                        try:
                            os.remove(local_temp_path)
                        except:
                            pass
                    else:
                        errors.append(f"{file_key}: Conversion failed - {convert_msg}")
                        # Use original file if conversion fails
                        final_path = local_temp_path
                        final_filename = filename
                
                # Store file info with final path (after conversion if applicable)
                downloaded_files.append({
                    'original_key': file_key,
                    'local_path': final_path,
                    'filename': final_filename
                })
                
                # Update progress
                progress = (idx + 1) / total_files
                progress_bar.progress(progress)
                
            except Exception as e:
                errors.append(f"{file_key}: Unexpected error - {str(e)}")
        
        # Store prepared files in session state for upload step
        st.session_state.prepared_files = downloaded_files
        
        # Display results
        status_text.empty()
        progress_bar.empty()
        
        if downloaded_files:
            st.success(f"✅ Successfully prepared {len(downloaded_files)} file(s) for upload!")
            
            if converted_files:
                st.info(f"📦 Converted {len(converted_files)} file(s) to Parquet format")
            
            # Store conversion info
            st.session_state.converted_files = converted_files
            st.session_state.convert_to_parquet_option = convert_to_parquet_opt
            
            # Now upload to Azure ADLS
            st.subheader("📤 Uploading to Azure ADLS Gen2...")
            
            # Initialize Azure connector
            adls_connector = ADLSConnector(
                account_name=azure_storage_account_name,
                account_key=azure_account_key
            )
            
            # Connect to Azure
            connect_success, connect_msg = adls_connector.connect()
            if not connect_success:
                st.error(f"❌ Azure connection failed: {connect_msg}")
            else:
                # Test container access
                test_success, test_msg = adls_connector.test_connection(azure_container_name)
                if not test_success:
                    st.error(f"❌ {test_msg}")
                else:
                    st.success(f"✅ {test_msg}")
                    
                    # Create raw_data directory if needed
                    dir_success, dir_msg = adls_connector.create_directory_if_not_exists(
                        container_name=azure_container_name,
                        directory_path="raw_data"
                    )
                    
                    # Upload files
                    upload_progress = st.progress(0)
                    upload_status = st.empty()
                    
                    uploaded_files = []
                    upload_errors = []
                    
                    for idx, file_info in enumerate(downloaded_files):
                        upload_status.text(f"Uploading {idx + 1}/{len(downloaded_files)}: {file_info['filename']}")
                        
                        # Construct remote path: /raw_data/{filename}
                        remote_path = f"/raw_data/{file_info['filename']}"
                        
                        upload_success, upload_msg = adls_connector.upload_file(
                            container_name=azure_container_name,
                            local_file_path=file_info['local_path'],
                            remote_path=remote_path
                        )
                        
                        if upload_success:
                            uploaded_files.append({
                                'filename': file_info['filename'],
                                'remote_path': remote_path,
                                'message': upload_msg
                            })
                        else:
                            upload_errors.append(f"{file_info['filename']}: {upload_msg}")
                        
                        # Update progress
                        upload_progress.progress((idx + 1) / len(downloaded_files))
                    
                    upload_progress.empty()
                    upload_status.empty()
                    
                    # Display upload results
                    if uploaded_files:
                        st.success(f"✅ Successfully uploaded {len(uploaded_files)} file(s) to Azure ADLS!")
                        
                        with st.expander("📋 Uploaded Files", expanded=True):
                            for file_info in uploaded_files:
                                # Extract size info from message if available
                                size_info = ""
                                if '(' in file_info['message']:
                                    size_info = file_info['message'].split('(')[1].rstrip(')')
                                line = f"✅ **{file_info['filename']}** → `{file_info['remote_path']}` {size_info}"
                                st.write(line)

                        # Optional checksum verification
                        if verify_checksum_opt:
                            st.subheader("🔎 Verifying checksums (MD5)")
                            verify_progress = st.progress(0)
                            verify_results = []
                            for idx, file_info in enumerate(downloaded_files):
                                local_ok, local_md5 = md5_file(file_info['local_path'])
                                if not local_ok:
                                    verify_results.append(f"{file_info['filename']}: local MD5 error - {local_md5}")
                                    continue
                                remote_ok, remote_md5 = adls_connector.compute_remote_md5(
                                    container_name=azure_container_name,
                                    remote_path=f"/raw_data/{file_info['filename']}"
                                )
                                if not remote_ok:
                                    verify_results.append(f"{file_info['filename']}: remote MD5 error - {remote_md5}")
                                else:
                                    if local_md5 == remote_md5:
                                        verify_results.append(f"✅ {file_info['filename']}: checksum OK")
                                    else:
                                        verify_results.append(f"❌ {file_info['filename']}: checksum MISMATCH")
                                verify_progress.progress((idx + 1) / len(downloaded_files))
                            verify_progress.empty()
                            for line in verify_results:
                                st.write(line)
                        
                        # Cleanup temporary files and reset manager
                        try:
                            if st.session_state.temp_file_manager:
                                st.session_state.temp_file_manager.cleanup()
                                st.session_state.temp_file_manager = None
                                st.info("🧹 Temporary files cleaned up successfully!")
                        except Exception as e:
                            st.warning(f"⚠️ Could not cleanup temp files: {str(e)}")
                    
                    if upload_errors:
                        st.error(f"❌ Upload errors for {len(upload_errors)} file(s):")
                        for error in upload_errors:
                            st.write(f"  - {error}")
        
        if errors:
            st.error(f"❌ Errors occurred with {len(errors)} file(s):")
            for error in errors:
                st.write(f"  - {error}")

# Status/info section
st.header("Status")
st.info("👆 Configure your AWS and Azure credentials in the sidebar, then click 'List Files' to get started!")

# Instructions
with st.expander("ℹ️ Instructions"):
    st.markdown("""
    ### How to use:
    1. **Configure AWS**: Enter your AWS credentials and S3 bucket name in the sidebar
    2. **Configure Azure**: Enter your Azure Storage Account details and container name
    3. **List Files**: Click "List Files" to see available files in your S3 bucket
    4. **Select Files**: Choose one or more files from the list
    5. **Upload**: Click "Upload to ADLS" to transfer files to Azure
    
    ### Notes:
    - AWS credentials are required to read from S3
    - Azure credentials are required to write to ADLS Gen2
    - Files will be uploaded to: `/raw_data/{filename}.parquet`
    - Temporary files are automatically cleaned up after upload
    """)




