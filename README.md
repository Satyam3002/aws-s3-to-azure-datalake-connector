# AWS S3 to Azure Data Lake Connector

A Python-based tool with a Streamlit UI that allows you to connect to an AWS S3 bucket, browse and select files, and automatically upload them to Azure Data Lake Storage Gen2 (ADLS).

## 🎯 Features

- **AWS S3 Integration**: Connect to S3 buckets and list available files (CSV, JSON, Parquet)
- **File Selection**: Select one or more files for transfer
- **Format Conversion**: Optional CSV/JSON to Parquet conversion before upload
- **Azure ADLS Upload**: Automatic upload to Azure Data Lake Storage Gen2
- **Progress Tracking**: Real-time progress bars and status updates
- **Automatic Cleanup**: Temporary files are automatically removed after upload
- **User-Friendly UI**: Simple Streamlit interface for easy operation

## 📋 Requirements

- Python 3.10 or higher
- AWS Account with S3 bucket and credentials
- Azure Account with Data Lake Storage Gen2 (Hierarchical namespace enabled)
- Required Python packages (see `requirements.txt`)

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd aws-s3-to-azure-datalake-connector
```

### 2. Create Virtual Environment

**On Windows (PowerShell):**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**On Linux/Mac:**

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## ☁️ Cloud Setup

### AWS S3 Setup

1. **Create an AWS Account** (if you don't have one)

   - Go to https://aws.amazon.com/
   - Sign up for a free tier account

2. **Create an S3 Bucket**

   - Log in to AWS Console
   - Navigate to S3 service
   - Click "Create bucket"
   - Choose a unique bucket name and region
   - Click "Create bucket"

3. **Create IAM User and Access Keys**

   - Go to IAM service in AWS Console
   - Click "Users" → "Create user"
   - User name: `s3-connector-user` (or any name)
   - Select "Provide user access to the AWS Management Console" = **OFF**
   - Click "Next"
   - Under "Permissions", select "Attach policies directly"
   - Attach policy: `AmazonS3ReadOnlyAccess` (or `AmazonS3FullAccess` for full access)
   - Click "Next" → "Create user"
   - Open the user → Go to "Security credentials" tab
   - Click "Create access key"
   - Select "Application running outside AWS"
   - Click "Next" → "Create access key"
   - **Save**:
     - **AWS Access Key ID**
     - **AWS Secret Access Key**
   - Also note your **AWS Region** (e.g., `us-east-1`)

4. **Upload Test Files to S3**
   - Go to your S3 bucket
   - Click "Upload" and add some CSV, JSON, or Parquet files

### Azure ADLS Gen2 Setup

1. **Create an Azure Account** (if you don't have one)

   - Go to https://azure.microsoft.com/free/
   - Sign up for a free account

2. **Create Storage Account with ADLS Gen2**

   - Log in to Azure Portal
   - Click "Create a resource" → Search "Storage account"
   - Click "Create"
   - Fill in:
     - **Subscription**: Your subscription
     - **Resource group**: Create new or use existing
     - **Storage account name**: `datalake<unique>` (must be globally unique)
     - **Region**: Choose your region
     - **Performance**: Standard
     - **Redundancy**: LRS (Locally-redundant storage) - cheapest option
   - Click "Next: Advanced"
   - **Enable "Hierarchical namespace"** = **ON** (This is required for ADLS Gen2!)
   - Click "Review + create" → "Create"

3. **Create Container**

   - Once the storage account is created, open it
   - Click "Containers" (under Data management)
   - Click "+ Container"
   - Name: `raw` (or any name you prefer)
   - Public access level: Private
   - Click "Create"

4. **Get Access Key**
   - In your Storage Account, go to "Access keys" (under Security + networking)
   - Click "Show" next to **key1**
   - Copy:
     - **Storage account name** (e.g., `datalake12345`)
     - **Key1** value

## 🏃 Running the Application

1. **Activate Virtual Environment** (if not already activated)

   ```powershell
   .\venv\Scripts\Activate.ps1
   ```

2. **Run Streamlit App**

   ```bash
   streamlit run app/streamlit_app.py
   ```

3. **Open Browser**
   - The app will automatically open at `http://localhost:8501`
   - Or manually navigate to the URL shown in the terminal

## 📖 Usage Instructions

### Step 1: Configure AWS Credentials

In the left sidebar, enter:

- **AWS Access Key ID**: Your AWS access key
- **AWS Secret Access Key**: Your AWS secret key
- **AWS Region**: Your bucket's region (e.g., `us-east-1`)
- **S3 Bucket Name**: Name of your S3 bucket

### Step 2: Configure Azure Credentials

In the left sidebar, enter:

- **Azure Storage Account Name**: Your storage account name
- **Azure Account Key**: Your storage account key (Key1)
- **Azure Container Name**: Your container name (e.g., `raw`)

### Step 3: List Files from S3

- Click the **"📋 List Files"** button
- The app will connect to S3 and display available files (CSV, JSON, Parquet)
- A table will show file names and sizes

### Step 4: Select Files

- Use the multi-select dropdown to choose one or more files
- Select files you want to transfer to Azure

### Step 5: Optional - Convert to Parquet

- Check the **"Convert CSV/JSON to Parquet format"** checkbox if you want automatic conversion

### Step 6: Upload to Azure

- Click the **"🚀 Upload to ADLS"** button
- The app will:
  1. Download files from S3 to a temporary directory
  2. Convert files to Parquet (if option enabled)
  3. Upload files to Azure ADLS at path: `/raw_data/{filename}.parquet`
  4. Clean up temporary files automatically

### Step 7: Verify Upload

- Check the success messages and uploaded file list
- Verify files in Azure Portal under your Storage Account → Containers → `raw` → `raw_data/`

## 📁 Project Structure

```
aws-s3-to-azure-datalake-connector/
├── app/
│   └── streamlit_app.py          # Main Streamlit UI application
├── src/
│   ├── aws_connector/
│   │   ├── __init__.py
│   │   └── s3_client.py          # AWS S3 connection and operations
│   ├── azure_connector/
│   │   ├── __init__.py
│   │   └── adls_client.py        # Azure ADLS connection and upload
│   └── utils/
│       ├── __init__.py
│       ├── file_converter.py     # CSV/JSON to Parquet conversion
│       └── file_manager.py       # Temporary file management
├── .gitignore
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── SETUP.md                      # Additional setup instructions
```

## 📦 Python Packages

The following packages are required (see `requirements.txt`):

- `streamlit` - Web UI framework
- `boto3` - AWS SDK for Python
- `azure-storage-file-datalake` - Azure ADLS Gen2 SDK
- `pandas` - Data manipulation
- `pyarrow` - Parquet file support

## 🔒 Security Notes

- **Never commit credentials to Git**: Credentials are entered in the UI and not stored
- **Use IAM roles when possible**: For production, use IAM roles instead of access keys
- **Rotate access keys regularly**: Keep your AWS and Azure keys secure
- **Use environment variables**: For automation, consider using environment variables for credentials

## 🐛 Troubleshooting

### "Container not found" Error

- Ensure the container exists in your Azure Storage Account
- Check the container name matches exactly (case-sensitive)

### "Access denied" Errors

- Verify AWS credentials have S3 read permissions
- Verify Azure key is correct and has write permissions
- Check if Hierarchical namespace is enabled on your Storage Account

### "Connection failed" Errors

- Verify network connectivity
- Check AWS region matches your bucket's region
- Ensure Azure Storage Account name is correct

### Files not showing in S3 list

- Ensure files have extensions: `.csv`, `.json`, or `.parquet`
- Check if files are actually in the bucket
- Verify AWS credentials have read permissions

## 🎓 Learning Notes

This project demonstrates:

- **Cloud integration**: Connecting to AWS and Azure services
- **File handling**: Download, conversion, and upload operations
- **UI development**: Building interactive web interfaces with Streamlit
- **Error handling**: Graceful error handling and user feedback
- **Progress tracking**: Real-time progress updates for long operations

## 📝 License

This project is created as a learning/demonstration project.

## 🤝 Contributing

Feel free to fork, modify, and use this project for your own purposes!

## 📞 Support

For issues or questions:

1. Check the troubleshooting section above
2. Verify your cloud account configurations
3. Review the error messages in the Streamlit UI

---

**Happy transferring! 🚀**
