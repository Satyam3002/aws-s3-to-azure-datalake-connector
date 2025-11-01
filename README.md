# AWS S3 to Azure Data Lake Connector

Python tool with Streamlit UI to transfer files from AWS S3 to Azure Data Lake Storage Gen2.

## Features

- Connect to AWS S3 and list files (CSV, JSON, Parquet)
- Select and download files from S3
- Optional CSV/JSON to Parquet conversion
- Upload to Azure ADLS Gen2 at `/raw_data/{filename}.parquet`
- Progress tracking and automatic cleanup

## Installation

```bash
# Create virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1  # Windows
# source venv/bin/activate    # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

## Running

```bash
streamlit run app/streamlit_app.py
```

Open `http://localhost:8501` in your browser.

## Usage

1. **Configure AWS** (sidebar): Enter Access Key ID, Secret Key, Region, and Bucket Name
2. **Configure Azure** (sidebar): Enter Storage Account Name, Account Key, and Container Name
3. **List Files**: Click "List Files" to see available S3 files
4. **Select Files**: Choose files from the dropdown
5. **Upload**: Click "Upload to ADLS" to transfer files to Azure

## Cloud Setup

**AWS:**

- Create S3 bucket
- Create IAM user with S3 read access
- Generate Access Key ID and Secret Access Key

**Azure:**

- Create Storage Account with **Hierarchical namespace enabled** (ADLS Gen2)
- Create container
- Get Storage Account Name and Access Key

## Project Structure

```
├── app/streamlit_app.py          # Main UI
├── src/
│   ├── aws_connector/           # AWS S3 operations
│   ├── azure_connector/         # Azure ADLS operations
│   └── utils/                    # File conversion & management
└── requirements.txt
```

## Requirements

- Python 3.10+
- AWS Account with S3 bucket
- Azure Account with ADLS Gen2 Storage Account
