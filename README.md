# AWS S3 → Azure Data Lake (ADLS Gen2) Connector

A minimal Python tool (Streamlit UI) to connect to an AWS S3 bucket, browse and select files, and upload them to Azure Data Lake Storage Gen2.

## 1) Cloud Context

- Runs on AWS and Azure free tiers
- Source: AWS S3 (personal AWS account)
- Destination: Azure ADLS Gen2 (personal Azure account)

## 2) UI (Streamlit)

Inputs (sidebar):
- AWS Access Key ID & Secret Access Key
- AWS Region (e.g., `us-east-1`)
- S3 Bucket name
- Azure Storage Account name & Account Key
- Target Azure container name

Buttons:
- List Files – fetch CSV/JSON/Parquet from S3
- Upload to ADLS – download, optional convert to Parquet, upload to ADLS

## 3) Backend Logic

1. Connect to S3 (boto3)
2. List CSV/JSON/Parquet objects
3. Download selected files to a temp dir
4. (Optional) Convert CSV/JSON → Parquet (pandas/pyarrow)
5. Upload to ADLS Gen2 (`/raw_data/{filename}`) using `azure-storage-file-datalake`
6. Clean up temp files
7. (Optional) Verify checksum (MD5) after upload

## 4) Install & Run

```bash
python -m venv venv
.\n+venv\Scripts\Activate.ps1  # Windows
# source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```
Open `http://localhost:8501`.

## 5) How to Use

1. Enter AWS keys, region, and S3 bucket
2. Click List Files and select one or more files
3. (Optional) Enable Convert to Parquet and/or Verify checksum (MD5)
4. Enter Azure storage account, key, and container
5. Click Upload to ADLS → files upload to `/raw_data/`

## 6) Set Up AWS & Azure

AWS (S3):
- IAM → Users → Create user → attach `AmazonS3ReadOnlyAccess` (or FullAccess as needed)
- Create access key (Application running outside AWS) → copy Access Key ID and Secret
- S3 → Create bucket → upload sample CSV/JSON/Parquet

Azure (ADLS Gen2):
- Create Storage Account → Advanced → enable Hierarchical namespace
- Containers → + Container (e.g., `raw`) → Private
- Access keys → copy Storage account name and key

## 7) Required Packages

`streamlit`, `boto3`, `azure-storage-file-datalake`, `pandas`, `pyarrow` (see `requirements.txt`).

## Project Structure

```
├─ app/streamlit_app.py
├─ src/
│  ├─ aws_connector/s3_client.py
│  ├─ azure_connector/adls_client.py
│  └─ utils/
│     ├─ file_converter.py
│     ├─ file_manager.py
│     └─ checksum.py
└─ requirements.txt
```

Notes:
- Credentials are entered at runtime and not stored
- Overwrites existing file with the same name in `/raw_data/`
- Only CSV/JSON/Parquet are listed per requirements
