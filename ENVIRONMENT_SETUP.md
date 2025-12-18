# Environment Setup Guide

## Overview

This project uses environment variables to securely manage InfluxDB credentials and configuration. **Never commit your actual InfluxDB tokens to version control.**

## Quick Start

### 1. Install Dependencies

```bash
pip install python-dotenv influxdb-client
```

Or if you have a requirements.txt:

```bash
pip install -r requirements.txt
```

### 2. Create Your `.env` File

Copy the example file and add your actual credentials:

```bash
cp .env.example .env
```

Then edit `.env` and replace the placeholder values with your actual credentials:

```ini
# InfluxDB Configuration
INFLUX_TOKEN=apiv3_YOUR_ACTUAL_TOKEN_HERE
INFLUX_URL=http://127.0.0.1:8181
INFLUX_BUCKET=ThetaData
```

### 3. Get Your InfluxDB Token

#### Option A: From InfluxDB UI
1. Open your InfluxDB instance in browser (e.g., http://127.0.0.1:8181)
2. Navigate to Data > Tokens
3. Click "Generate Token" or copy an existing token
4. Paste the token into your `.env` file

#### Option B: Using InfluxDB CLI
```bash
influx auth create \
  --all-access \
  --description "ThetaSync Token"
```

## File Structure

```
tdSynchManager/
├── .env                 # Your local credentials (NEVER commit)
├── .env.example         # Template file (safe to commit)
├── .gitignore           # Configured to exclude .env files
└── requirements.txt     # Includes python-dotenv
```

## Security Best Practices

### ✅ DO:
- Store your token in `.env` file
- Use `.env.example` as a template
- Keep `.env` in `.gitignore`
- Use `python-dotenv` to load environment variables
- Share `.env.example` with team members
- Rotate tokens regularly

### ❌ DON'T:
- Commit `.env` to version control
- Hardcode tokens in source files
- Share tokens via email or chat
- Use the same token across environments
- Commit files with actual tokens

## Usage in Code

All scripts and notebooks now automatically load environment variables:

```python
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access configuration
influx_token = os.getenv('INFLUX_TOKEN')
influx_url = os.getenv('INFLUX_URL', 'http://127.0.0.1:8181')
influx_bucket = os.getenv('INFLUX_BUCKET', 'ThetaData')

# Validate token is set
if not influx_token:
    raise ValueError("INFLUX_TOKEN environment variable is required. Please set it in your .env file.")
```

## Troubleshooting

### Error: "INFLUX_TOKEN environment variable is required"

**Solution:** Make sure you have created a `.env` file with your token:

```bash
# Check if .env exists
ls -la .env

# If not, create it from example
cp .env.example .env

# Edit it with your actual token
nano .env  # or use your preferred editor
```

### Error: "ModuleNotFoundError: No module named 'dotenv'"

**Solution:** Install python-dotenv:

```bash
pip install python-dotenv
```

### Token not loading in Jupyter Notebooks

**Solution:** Make sure to restart your Jupyter kernel after creating/modifying `.env`:

1. In Jupyter: Kernel > Restart Kernel
2. Re-run the cells that import and load environment variables

## Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `INFLUX_TOKEN` | ✅ Yes | None | Your InfluxDB API token (starts with `apiv3_`) |
| `INFLUX_URL` | No | `http://127.0.0.1:8181` | InfluxDB instance URL |
| `INFLUX_BUCKET` | No | `ThetaData` | InfluxDB bucket/database name |
| `INFLUX_ORG` | No | None | InfluxDB organization (if needed) |

## Testing Your Setup

Run this simple test to verify your configuration:

```python
import os
from dotenv import load_dotenv

load_dotenv()

print("Environment Variables:")
print(f"  INFLUX_URL: {os.getenv('INFLUX_URL', 'NOT SET')}")
print(f"  INFLUX_BUCKET: {os.getenv('INFLUX_BUCKET', 'NOT SET')}")
print(f"  INFLUX_TOKEN: {'***' + os.getenv('INFLUX_TOKEN', '')[-8:] if os.getenv('INFLUX_TOKEN') else 'NOT SET'}")
```

Expected output:
```
Environment Variables:
  INFLUX_URL: http://127.0.0.1:8181
  INFLUX_BUCKET: ThetaData
  INFLUX_TOKEN: ***Zc2w (last 8 chars)
```

## Additional Resources

- [python-dotenv Documentation](https://github.com/theskumar/python-dotenv)
- [InfluxDB API Tokens](https://docs.influxdata.com/influxdb/v2.0/security/tokens/)
- [12 Factor App: Config](https://12factor.net/config)

## Support

If you encounter issues with environment setup:

1. Check that `.env` exists and contains your token
2. Verify the token format starts with `apiv3_`
3. Ensure `python-dotenv` is installed
4. Restart your Python interpreter/Jupyter kernel
5. Check file permissions on `.env` (should be readable)

---

**Last Updated:** 2025-12-17
**Security Note:** This file contains no sensitive information and is safe to commit.
