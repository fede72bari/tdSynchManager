# ThetaData Synchronization Manager - User & Developer Manual

**Version:** 1.0.9
**Last Updated:** December 2025
**License:** Custom (Free for personal use, Commercial license required)
**Author:** Federico Bari
**Contact:** fede72bari@gmail.com

---

## Table of Contents

1. [Introduction](#1-introduction)
   - 1.1 [Synchronized Historical Data Persistence](#11-synchronized-historical-data-persistence)
   - 1.2 [Supported Timeframes](#12-supported-timeframes)
   - 1.3 [Coherence Control](#13-coherence-control)
   - 1.4 [System Architecture Overview](#14-system-architecture-overview)
   - 1.5 [What This Tool Does NOT Do](#15-what-this-tool-does-not-do)

2. [Installation & Environment Setup](#2-installation--environment-setup)
   - 2.1 [System Requirements](#21-system-requirements)
   - 2.2 [Python Environment Setup](#22-python-environment-setup)
   - 2.3 [Installing tdSynchManager](#23-installing-tdsynchmanager)
   - 2.4 [Dependencies Installation](#24-dependencies-installation)
   - 2.5 [Anaconda Environment File](#25-anaconda-environment-file)

3. [ThetaData Client V3 Configuration](#3-thetadata-client-v3-configuration)
   - 3.1 [ThetaData Account Setup](#31-thetadata-account-setup)
   - 3.2 [Obtaining API Credentials](#32-obtaining-api-credentials)
   - 3.3 [Client Configuration](#33-client-configuration)
   - 3.4 [ThetaData Terminal Setup](#34-thetadata-terminal-setup-optional)
   - 3.5 [Testing Client Connection](#35-testing-client-connection)

4. [InfluxDB Setup & Configuration](#4-influxdb-setup--configuration)
   - 4.1 [InfluxDB Installation](#41-influxdb-installation)
   - 4.2 [Initial Configuration](#42-initial-configuration)
   - 4.3 [Obtaining InfluxDB Token](#43-obtaining-influxdb-token)
   - 4.4 [Database Schema Design](#44-database-schema-design)
   - 4.5 [Testing InfluxDB Connection](#45-testing-influxdb-connection)
   - 4.6 [InfluxDB Startup Scripts](#46-influxdb-startup-scripts)

5. [Helper Scripts & Batch Files](#5-helper-scripts--batch-files) **(Windows)**
   - 5.1 [Batch Files Overview](#51-batch-files-overview)
   - 5.2 [start_environment.bat](#52-start_environmentbat)
   - 5.3 [start_influxdb.bat](#53-start_influxdbbat)
   - 5.4 [start_thetadata.bat](#54-start_thetadatabat)
   - 5.5 [start_jupyter.bat](#55-start_jupyterbat)
   - 5.6 [run_sync.bat](#56-run_syncbat)
   - 5.7 [Example Jupyter Notebook](#57-example-jupyter-notebook)

6. [Core API Reference](#6-core-api-reference)
   - 6.1 [ThetaDataV3Client](#61-thetadatav3client)
   - 6.2 [ManagerConfig](#62-managerconfig)
   - 6.3 [Task](#63-task)
   - 6.4 [DiscoverPolicy](#64-discoverpolicy)
   - 6.5 [ThetaSyncManager](#65-thetasyncmanager)

7. [Quick Start](#7-quick-start)
   - 7.1 [First Working Example](#71-first-working-example)
   - 7.2 [Minimal Configuration](#72-minimal-configuration)
   - 7.3 [Running Basic Sync](#73-running-basic-sync)
   - 7.4 [Verifying Results](#74-verifying-results)

8. [ThetaDataV3Client Detailed](#8-thetadatav3client-detailed)
   - 8.1 [Fundamental Concepts](#81-fundamental-concepts)
   - 8.2 [Initialization and Configuration](#82-initialization-and-configuration)
   - 8.3 [Data Download Methods](#83-data-download-methods)
   - 8.4 [Supported Asset Types](#84-supported-asset-types)
   - 8.5 [API Methods Reference](#85-api-methods-reference)
   - 8.6 [Error Handling](#86-error-handling)

9. [ManagerConfig Detailed](#9-managerconfig-detailed)
   - 9.1 [Configuration Parameters](#91-configuration-parameters)
   - 9.2 [Sink Configuration](#92-sink-configuration)
   - 9.3 [Coherence Settings](#93-coherence-settings)
   - 9.4 [Logging Configuration](#94-logging-configuration)

10. [Task Definition](#10-task-definition)
    - 10.1 [Task Structure](#101-task-structure)
    - 10.2 [Discovery Policy](#102-discovery-policy)
    - 10.3 [Advanced Options](#103-advanced-options)

11. [ThetaSyncManager Detailed](#11-thetasyncmanager-detailed)
    - 11.1 [Initialization](#111-initialization)
    - 11.2 [run() Method](#112-run-method)
    - 11.3 [Internal Pipeline](#113-internal-pipeline)
    - 11.4 [Idempotent Operations](#114-idempotent-operations)

12. [Sink Strategies](#12-sink-strategies)
    - 12.1 [CSV Sink](#121-csv-sink)
    - 12.2 [Parquet Sink](#122-parquet-sink)
    - 12.3 [InfluxDB Sink](#123-influxdb-sink)

13. [Coherence Checking](#13-coherence-checking)
    - 13.1 [Coherence Concepts](#131-coherence-concepts)
    - 13.2 [Coherence Modes](#132-coherence-modes)
    - 13.3 [Bucket Analysis](#133-bucket-analysis-intraday)
    - 13.4 [Validation Flow](#134-validation-flow)
    - 13.5 [Output and Reports](#135-output-and-reports)

14. [Recovery & Repair](#14-recovery--repair)
    - 14.1 [Automatic Recovery](#141-automatic-recovery)
    - 14.2 [Manual Recovery](#142-manual-recovery)
    - 14.3 [Common Troubleshooting](#143-common-troubleshooting)

15. [Timestamp Handling](#15-timestamp-handling)
    - 15.1 [Supported ISO8601 Formats](#151-supported-iso8601-formats)
    - 15.2 [Pandas Compatibility](#152-pandas-compatibility)
    - 15.3 [Timezone Management](#153-timezone-management)
    - 15.4 [EOD vs Intraday vs Tick Timestamps](#154-eod-vs-intraday-vs-tick-timestamps)
    - 15.5 [Known Issues and Solutions](#155-known-issues-and-solutions)

16. [Performance & Optimization](#16-performance--optimization)
17. [Practical Examples](#17-practical-examples)
18. [Advanced Topics](#18-advanced-topics)
19. [Full API Reference](#19-full-api-reference)
20. [Best Practices](#20-best-practices)
21. [Changelog & Versioning](#21-changelog--versioning)
22. [FAQ & Troubleshooting](#22-faq--troubleshooting)
23. [Appendices](#appendices)

---

## 1. Introduction

### 1.1 Synchronized Historical Data Persistence

**tdSynchManager** is a Python library designed for automated, robust, and idempotent synchronization of financial market data from ThetaData's V3 API to local storage systems.

**Primary Objectives:**

- **Automated Data Synchronization**: Download and maintain up-to-date historical datasets without manual intervention
- **Multi-Sink Architecture**: Store data in multiple formats simultaneously (CSV, Parquet, InfluxDB)
- **Coherence Validation**: Automatically detect missing data, gaps, duplicates, and inconsistencies
- **Auto-Recovery**: Intelligently backfill gaps and repair incomplete datasets
- **Bandwidth Optimization**: Minimize API calls through idempotent operations and incremental updates
- **Idempotent Operations**: Safe to re-run - duplicate data is automatically handled

**Key Features:**

✅ **Zero-configuration discovery** - automatically finds symbols and date ranges
✅ **Idempotent downloads** - re-running the same task won't create duplicates
✅ **Automatic sorting & deduplication** - data always arrives clean
✅ **Gap detection & backfill** - missing data is automatically recovered
✅ **Multi-format output** - CSV, Parquet, InfluxDB supported simultaneously
✅ **Timestamp normalization** - handles mixed ISO8601 formats from ThetaData API

---

### 1.2 Supported Timeframes

tdSynchManager supports all major market data granularities:

#### End-of-Day (EOD)
- **Interval**: `1d`
- **Data**: Daily OHLCV bars
- **Use Case**: Long-term backtesting, portfolio analysis
- **Historical Range**: Varies by asset (up to 40+ years for stocks)

#### Intraday Bars
- **Intervals**: `1min`, `5min`, `15min`, `30min`, `1h`, `4h`
- **Data**: Intrabar OHLCV
- **Use Case**: Short-term strategies, volatility analysis
- **Historical Range**: Typically 1-5 years depending on subscription

#### Tick-Level Data
- **Types**: Trades, Quotes, OHLC Ticks
- **Resolution**: Microsecond timestamps
- **Data Fields**:
  - Trades: price, size, exchange, conditions
  - Quotes: bid/ask, bid_size/ask_size, exchange
  - OHLC: open, high, low, close per tick interval
- **Use Case**: High-frequency research, market microstructure
- **Historical Range**: Typically months to 1 year (large data volumes)

**Supported Asset Classes:**
- Stocks (US equities)
- Indices (SPX, NDX, RUT, etc.)
- Options (equity and index options)
- Futures (commodity, financial, index futures)

---

### 1.3 Coherence Control

**Coherence** refers to the completeness and consistency of your historical dataset.

#### Completeness Checks
- **Missing Dates**: Detects gaps in daily EOD data (excluding market holidays)
- **Missing Hours**: Identifies missing intraday bars during market hours
- **Bucket Analysis**: Validates that each trading day has expected number of bars

#### Consistency Validation
- **Duplicate Detection**: Finds and removes duplicate timestamps
- **Invalid Data**: Identifies NaN values, zero volumes, malformed records
- **Timestamp Ordering**: Ensures chronological sorting

#### Near Real-Time Synchronization
⚠️ **Important**: tdSynchManager is **NOT** a real-time streaming solution.

- Updates data periodically (e.g., every 15 minutes via cron/scheduled tasks)
- Performs coherence checks after each download
- Suitable for "near real-time" applications (minute-level latency)
- **For true real-time streaming**, use ThetaData Terminal's WebSocket API

**Typical Use Case:**
```python
# Run this script every 15 minutes via cron
# Downloads latest bars + validates coherence + backfills gaps
await manager.run(tasks)  # Idempotent - safe to re-run
```

---

### 1.4 System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     ThetaData V3 API (Cloud)                    │
│                  (EOD, Intraday, Tick Data)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTPS
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ThetaDataV3Client                            │
│  - Authentication & Rate Limiting                               │
│  - Async Download (asyncio)                                     │
│  - Response Caching                                             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ThetaSyncManager Core                         │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 1. Discovery Phase                                        │ │
│  │    - Find symbols/dates to download                       │ │
│  │    - Apply DiscoverPolicy (skip/mild_skip/wild)          │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 2. Download Phase                                         │ │
│  │    - Parallel downloads (max_concurrency)                 │ │
│  │    - Idempotency check (skip existing dates)             │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 3. Validation Phase                                       │ │
│  │    - Timestamp parsing (mixed ISO8601 formats)           │ │
│  │    - Sort & Deduplication                                 │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 4. Write Phase (Multi-Sink)                              │ │
│  │    ├─ CSV Sink                                            │ │
│  │    ├─ Parquet Sink                                        │ │
│  │    └─ InfluxDB Sink                                       │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 5. Coherence Check Phase                                 │ │
│  │    - Gap detection                                        │ │
│  │    - Bucket analysis (intraday)                          │ │
│  │    - Auto-recovery triggers                               │ │
│  └───────────────────────────────────────────────────────────┘ │
└────────────────────────────┬────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   CSV Files     │ │ Parquet Files   │ │  InfluxDB 3.x   │
│ (root_dir/...)  │ │ (root_dir/...)  │ │ (Remote/Local)  │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

**Software Structure:**

```
tdSynchManager/
├── src/tdSynchManager/
│   ├── __init__.py          # Public API exports
│   ├── ThetaDataV3Client.py # ThetaDataV3Client implementation
│   ├── manager.py           # ThetaSyncManager core logic
│   ├── config.py            # ManagerConfig, Task, DiscoverPolicy
│   ├── validator.py         # Coherence validation logic
│   ├── coherence.py         # Gap detection & recovery
│   └── logger.py            # Logging utilities
├── examples/
│   ├── notebooks/           # Jupyter example notebooks
│   ├── scripts/             # Python example scripts
│   └── batch/               # Batch files (.bat for Windows)
├── tests/
│   ├── test_client.py
│   ├── test_manager.py
│   └── test_coherence.py
├── environment.yml          # Anaconda environment definition
├── requirements.txt         # pip dependencies
├── setup.py                 # Package installation
└── README.md                # Quick reference
```

---

### 1.5 What This Tool Does NOT Do

❌ **Real-time streaming**: Use ThetaData Terminal WebSocket API instead
❌ **Order execution**: This is a data persistence tool, not a trading platform
❌ **Technical indicators**: Use pandas-ta, ta-lib, or similar libraries on downloaded data
❌ **Backtesting framework**: Use backtrader, zipline, VectorBT on the persisted data
❌ **Data visualization**: Use Grafana (InfluxDB), matplotlib, plotly on the data

✅ **What it DOES**: Reliably download, validate, and persist historical market data with automatic gap recovery.

---

## 2. Installation & Environment Setup

### 2.1 System Requirements

**Minimum:**
- Python 3.9+
- 4 GB RAM
- 10 GB free disk space (more for extensive historical data)
- Internet connection for API access

**Recommended:**
- Python 3.11+ (better async performance)
- 8+ GB RAM (for large tick datasets)
- SSD storage (faster I/O for Parquet/CSV)
- Anaconda/Miniconda for environment management

**Operating Systems:**
- Windows 10/11
- Linux (Ubuntu 20.04+, CentOS 8+)
- macOS 11+

---

### 2.2 Python Environment Setup

#### Using Anaconda (Recommended)

**Install Anaconda/Miniconda:**
```bash
# Download from https://www.anaconda.com/download
# or Miniconda: https://docs.conda.io/en/latest/miniconda.html

# Verify installation
conda --version
```

**Create isolated environment:**
```bash
conda create -n tdsync python=3.11 -y
conda activate tdsync
```

#### Using venv (Alternative)

```bash
python -m venv tdsync_env
# Windows
tdsync_env\Scripts\activate
# Linux/macOS
source tdsync_env/bin/activate
```

---

### 2.3 Installing tdSynchManager

#### From PyPI (when published)
```bash
pip install tdSynchManager
```

#### From Source
```bash
git clone https://github.com/fede72bari/tdSynchManager.git
cd tdSynchManager
pip install -e .
```

#### Verify Installation
```python
from tdSynchManager import ThetaSyncManager, ManagerConfig, ThetaDataV3Client
print("Installation successful!")
```

---

### 2.4 Dependencies Installation

**Core dependencies:**
```bash
pip install pandas>=2.0.0 pyarrow influxdb-client-3 aiohttp
```

**Optional dependencies:**
```bash
# For Jupyter notebooks
pip install jupyter notebook ipywidgets

# For development
pip install pytest black flake8 mypy
```

**Full dependency list** (see `requirements.txt`):
```txt
pandas>=2.0.0
pyarrow>=10.0.0
influxdb-client-3>=0.3.0
aiohttp>=3.8.0
thetadata>=0.9.0
python-dotenv>=1.0.0
```

---

### 2.5 Anaconda Environment File

Create `environment.yml` in your project directory:

```yaml
name: tdsync
channels:
  - defaults
  - conda-forge
dependencies:
  - python=3.11
  - pandas>=2.0
  - pyarrow>=10.0
  - aiohttp
  - jupyter
  - notebook
  - pip
  - pip:
    - influxdb-client-3>=0.3.0
    - thetadata>=0.9.0
    - python-dotenv
    - tdSynchManager  # If published to PyPI
```

**Create environment from file:**
```bash
conda env create -f environment.yml
conda activate tdsync
```

**Update existing environment:**
```bash
conda env update -f environment.yml --prune
```

---

## 3. ThetaData Client V3 Configuration

### 3.1 ThetaData Account Setup

**Step 1: Create Account**
1. Visit https://www.thetadata.net
2. Sign up for an account
3. Choose subscription tier:
   - **Free Tier**: Limited EOD data, delayed quotes
   - **Standard**: Real-time data, full historical EOD/intraday
   - **Professional**: Tick data, options chains, full entitlements

**Step 2: Verify Entitlements**
- Check which asset classes you have access to (stocks, options, futures)
- Verify historical data depth (e.g., 10 years vs 40 years)

---

### 3.2 Obtaining API Credentials

**Generate V3 API Token:**

1. Log in to ThetaData portal: https://portal.thetadata.net
2. Navigate to **Account Settings** → **API Keys**
3. Click **Generate New API Key (V3)**
4. **Important**: Copy the token immediately - it won't be shown again!
5. Store securely (see section 3.3)

**Token Format:**
```
thetadata_v3_abc123def456ghi789jkl012mno345pqr678stu901vwx234yz
```

**Security Best Practices:**
- ✅ Never commit tokens to git
- ✅ Use environment variables or `.env` files (add to `.gitignore`)
- ✅ Rotate tokens periodically
- ❌ Don't hardcode tokens in scripts

---

### 3.3 Client Configuration

#### Option 1: Environment Variables

**Linux/macOS:**
```bash
export THETADATA_API_TOKEN="your_token_here"
```

**Windows (PowerShell):**
```powershell
$env:THETADATA_API_TOKEN="your_token_here"
```

**Windows (CMD):**
```cmd
set THETADATA_API_TOKEN=your_token_here
```

#### Option 2: .env File (Recommended)

Create `.env` in project root:
```ini
THETADATA_API_TOKEN=your_token_here
```

Add to `.gitignore`:
```gitignore
.env
*.env
```

Load in Python:
```python
from dotenv import load_dotenv
import os

load_dotenv()
token = os.getenv("THETADATA_API_TOKEN")
```

#### Option 3: Programmatic (Not Recommended)

```python
# Only for testing - don't commit this!
client = ThetaDataV3Client(token="your_token_here")
```

---

### 3.4 ThetaData Terminal Setup (Optional)

**ThetaData Terminal** is a local application that can cache data and reduce API load.

**Installation:**
1. Download from https://www.thetadata.net/terminal
2. Install and run the application
3. Configure local endpoint: `http://127.0.0.1:25510`

**Configure Client to Use Terminal:**
```python
client = ThetaDataV3Client(
    base_url="http://127.0.0.1:25510",  # Local terminal
    token=your_token
)
```

**Benefits:**
- Reduced API quota usage
- Faster response for repeated queries
- Offline access to cached data

---

### 3.5 Testing Client Connection

**Simple connection test:**

```python
import asyncio
from tdSynchManager import ThetaDataV3Client

async def test_connection():
    async with ThetaDataV3Client() as client:
        # Test EOD download
        data = await client.get_eod(
            symbol="AAPL",
            asset="stock",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        print(f"Downloaded {len(data.splitlines())} rows for AAPL")
        print("✅ Connection successful!")

asyncio.run(test_connection())
```

**Expected output:**
```
Downloaded 22 rows for AAPL
✅ Connection successful!
```

**Troubleshooting:**
- ❌ `AuthenticationError`: Check token validity
- ❌ `ConnectionError`: Check internet connection / firewall
- ❌ `RateLimitError`: Wait and retry (quota exceeded)

---

## 4. InfluxDB Setup & Configuration

### 4.1 InfluxDB Installation

#### Windows Installation

**Download InfluxDB 3.x:**
1. Visit https://portal.influxdata.com/downloads/
2. Select **InfluxDB 3.x OSS**
3. Download Windows installer

**Install:**
```powershell
# Run installer
influxdb-3.x-windows-amd64.exe

# Or extract portable version
Expand-Archive influxdb-3.x-windows-amd64.zip -DestinationPath C:\InfluxDB
```

**Verify installation:**
```cmd
influxd version
```

#### Linux Installation (Ubuntu/Debian)

```bash
# Add InfluxData repository
wget -q https://repos.influxdata.com/influxdata-archive_compat.key
echo '23a1c8836f0afc5ed24e0486339d7cc8f6790b83886c4c96995b88a061c5bb5d influxdata-archive_compat.key' | sha256sum -c && cat influxdata-archive_compat.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg > /dev/null

echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg] https://repos.influxdata.com/debian stable main' | sudo tee /etc/apt/sources.list.d/influxdata.list

# Install
sudo apt-get update && sudo apt-get install influxdb2

# Start service
sudo systemctl start influxdb
sudo systemctl enable influxdb
```

#### macOS Installation

```bash
brew install influxdb
brew services start influxdb
```

---

### 4.2 Initial Configuration

**Start InfluxDB (first time):**

```bash
# Windows
influxd

# Linux (if not using systemd)
influxd

# macOS
brew services start influxdb
```

**Access UI:**
Open browser: http://localhost:8086

**Setup wizard:**
1. **Username**: admin
2. **Password**: [choose strong password]
3. **Organization**: YourOrg (e.g., "TradingDesk")
4. **Bucket**: ThetaData (initial bucket name)
5. Click **Continue**

**Configuration file** (auto-generated at `~/.influxdbv2/configs`):
```toml
[default]
  url = "http://localhost:8086"
  token = "your-generated-token-here"
  org = "YourOrg"
  active = true
```

---

### 4.3 Obtaining InfluxDB Token

#### Via Web UI

1. Login to http://localhost:8086
2. Click **Load Data** → **API Tokens**
3. Click **Generate API Token** → **All Access Token**
4. Name: "ThetaSync"
5. **Copy token** (starts with `apiv3_...`)

#### Via CLI

```bash
influx auth create \
  --org YourOrg \
  --all-access \
  --description "ThetaSync Token"
```

**Token format:**
```
apiv3_<YOUR_ACTUAL_TOKEN_HERE>
```

**Store securely in .env file:**
```ini
# .env file (DO NOT commit to git)
INFLUX_URL=http://127.0.0.1:8181
INFLUX_TOKEN=apiv3_your_actual_token_here
INFLUX_ORG=YourOrg
INFLUX_BUCKET=ThetaData
```

**IMPORTANT:** Never commit your actual InfluxDB token to version control. Always use environment variables loaded from a .env file.

---

### 4.4 Database Schema Design

#### Measurement Naming Convention

**Format:** `{symbol}_{asset}_{interval}`

Examples:
- `AAPL_stock_1d` - Apple daily EOD
- `ES_index_5min` - S&P 500 E-mini 5-minute bars
- `SPY_stock_tick` - SPY tick data

#### Tags vs Fields

**Tags** (indexed, for filtering):
- `symbol`: AAPL, ES, SPY
- `asset`: stock, index, option, future
- `interval`: 1d, 5min, tick
- `exchange`: (optional)

**Fields** (numerical data):
- `open`, `high`, `low`, `close`
- `volume`, `count` (trade count)
- `bid`, `ask`, `bid_size`, `ask_size` (quotes)

#### Example Schema

```python
# Point structure
{
  "measurement": "AAPL_stock_1d",
  "tags": {
    "symbol": "AAPL",
    "asset": "stock",
    "interval": "1d"
  },
  "fields": {
    "open": 150.25,
    "high": 152.10,
    "low": 149.80,
    "close": 151.50,
    "volume": 75000000
  },
  "time": "2024-01-03T21:00:00Z"  # UTC, nanosecond precision
}
```

---

### 4.5 Testing InfluxDB Connection

**Python test script:**

```python
from influxdb_client_3 import InfluxDBClient3

client = InfluxDBClient3(
    host="http://localhost:8086",
    token="your_token",
    database="ThetaData"
)

# Test write
client.write_record(
    record={
        "measurement": "test",
        "tags": {"symbol": "TEST"},
        "fields": {"value": 100},
        "time": "2024-01-01T00:00:00Z"
    }
)

# Test query
table = client.query("SELECT * FROM test LIMIT 1")
print(table.to_pandas())

client.close()
print("✅ InfluxDB connection successful!")
```

---

### 4.6 InfluxDB Startup Scripts

#### Windows: `start_influxdb.bat`

```batch
@echo off
echo Starting InfluxDB 3.x...
echo Default URL: http://localhost:8086
echo.

cd /d C:\InfluxDB
start influxd

echo InfluxDB started. Access UI at http://localhost:8086
echo Press any key to stop InfluxDB...
pause > nul

taskkill /F /IM influxd.exe
```

#### Linux/macOS: `start_influxdb.sh`

```bash
#!/bin/bash
echo "Starting InfluxDB 3.x..."
echo "Default URL: http://localhost:8086"

# Start InfluxDB
influxd &
INFLUX_PID=$!

echo "InfluxDB started (PID: $INFLUX_PID)"
echo "Press Ctrl+C to stop..."

# Wait for interrupt
trap "kill $INFLUX_PID; echo 'InfluxDB stopped'; exit" SIGINT SIGTERM
wait
```

**Make executable:**
```bash
chmod +x start_influxdb.sh
```

---

## 5. Helper Scripts & Batch Files

### 5.1 Batch Files Overview

Helper scripts automate common tasks. **Note:** All `.bat` files are for **Windows only**. Linux/macOS users should create equivalent `.sh` scripts.

| Script | Purpose | Platform |
|--------|---------|----------|
| `start_environment.bat` | **All-in-one launcher** (InfluxDB + ThetaData + Jupyter) | **Windows** |
| `start_influxdb.bat` | Start InfluxDB server | **Windows** |
| `start_thetadata.bat` | Launch ThetaData Terminal | **Windows** |
| `start_jupyter.bat` | Open Jupyter notebooks | **Windows** |
| `run_sync.bat` | Execute sync script | **Windows** |
| `*.sh` | Linux/macOS equivalents (user-created) | Unix |

---

### 5.2 start_environment.bat ⭐

**All-in-one environment launcher** - Recommended for beginners!

This script automatically starts:
1. InfluxDB 3 Enterprise
2. ThetaData Terminal (if installed)
3. Jupyter Lab in the project directory

**Location:** `start_environment.bat` (project root)

**Usage:**
```batch
REM Double-click the file or run from command line:
start_environment.bat
```

**Script Content:**
```batch
@echo off
REM Full script available in project repository
REM See: start_environment.bat
REM
REM This launcher:
REM   - Detects conda installation automatically
REM   - Starts InfluxDB in separate window
REM   - Starts ThetaData Terminal if available
REM   - Launches Jupyter Lab with tdsync environment
```

**Prerequisites:**
- Anaconda/Miniconda with `tdsync` environment
- InfluxDB 3 installed (optional but recommended)
- ThetaData Terminal (optional)

**What it does:**
1. ✅ Checks for required components
2. ✅ Starts InfluxDB in background window
3. ✅ Launches ThetaData Terminal
4. ✅ Activates conda environment `tdsync`
5. ✅ Opens Jupyter Lab in project directory

---

### 5.3 start_influxdb.bat

```batch
@echo off
title InfluxDB Server
color 0A

echo ╔════════════════════════════════════════════════╗
echo ║        InfluxDB 3.x Server Manager             ║
echo ╚════════════════════════════════════════════════╝
echo.
echo [INFO] Starting InfluxDB on port 8086...
echo [INFO] Web UI: http://localhost:8086
echo [INFO] Press Ctrl+C to stop server
echo.

cd /d C:\InfluxDB
influxd

pause
```

---

### 5.3 start_thetadata.bat

```batch
@echo off
title ThetaData Terminal
color 0B

echo ╔════════════════════════════════════════════════╗
echo ║        ThetaData Terminal Launcher             ║
echo ╚════════════════════════════════════════════════╝
echo.

set THETA_PATH=C:\Program Files\ThetaData\ThetaTerminal.exe

if exist "%THETA_PATH%" (
    echo [INFO] Launching ThetaData Terminal...
    start "" "%THETA_PATH%"
    echo [SUCCESS] Terminal started!
) else (
    echo [ERROR] ThetaData Terminal not found at:
    echo %THETA_PATH%
    echo.
    echo Please install from: https://www.thetadata.net/terminal
)

pause
```

---

### 5.4 start_jupyter.bat

```batch
@echo off
title Jupyter Notebook Server
color 0E

echo ╔════════════════════════════════════════════════╗
echo ║        Jupyter Notebook Server                 ║
echo ╚════════════════════════════════════════════════╝
echo.

REM Activate conda environment
call conda activate tdsync

echo [INFO] Starting Jupyter Notebook...
echo [INFO] Server will open in browser
echo [INFO] Press Ctrl+C in terminal to stop server
echo.

cd /d "%~dp0examples\notebooks"
jupyter notebook

pause
```

---

### 5.5 run_sync.bat

```batch
@echo off
title ThetaData Sync Manager
color 0D

echo ╔════════════════════════════════════════════════╗
echo ║        ThetaData Synchronization Runner        ║
echo ╚════════════════════════════════════════════════╝
echo.

REM Activate conda environment
call conda activate tdsync

echo [INFO] Running synchronization script...
echo.

python examples/scripts/daily_eod_sync.py

echo.
echo [INFO] Synchronization complete!
pause
```

---

### 5.7 Example Jupyter Notebook

**Single unified notebook** (in `examples/`): **`ThetaDataManager_Examples.ipynb`**

This comprehensive notebook contains **8 practical examples** demonstrating all major features of tdSynchManager using the exported library classes.

**Location:** `examples/ThetaDataManager_Examples.ipynb`

**Contents:**

1. **Setup & Imports** - Verify installation and import required libraries
2. **Example 1: Basic EOD Download to CSV** - Single symbol daily data download
3. **Example 2: Multi-Symbol EOD Download** - Parallel downloads for multiple symbols
4. **Example 3: Intraday Data to Parquet** - 5-minute bars with Parquet compression
5. **Example 4: InfluxDB Integration** - Write data directly to InfluxDB time-series database
6. **Example 5: Coherence Check & Recovery** - Automatic gap detection and backfill
7. **Example 6: Custom Discovery Policy** - Control symbol/date discovery behavior (skip/mild_skip/wild)
8. **Example 7: Querying InfluxDB Data** - Retrieve and analyze stored time-series data
9. **Example 8: Verify Data Completeness** - Check for missing dates and validate coverage

**How to use:**

```batch
REM Windows - Start Jupyter via environment launcher
start_environment.bat

REM Or manually
conda activate tdsync
jupyter lab examples/ThetaDataManager_Examples.ipynb
```

```bash
# Linux/macOS
conda activate tdsync
jupyter lab examples/ThetaDataManager_Examples.ipynb
```

**Each example includes:**
- ✅ Complete working code using `ThetaSyncManager`, `ManagerConfig`, and `Task` classes
- ✅ Expected output and verification steps
- ✅ Inline comments explaining each parameter
- ✅ Ready to run (just add your ThetaData API token)

**Prerequisites:**
- ThetaData API token set as environment variable: `THETADATA_API_TOKEN`
- For InfluxDB examples: InfluxDB 3.x running with valid token

---

## 6. Core API Reference

This chapter provides a comprehensive reference for all user-facing classes and their parameters. These are the main building blocks for using tdSynchManager.

---

### 6.1 ThetaDataV3Client

**Purpose:** Asynchronous client for ThetaData V3 API

**Import:**
```python
from tdSynchManager import ThetaDataV3Client
```

**Initialization:**
```python
client = ThetaDataV3Client(
    token=None,           # API token (default: reads from env THETADATA_API_TOKEN)
    base_url=None,        # API endpoint (default: ThetaData cloud API)
    timeout=30            # Request timeout in seconds
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `token` | `str` | `None` | ThetaData V3 API token. If `None`, reads from `THETADATA_API_TOKEN` environment variable |
| `base_url` | `str` | `None` | API endpoint URL. If `None`, uses default ThetaData cloud API. Set to `http://127.0.0.1:25510` for local ThetaData Terminal |
| `timeout` | `int` | `30` | HTTP request timeout in seconds |

**Usage (Context Manager - Recommended):**
```python
async with ThetaDataV3Client() as client:
    data = await client.get_eod(symbol="AAPL", asset="stock", start_date="2024-01-01", end_date="2024-12-31")
```

---

### 6.2 ManagerConfig

**Purpose:** Configuration object for ThetaSyncManager

**Import:**
```python
from tdSynchManager import ManagerConfig
```

**Initialization:**
```python
config = ManagerConfig(
    root_dir="./data",                # Base directory for file outputs
    max_concurrency=10,               # Max parallel downloads
    coherence_mode="off",             # Coherence checking: "off", "light", "full"
    coherence_tolerance=0.05,         # Acceptable gap threshold (5%)
    influx_url=None,                  # InfluxDB URL (e.g., "http://localhost:8086")
    influx_bucket=None,               # InfluxDB bucket/database name
    influx_token=None,                # InfluxDB authentication token
    influx_measure_prefix="",         # Prefix for InfluxDB measurements
    influx_write_batch=5000           # Batch size for InfluxDB writes
)
```

**Parameters:**

| Parameter | Type | Default | Values/Description |
|-----------|------|---------|-------------------|
| `root_dir` | `str` | `"./data"` | Base directory for CSV/Parquet outputs |
| `max_concurrency` | `int` | `10` | Maximum parallel downloads. Range: 1-50 (recommended: 5-15) |
| `coherence_mode` | `str` | `"off"` | **Options:** `"off"` (no checks), `"light"` (basic validation), `"full"` (deep validation + auto-recovery) |
| `coherence_tolerance` | `float` | `0.05` | Acceptable missing data threshold (0.05 = 5%). Used with `coherence_mode="full"` |
| `influx_url` | `str` | `None` | InfluxDB server URL. Required for `sink="influxdb"` |
| `influx_bucket` | `str` | `None` | InfluxDB bucket (database) name. Required for `sink="influxdb"` |
| `influx_token` | `str` | `None` | InfluxDB API token. Required for `sink="influxdb"` |
| `influx_measure_prefix` | `str` | `""` | Prefix for InfluxDB measurement names. Example: `"market_"` → `market_AAPL_stock_1d` |
| `influx_write_batch` | `int` | `5000` | Batch size for InfluxDB writes. Range: 1000-10000 (larger = faster but more memory) |

---

### 6.3 Task

**Purpose:** Defines a synchronization task (what to download and where to save it)

**Import:**
```python
from tdSynchManager.config import Task, DiscoverPolicy
```

**Initialization:**
```python
task = Task(
    asset="stock",                          # Asset type
    symbols=["AAPL", "MSFT"],              # List of symbols to download
    intervals=["1d", "5min"],              # List of intervals
    sink="csv",                             # Output format
    first_date_override=None,               # Start date (YYYYMMDD or "YYYY-MM-DD")
    end_date_override=None,                 # End date (YYYYMMDD or "YYYY-MM-DD")
    discover_policy=DiscoverPolicy(mode="skip"),  # Discovery behavior
    ignore_existing=False                   # Force redownload
)
```

**Parameters:**

| Parameter | Type | Default | Values/Description |
|-----------|------|---------|-------------------|
| `asset` | `str` | **Required** | **Options:** `"stock"`, `"index"`, `"option"`, `"future"` |
| `symbols` | `List[str]` | **Required** | List of ticker symbols (e.g., `["AAPL", "MSFT", "GOOGL"]`) |
| `intervals` | `List[str]` | **Required** | **Options:** `"1d"` (EOD), `"1min"`, `"5min"`, `"15min"`, `"30min"`, `"1h"`, `"4h"`, `"tick"` |
| `sink` | `str` | **Required** | **Options:** `"csv"`, `"parquet"`, `"influxdb"` |
| `first_date_override` | `str` | `None` | Start date. Format: `"20240101"` or `"2024-01-01"`. If `None`, uses default lookback |
| `end_date_override` | `str` | `None` | End date. Format: `"20241231"` or `"2024-12-31"`. If `None`, uses current date |
| `discover_policy` | `DiscoverPolicy` | `DiscoverPolicy(mode="skip")` | Controls symbol/date discovery (see 6.4) |
| `ignore_existing` | `bool` | `False` | If `True`, redownload all data even if files exist (breaks idempotency) |

---

### 6.4 DiscoverPolicy

**Purpose:** Controls automatic symbol and date range discovery behavior

**Import:**
```python
from tdSynchManager.config import DiscoverPolicy
```

**Initialization:**
```python
policy = DiscoverPolicy(mode="skip")
```

**Parameter:**

| Parameter | Type | Default | Values/Description |
|-----------|------|---------|-------------------|
| `mode` | `str` | `"skip"` | **Options:** `"skip"`, `"mild_skip"`, `"wild"` (see table below) |

**Discovery Modes:**

| Mode | Symbol Discovery | Date Range Extension | Use Case |
|------|-----------------|---------------------|----------|
| **`"skip"`** | ❌ No discovery | ❌ Use only specified dates | Precise control - download exactly what you specify |
| **`"mild_skip"`** | ✅ Discover new symbols | ❌ Keep existing date ranges | Incremental updates - add new symbols but don't extend dates |
| **`"wild"`** | ✅ Discover new symbols | ✅ Extend to current date | Continuous sync - auto-update to present |

**Examples:**

```python
# Example 1: Strict control (skip)
task = Task(
    symbols=["AAPL"],
    intervals=["1d"],
    first_date_override="20240101",
    end_date_override="20240131",
    discover_policy=DiscoverPolicy(mode="skip")
)
# Downloads ONLY AAPL from 2024-01-01 to 2024-01-31

# Example 2: Add new symbols but keep date range (mild_skip)
task = Task(
    symbols=["AAPL", "MSFT"],  # If MSFT is new, it will be added
    intervals=["1d"],
    first_date_override="20240101",
    end_date_override="20240131",
    discover_policy=DiscoverPolicy(mode="mild_skip")
)
# Downloads MSFT if not present, keeps 2024-01-01 to 2024-01-31 range

# Example 3: Continuous sync (wild)
task = Task(
    symbols=["AAPL"],
    intervals=["1d"],
    discover_policy=DiscoverPolicy(mode="wild")
)
# Extends date range to current date on each run
```

---

### 6.5 ThetaSyncManager

**Purpose:** Main synchronization manager - orchestrates downloads, validation, and writes

**Import:**
```python
from tdSynchManager import ThetaSyncManager
```

**Initialization:**
```python
manager = ThetaSyncManager(
    config=my_config,      # ManagerConfig instance
    client=my_client       # ThetaDataV3Client instance
)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `config` | `ManagerConfig` | ✅ Yes | Configuration object (see 6.2) |
| `client` | `ThetaDataV3Client` | ✅ Yes | Authenticated client instance (see 6.1) |

**Main Method: `run()`**

**Signature:**
```python
async def run(tasks: List[Task]) -> None
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `tasks` | `List[Task]` | List of Task objects to execute |

**Returns:** `None` (outputs to log and files)

**Usage:**
```python
async with ThetaDataV3Client() as client:
    manager = ThetaSyncManager(config, client=client)
    await manager.run(tasks)
```

**What `run()` does:**
1. Validates all task definitions
2. Executes tasks in parallel (up to `max_concurrency`)
3. Downloads data from ThetaData API
4. Validates and transforms data (timestamp parsing, sort, dedup)
5. Writes to specified sink (CSV/Parquet/InfluxDB)
6. Performs coherence checks (if enabled)
7. Triggers auto-recovery for gaps (if `coherence_mode="full"`)

---

## 7. Quick Start

### 6.1 First Working Example

**Simple EOD download to CSV:**

```python
import asyncio
from tdSynchManager import ManagerConfig, ThetaSyncManager, ThetaDataV3Client
from tdSynchManager.config import Task

# 1. Configure manager
config = ManagerConfig(
    root_dir="./data",        # Output directory
    max_concurrency=5         # Parallel downloads
)

# 2. Define task
tasks = [
    Task(
        asset="stock",
        symbols=["AAPL", "MSFT", "GOOGL"],
        intervals=["1d"],
        sink="csv",
        first_date_override="2024-01-01",
        end_date_override="2024-12-31"
    )
]

# 3. Run sync
async def main():
    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(config, client=client)
        await manager.run(tasks)

asyncio.run(main())
```

**Expected output:**
```
[INFO] stock AAPL 1d: Downloading 2024-01-01 to 2024-12-31...
[INFO] stock AAPL 1d: Downloaded 252 days
[EOD-BATCH][SORT] Wrote 252 total rows (0 existing + 252 new)
[SUCCESS] ✓ ./data/stock/AAPL/1d/csv/2024-01-01T00-00-00Z-AAPL-stock-1d_part01.csv

[INFO] stock MSFT 1d: Downloading 2024-01-01 to 2024-12-31...
[EOD-BATCH][SORT] Wrote 252 total rows
...
```

---

### 6.2 Minimal Configuration

**Bare minimum config:**

```python
from tdSynchManager import ManagerConfig, ThetaSyncManager, ThetaDataV3Client
from tdSynchManager.config import Task

cfg = ManagerConfig(root_dir="./data")

tasks = [Task(
    asset="stock",
    symbols=["SPY"],
    intervals=["1d"],
    sink="csv"
)]

async def sync():
    async with ThetaDataV3Client() as client:
        await ThetaSyncManager(cfg, client=client).run(tasks)

import asyncio
asyncio.run(sync())
```

**Defaults used:**
- `max_concurrency=10`
- `coherence_mode="off"` (no validation)
- Date range: last 1 year
- Discovery: `DiscoverPolicy(mode="skip")`

---

### 6.3 Running Basic Sync

**Step-by-step execution:**

```python
import asyncio
from tdSynchManager import ManagerConfig, ThetaSyncManager, ThetaDataV3Client
from tdSynchManager.config import Task, DiscoverPolicy

# Configuration
cfg = ManagerConfig(
    root_dir=r"C:\MarketData",
    max_concurrency=3
)

# Task definition
tasks = [
    Task(
        asset="stock",
        symbols=["AAPL"],
        intervals=["1d"],
        sink="csv",
        first_date_override="20240101",  # YYYYMMDD format
        end_date_override="20241231",
        discover_policy=DiscoverPolicy(mode="skip")  # Don't auto-discover
    )
]

# Run
async def run_sync():
    # Client handles authentication automatically (reads THETADATA_API_TOKEN env var)
    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

# Execute
if __name__ == "__main__":
    asyncio.run(run_sync())
```

**Run script:**
```bash
python my_sync_script.py
```

---

### 6.4 Verifying Results

**Check output file:**

```python
import pandas as pd

# Read downloaded CSV
df = pd.read_csv(
    r"C:\MarketData\stock\AAPL\1d\csv\2024-01-01T00-00-00Z-AAPL-stock-1d_part01.csv",
    dtype=str
)

print(f"Rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print(f"\nFirst 5 rows:\n{df.head()}")

# Check date coverage
df['date'] = pd.to_datetime(df['last_trade']).dt.date
print(f"\nDate range: {df['date'].min()} to {df['date'].max()}")
print(f"Trading days: {df['date'].nunique()}")
```

**Expected output:**
```
Rows: 252
Columns: ['ms_of_day', 'open', 'high', 'low', 'close', 'volume', 'count', 'last_trade']

First 5 rows:
   ms_of_day      open      high  ...
0  57600000   185.58   186.40  ...

Date range: 2024-01-02 to 2024-12-31
Trading days: 252
```

---

## 10. ThetaSyncManager

### 10.1 Initialization

**Basic initialization:**

```python
from tdSynchManager import ThetaSyncManager, ManagerConfig, ThetaDataV3Client

config = ManagerConfig(
    root_dir="./data",
    max_concurrency=5
)

async with ThetaDataV3Client() as client:
    manager = ThetaSyncManager(config, client=client)
```

**Constructor parameters:**
- `config`: ManagerConfig instance
- `client`: ThetaDataV3Client instance (must be async context manager)

---

### 10.2 run() Method

**Signature:**
```python
async def run(self, tasks: List[Task]) -> None
```

**Example:**
```python
await manager.run(tasks)
```

**What happens during `run()`:**
1. Validates all task definitions
2. Executes tasks in parallel (up to `max_concurrency`)
3. Handles errors and retries
4. Logs progress
5. Triggers coherence checks (if enabled)

---

### 10.3 Internal Pipeline

**Complete execution pipeline:**

```
┌─────────────────────────────────────────────────────────────┐
│                    manager.run(tasks)                       │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
          ┌─────────────────────────────────────┐
          │  FOR EACH TASK (parallel execution) │
          └────────────────┬────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
    Task 1             Task 2             Task N
        │                  │                  │
        └──────────────────┴──────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ PHASE 1: DISCOVERY                                           │
│  ├─ Apply DiscoverPolicy                                     │
│  ├─ Determine symbols to process                            │
│  └─ Determine date ranges                                    │
└───────────────────────────┬──────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────┐
│ PHASE 2: DOWNLOAD                                            │
│  ├─ FOR EACH (symbol, interval):                            │
│  │   ├─ Check existing files (idempotency)                  │
│  │   ├─ Determine missing dates                             │
│  │   ├─ Download from ThetaData API                         │
│  │   └─ Parse response (CSV text)                           │
│  └─ Parallel downloads (max_concurrency limit)              │
└───────────────────────────┬──────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────┐
│ PHASE 3: VALIDATION & TRANSFORMATION                         │
│  ├─ Parse timestamps (handle mixed ISO8601 formats)         │
│  │   - '2024-01-29T17:10:35.602'  (3 decimals)             │
│  │   - '2024-01-31T16:48:56.03'   (2 decimals)             │
│  │   - '2024-02-01T16:46:45'      (0 decimals)             │
│  ├─ Convert to pandas DataFrame                             │
│  ├─ Sort by timestamp (chronological)                       │
│  ├─ Deduplicate (keep='last')                               │
│  └─ Validate data integrity                                  │
└───────────────────────────┬──────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────┐
│ PHASE 4: WRITE (Multi-Sink)                                 │
│  ├─ IF sink == "csv":                                       │
│  │   ├─ Determine output file path                          │
│  │   ├─ Read existing CSV (if exists)                       │
│  │   ├─ Merge new + existing data                           │
│  │   ├─ Sort & deduplicate combined data                    │
│  │   └─ Write to CSV                                         │
│  ├─ IF sink == "parquet":                                   │
│  │   ├─ Partition by date/symbol                            │
│  │   ├─ Read existing parquet (if exists)                   │
│  │   ├─ Merge & deduplicate                                 │
│  │   └─ Write parquet with compression                      │
│  └─ IF sink == "influxdb":                                  │
│      ├─ Convert timestamps to nanosecond precision UTC      │
│      ├─ Map columns to InfluxDB schema (tags/fields)        │
│      ├─ Batch write (influx_write_batch size)               │
│      └─ Verify write with query                             │
└───────────────────────────┬──────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────┐
│ PHASE 5: COHERENCE CHECK (if enabled)                       │
│  ├─ IF coherence_mode == "off": SKIP                        │
│  ├─ IF coherence_mode == "light":                           │
│  │   ├─ Check date completeness (EOD)                       │
│  │   └─ Check hour completeness (intraday)                  │
│  └─ IF coherence_mode == "full":                            │
│      ├─ Deep validation (bucket analysis)                   │
│      ├─ Detect gaps with tolerance                          │
│      ├─ Generate recovery tasks                             │
│      └─ Optionally trigger auto-recovery                    │
└───────────────────────────┬──────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────┐
│ RECOVERY (if gaps detected)                                  │
│  ├─ Create targeted tasks for missing data                  │
│  ├─ Re-run download for gaps only                           │
│  └─ Merge recovered data with existing                      │
└───────────────────────────┬──────────────────────────────────┘
                            ▼
                    ┌───────────────┐
                    │   COMPLETE    │
                    └───────────────┘
```

**Key Points:**

1. **Discovery**: Determines WHAT to download
2. **Download**: Fetches data from ThetaData API
3. **Validation**: Ensures data quality and ordering
4. **Write**: Persists to storage (idempotent)
5. **Coherence**: Validates completeness and triggers recovery

---

### 10.4 Idempotent Operations

#### What is Idempotency?

**Idempotency** means that executing the same operation multiple times produces the same result as executing it once.

**In tdSynchManager:**
- ✅ Running the same task twice does NOT create duplicate data
- ✅ Safe to re-run failed tasks
- ✅ Gracefully handles partial downloads
- ✅ Automatic deduplication on every write

#### How Idempotency Works

**Idempotency Check Flow:**

```
Download 2024-01-27 to 2024-02-25 (30 days)
         │
         ▼
┌────────────────────────┐
│ Read existing CSV file │  ← Check what's already on disk
└──────────┬─────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│ Parse existing timestamps           │
│ Extract unique dates already saved  │
│ Example: [2024-01-27, 2024-01-28,  │
│           ..., 2024-02-10]          │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│ Filter downloaded batch             │
│ KEEP ONLY rows with dates NOT in    │
│ existing file                        │
│ Example: Keep 2024-02-11 to         │
│          2024-02-25 (15 new days)   │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│ Append new rows to existing file    │
│ Sort by timestamp                    │
│ Deduplicate (keep='last')           │
└──────────┬──────────────────────────┘
           │
           ▼
     Write to disk
```

**Example:**

```python
# First run: Download 30 days
await manager.run([task])
# Output: [EOD-BATCH][SORT] Wrote 30 total rows (0 existing + 30 new)

# Second run: Same task, same date range
await manager.run([task])
# Output: [EOD-BATCH][SORT] Wrote 30 total rows (30 existing + 0 new)
#         ↑ No duplicates created!

# Third run: Extend date range by 5 days
task.end_date_override = "20240302"
await manager.run([task])
# Output: [EOD-BATCH][SORT] Wrote 35 total rows (30 existing + 5 new)
```

#### Deduplication Logic

**Sort & Dedup Function** (simplified):

```python
def sort_and_deduplicate(df, time_column):
    # 1. Parse timestamps
    df['__parsed_time'] = pd.to_datetime(df[time_column], utc=True, errors='coerce')

    # 2. Remove invalid timestamps
    df = df[df['__parsed_time'].notna()]

    # 3. Sort chronologically
    df = df.sort_values(by='__parsed_time')

    # 4. Deduplicate (keep last occurrence)
    df = df.drop_duplicates(subset=['__parsed_time'], keep='last')

    # 5. Drop helper column
    df = df.drop(columns=['__parsed_time'])

    return df
```

**Why `keep='last'`?**
- Later downloads may have corrected data
- ThetaData sometimes updates historical values
- Ensures most recent version is preserved

#### Idempotency Guarantees

✅ **Same input → Same output**
```python
# Run 1
await manager.run(tasks)  # Creates file with 100 rows

# Run 2 (no changes)
await manager.run(tasks)  # File still has 100 rows (no duplicates)
```

✅ **Partial failures handled**
```python
# Run 1: Crashes after downloading 50/100 days
await manager.run(tasks)  # File has 50 rows

# Run 2: Completes successfully
await manager.run(tasks)  # File now has 100 rows (50 existing + 50 new)
```

✅ **Date extensions work**
```python
# Run 1: 2024-01-01 to 2024-06-30
await manager.run(tasks)  # 125 rows

# Run 2: 2024-01-01 to 2024-12-31 (extended)
await manager.run(tasks)  # 252 rows (125 existing + 127 new)
```

❌ **What's NOT idempotent:**
- Using `ignore_existing=True` (forces redownload)
- Manually editing files between runs (dedup will fix, but inefficient)

---

## 11. Sink Strategies

### 11.1 CSV Sink

#### Directory Structure

```
root_dir/
└── {asset}/              # stock, index, option, future
    └── {symbol}/         # AAPL, ES, SPY
        └── {interval}/   # 1d, 5min, tick
            └── csv/
                └── {start_date}T00-00-00Z-{symbol}-{asset}-{interval}_part01.csv
```

**Example:**
```
C:/MarketData/
└── stock/
    └── AAPL/
        └── 1d/
            └── csv/
                └── 2024-01-01T00-00-00Z-AAPL-stock-1d_part01.csv
```

#### File Naming Convention

**Format:**
```
{start_date}T00-00-00Z-{symbol}-{asset}-{interval}_part{N}.csv
```

**Components:**
- `start_date`: First date in file (ISO8601, UTC)
- `symbol`: Ticker symbol (uppercase)
- `asset`: Asset type
- `interval`: Timeframe
- `partN`: Partition number (for large files)

**Examples:**
```
2023-01-01T00-00-00Z-AAPL-stock-1d_part01.csv
2024-06-01T00-00-00Z-ES-index-5min_part01.csv
2024-11-15T00-00-00Z-SPY-stock-tick_part01.csv
```

#### Append and Idempotency Logic

**CSV Write Flow:**

```python
# Simplified CSV sink logic
def write_csv_sink(symbol, interval, new_data):
    # 1. Determine output file
    csv_file = f"{root_dir}/{asset}/{symbol}/{interval}/csv/{symbol}_{interval}.csv"

    # 2. Read existing data (if file exists)
    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file, dtype=str)
    else:
        existing_df = pd.DataFrame()

    # 3. Merge new + existing
    combined_df = pd.concat([existing_df, new_data], ignore_index=True)

    # 4. Sort & deduplicate
    combined_df = sort_and_deduplicate(combined_df, time_column='last_trade')

    # 5. Write back
    combined_df.to_csv(csv_file, index=False)

    print(f"[EOD-BATCH][SORT] Wrote {len(combined_df)} total rows "
          f"({len(existing_df)} existing + {len(new_data)} new)")
```

#### Sort and Deduplication

**Timestamp Parsing:**
```python
# Handles mixed ISO8601 formats from ThetaData
ts = pd.to_datetime(df['last_trade'], utc=True, errors='coerce')

# Examples of supported formats:
# '2024-01-29T17:10:35.602'   - 3 decimal places
# '2024-01-31T16:48:56.03'    - 2 decimal places
# '2024-02-01T16:46:45'       - no decimal places
```

**Sort:**
```python
df = df.sort_values(by='__parsed_time', ascending=True)
```

**Deduplicate:**
```python
df = df.drop_duplicates(subset=['__parsed_time'], keep='last')
```

#### Timestamp Handling (ISO8601 Variants)

**ThetaData API returns mixed formats:**

| Format | Example | Decimals |
|--------|---------|----------|
| Standard | `2024-01-29T17:10:35.602` | 3 |
| Centiseconds | `2024-01-31T16:48:56.03` | 2 |
| Seconds only | `2024-02-01T16:46:45` | 0 |

**Parsing strategy:**
```python
# Pandas 2.x handles all variants automatically with errors='coerce'
ts = pd.to_datetime(df['last_trade'], utc=True, errors='coerce')
```

**Previous issues (fixed in v1.0.9):**
- ❌ Pandas 1.x with `infer_datetime_format=True` caused exceptions
- ✅ Pandas 2.x default behavior: strict parsing without format locking

---

### 11.2 Parquet Sink

#### Partitioning Strategy

**Directory structure:**
```
root_dir/
└── {asset}/
    └── {symbol}/
        └── {interval}/
            └── parquet/
                └── {start_date}T00-00-00Z-{symbol}-{asset}-{interval}_part01.parquet
```

**Partition logic:**
- One file per symbol/interval/date range
- Large date ranges split into multiple parts
- Part size configurable (default: ~1M rows per part)

#### Schema Management

**Parquet schema (auto-inferred):**
```python
schema = {
    'ms_of_day': 'int64',
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'int64',
    'count': 'int64',
    'last_trade': 'string'  # ISO8601 timestamp
}
```

**Schema evolution:**
- Compatible changes: adding columns (auto-handled)
- Incompatible changes: changing data types (requires manual migration)

#### Compression Settings

**Default compression: SNAPPY**

```python
df.to_parquet(
    file_path,
    compression='snappy',  # Fast compression/decompression
    index=False
)
```

**Available options:**
- `snappy`: Fast, good compression ratio (default)
- `gzip`: Slower, better compression
- `brotli`: Best compression, slowest
- `zstd`: Balanced (fast + good compression)

**Recommendation:**
- EOD/Intraday: `snappy` (best balance)
- Tick data: `zstd` (large files benefit from better compression)

#### Performance Optimization Tips

**1. Use Parquet for large datasets**
```python
# CSV: ~500 MB/million rows
# Parquet (snappy): ~50 MB/million rows (10x smaller)
```

**2. Read performance**
```python
# CSV: ~10 seconds to read 10M rows
# Parquet: ~1 second to read 10M rows (10x faster)
```

**3. Column selection** (Parquet only)
```python
# Read only needed columns (impossible with CSV)
df = pd.read_parquet('data.parquet', columns=['close', 'volume'])
```

**4. Predicate pushdown**
```python
# Filter during read (not after)
import pyarrow.parquet as pq
table = pq.read_table('data.parquet', filters=[('date', '>', '2024-01-01')])
```

---

### 11.3 InfluxDB Sink

#### Measurement Naming Conventions

**Format:**
```
{symbol}_{asset}_{interval}
```

**Examples:**
- `AAPL_stock_1d`
- `ES_index_5min`
- `SPY_stock_tick`

**Customization:**
```python
cfg = ManagerConfig(
    influx_measure_prefix="market_",  # Prefix all measurements
    # Result: market_AAPL_stock_1d
)
```

#### Tags vs Fields Mapping

**Tags** (indexed, for filtering):
```python
tags = {
    'symbol': 'AAPL',
    'asset': 'stock',
    'interval': '1d'
}
```

**Fields** (numerical data):
```python
fields = {
    'open': 150.25,
    'high': 152.10,
    'low': 149.80,
    'close': 151.50,
    'volume': 75000000,
    'count': 450000
}
```

**Why this split?**
- Tags: Efficient filtering (WHERE clauses)
- Fields: Storage of actual measurements

#### Timestamp Conversion (UTC, Nanosecond Precision)

**Conversion pipeline:**

```python
# 1. Parse ISO8601 string
ts_str = '2024-01-03T21:00:00'

# 2. Convert to pandas Timestamp (UTC)
ts = pd.to_datetime(ts_str, utc=True)

# 3. Convert to nanoseconds since epoch (InfluxDB format)
ts_ns = int(ts.value)  # Example: 1704315600000000000

# 4. Write to InfluxDB
point = {
    'measurement': 'AAPL_stock_1d',
    'tags': {'symbol': 'AAPL'},
    'fields': {'close': 151.50},
    'time': ts_ns  # Nanosecond precision
}
```

**Timezone handling:**
```python
# InfluxDB stores ALL timestamps in UTC
# Query results can be converted to local timezone client-side
```

#### Batch Writing Strategies

**Default batch size: 5000**

```python
cfg = ManagerConfig(
    influx_write_batch=5000  # Write 5000 points per batch
)
```

**Batch write flow:**

```
Download 10,000 rows
        │
        ▼
┌────────────────────┐
│ Split into batches │
│ [5000, 5000]       │
└─────────┬──────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
Batch 1      Batch 2
(5000)       (5000)
    │           │
    └─────┬─────┘
          │
          ▼
  Write to InfluxDB
```

**Tuning batch size:**
- **Too small** (e.g., 100): Many API calls, slow
- **Too large** (e.g., 50000): Memory issues, timeout risk
- **Optimal** (5000-10000): Balanced performance

#### Query-Based Verification

**After write, verify with query:**

```python
# Write data
await write_to_influxdb(df, measurement='AAPL_stock_1d')

# Verify
client = InfluxDBClient3(host=influx_url, token=token, database=bucket)
query = """
SELECT COUNT(*)
FROM AAPL_stock_1d
WHERE time >= '2024-01-01T00:00:00Z'
  AND time < '2025-01-01T00:00:00Z'
"""
result = client.query(query)
count = result.to_pandas().iloc[0, 0]

print(f"Verified: {count} rows written")
client.close()
```

**Common verification queries:**

```sql
-- Check date range
SELECT MIN(time), MAX(time) FROM AAPL_stock_1d

-- Check for duplicates
SELECT time, COUNT(*) FROM AAPL_stock_1d GROUP BY time HAVING COUNT(*) > 1

-- Check data completeness
SELECT COUNT(*) FROM AAPL_stock_1d WHERE close IS NULL
```

---

## 14. Timestamp Handling

### 14.1 Supported ISO8601 Formats

ThetaData API returns timestamps in **ISO8601 format** but with **inconsistent decimal precision**:

**Supported formats:**

| Format | Example | Precision | Notes |
|--------|---------|-----------|-------|
| Milliseconds (3 decimals) | `2024-01-29T17:10:35.602` | 0.001s | Most common |
| Centiseconds (2 decimals) | `2024-01-31T16:48:56.03` | 0.01s | Rare |
| Seconds only (0 decimals) | `2024-02-01T16:46:45` | 1s | EOD timestamps |

**Key characteristics:**
- ✅ All timestamps are in **local exchange time** (not UTC)
- ✅ No timezone suffix (`Z` or `+00:00`)
- ✅ Missing milliseconds are valid (not an error)

**Parsing strategy:**
```python
# Pandas 2.x automatically handles all variants
ts = pd.to_datetime(df['last_trade'], utc=True, errors='coerce')
```

---

### 14.2 Pandas Compatibility

#### Pandas 1.x Issues (DEPRECATED)

**Problem:**
```python
# Pandas 1.4.2 with default infer_datetime_format=True
ts = pd.to_datetime(df['last_trade'], utc=True)

# First row: '2024-01-29T17:10:35.602'
# Pandas infers format: '%Y-%m-%dT%H:%M:%S.%f'

# Later row: '2024-02-01T16:46:45'
# Pandas tries to apply inferred format → FAILS!
# Exception: "time data '2024-02-01T16:46:45' doesn't match format '%Y-%m-%dT%H:%M:%S.%f'"
```

**Fix for Pandas 1.x:**
```python
ts = pd.to_datetime(df['last_trade'], utc=True, errors='coerce', infer_datetime_format=False)
```

#### Pandas 2.x (RECOMMENDED)

**Solution:**
```python
# Pandas 2.x: infer_datetime_format parameter is DEPRECATED
# Default behavior: strict parsing without format locking
ts = pd.to_datetime(df['last_trade'], utc=True, errors='coerce')
```

**Version recommendation:**
```bash
pip install pandas>=2.0.0
```

**Verification:**
```python
import pandas as pd
print(pd.__version__)  # Should be 2.x.x
```

---

### 14.3 Timezone Management

#### ThetaData Timestamps

**Exchange time vs UTC:**
- ThetaData returns timestamps in **exchange local time**
- **US stocks/indices**: Eastern Time (ET)
- **No timezone suffix** in API response

**Example:**
```
Last trade: 2024-01-03T16:00:00  # This is 4:00 PM ET (market close)
```

#### UTC Normalization

**Manager normalizes to UTC:**

```python
# Parse as UTC (even though it's ET)
ts_utc = pd.to_datetime('2024-01-03T16:00:00', utc=True)
# Result: 2024-01-03 16:00:00+00:00

# For InfluxDB: store as UTC nanoseconds
ts_ns = int(ts_utc.value)
```

**⚠️ Important:**
- Timestamps are stored "as-is" (interpreted as UTC)
- **User responsibility**: Convert to local timezone when querying
- This avoids daylight saving time complexity

#### Converting to Local Timezone

**Example: Convert UTC to ET:**

```python
import pandas as pd

# Read data
df = pd.read_csv('data.csv')
df['timestamp'] = pd.to_datetime(df['last_trade'], utc=True)

# Convert to Eastern Time
df['timestamp_et'] = df['timestamp'].dt.tz_convert('America/New_York')

print(df[['timestamp', 'timestamp_et']].head())
```

**Output:**
```
                 timestamp            timestamp_et
0 2024-01-03 16:00:00+00:00 2024-01-03 11:00:00-05:00  # 4 PM UTC → 11 AM ET
```

---

### 14.4 EOD vs Intraday vs Tick Timestamps

#### EOD (End-of-Day)

**Characteristics:**
- **Format**: Usually no milliseconds (`2024-01-03T16:00:00`)
- **Time**: Fixed to market close (4:00 PM ET for US stocks)
- **Timezone**: Eastern Time (stored as UTC)

**Example:**
```csv
last_trade,open,high,low,close,volume
2024-01-03T21:00:00,150.25,152.10,149.80,151.50,75000000
```

#### Intraday Bars

**Characteristics:**
- **Format**: Usually with milliseconds (`2024-01-03T09:30:00.000`)
- **Time**: Bar close time
- **Frequency**: 1min, 5min, 15min, etc.

**Example (5min bars):**
```csv
last_trade,open,high,low,close,volume
2024-01-03T14:30:00.000,150.25,150.50,150.20,150.45,125000
2024-01-03T14:35:00.000,150.45,150.60,150.40,150.55,130000
```

#### Tick Data

**Characteristics:**
- **Format**: Microsecond precision (`2024-01-03T09:30:00.123456`)
- **Time**: Exact trade/quote timestamp
- **Volume**: Massive (millions of ticks per day)

**Example (trades):**
```csv
timestamp,price,size,exchange,conditions
2024-01-03T14:30:00.123456,150.25,100,Q,@
2024-01-03T14:30:00.125789,150.26,200,N,@FT
```

---

### 14.5 Known Issues and Solutions

#### Issue 1: Mixed Timestamp Formats

**Symptom:**
```
[WARN] time data "2024-02-01T16:46:45" doesn't match format "%Y-%m-%dT%H:%M:%S.%f"
```

**Cause:**
- Pandas 1.x with `infer_datetime_format=True`
- Format inferred from first row, fails on later rows

**Solution:**
```python
# Upgrade to pandas 2.x
pip install --upgrade pandas>=2.0.0

# Or use infer_datetime_format=False (pandas 1.x)
ts = pd.to_datetime(df['last_trade'], utc=True, errors='coerce', infer_datetime_format=False)
```

**Status:** ✅ Fixed in tdSynchManager v1.0.9

---

#### Issue 2: Timezone Confusion

**Symptom:**
```python
# Market close is 4 PM ET, but stored as 4 PM UTC
df['last_trade'] = '2024-01-03T16:00:00'  # Is this 4 PM ET or 4 PM UTC?
```

**Solution:**
- **Store as-is** (manager does this automatically)
- **Convert to ET client-side** when needed:
  ```python
  df['timestamp'] = pd.to_datetime(df['last_trade'], utc=True)
  df['timestamp_et'] = df['timestamp'].dt.tz_convert('America/New_York')
  ```

---

#### Issue 3: Daylight Saving Time

**Symptom:**
- Market hours shift during DST transitions
- 9:30 AM ET can be UTC-4 or UTC-5 depending on season

**Solution:**
- Store timestamps without timezone offset (as manager does)
- Convert to ET with `tz_convert('America/New_York')` which handles DST automatically

**Example:**
```python
# Winter (EST): 9:30 AM ET = 2:30 PM UTC (UTC-5)
# Summer (EDT): 9:30 AM ET = 1:30 PM UTC (UTC-4)

# pandas handles this automatically:
ts = pd.to_datetime('2024-01-03T09:30:00').tz_localize('America/New_York')
print(ts)  # 2024-01-03 09:30:00-05:00 (EST)

ts = pd.to_datetime('2024-07-03T09:30:00').tz_localize('America/New_York')
print(ts)  # 2024-07-03 09:30:00-04:00 (EDT)
```

---

## Appendices

### Appendix A: Glossary of Terms

| Term | Definition |
|------|------------|
| **Idempotency** | Property where running the same operation multiple times produces the same result as running it once |
| **Coherence** | Measure of data completeness and consistency |
| **EOD** | End-of-Day - daily OHLCV bars |
| **Intraday** | Intra-day bars (1min, 5min, etc.) |
| **Tick** | Individual trade, quote, or OHLC tick |
| **Sink** | Output destination (CSV, Parquet, InfluxDB) |
| **Discovery Policy** | Strategy for finding symbols/dates to download |
| **Bucket** | Intraday hour segment (e.g., 9-10 AM bucket) |
| **Gap** | Missing data in expected date/time range |
| **Recovery** | Process of backfilling missing data |
| **Measurement** | InfluxDB table name |
| **Tag** | InfluxDB indexed metadata field |
| **Field** | InfluxDB numerical data column |

---

### Appendix G: Example Batch Scripts

#### run_daily_sync.bat (Windows)

```batch
@echo off
title Daily EOD Sync
color 0A

REM Activate conda environment
call conda activate tdsync

REM Run sync script
echo [%date% %time%] Starting daily EOD synchronization...
python C:\Trading\scripts\daily_eod_sync.py >> C:\Trading\logs\sync.log 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%date% %time%] Sync completed successfully.
) else (
    echo [%date% %time%] ERROR: Sync failed with code %ERRORLEVEL%
    pause
)
```

#### run_daily_sync.sh (Linux/macOS)

```bash
#!/bin/bash
LOG_FILE="/home/trading/logs/sync.log"

echo "[$(date)] Starting daily EOD synchronization..." | tee -a "$LOG_FILE"

# Activate conda
source ~/anaconda3/etc/profile.d/conda.sh
conda activate tdsync

# Run sync
python /home/trading/scripts/daily_eod_sync.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "[$(date)] Sync completed successfully." | tee -a "$LOG_FILE"
else
    echo "[$(date)] ERROR: Sync failed with code $?" | tee -a "$LOG_FILE"
    exit 1
fi
```

**Schedule with cron (Linux):**
```bash
# Edit crontab
crontab -e

# Add job (run daily at 6 PM ET)
0 18 * * 1-5 /home/trading/scripts/run_daily_sync.sh
```

---

### Appendix H: Anaconda Environment Files

#### environment.yml (Full Setup)

```yaml
name: tdsync
channels:
  - defaults
  - conda-forge
dependencies:
  # Python
  - python=3.11

  # Core data libraries
  - pandas>=2.0.0
  - pyarrow>=10.0.0
  - numpy>=1.24.0

  # Async/networking
  - aiohttp>=3.8.0
  - requests>=2.28.0

  # Jupyter
  - jupyter
  - notebook
  - ipywidgets
  - matplotlib
  - plotly

  # Development tools
  - black
  - flake8
  - mypy
  - pytest
  - pytest-asyncio

  # pip-only packages
  - pip
  - pip:
    - influxdb-client-3>=0.3.0
    - thetadata>=0.9.0
    - python-dotenv>=1.0.0
    - tdSynchManager  # If published to PyPI
```

**Create environment:**
```bash
conda env create -f environment.yml
conda activate tdsync
```

**Export current environment:**
```bash
conda env export > environment.yml
```

**Update environment:**
```bash
conda env update -f environment.yml --prune
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

---

## Support

- **Issues**: https://github.com/fede72bari/tdSynchManager/issues
- **Discussions**: https://github.com/fede72bari/tdSynchManager/discussions
- **Email**: support@tdsyncmanager.dev

---

**End of Manual**

*Last updated: December 2025*
*Version: 1.0.9*
