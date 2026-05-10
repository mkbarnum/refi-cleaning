# Refinance Data Cleansing

A Streamlit web application for cleaning and validating refinance lead data through a multi-step pipeline with TCPA/DNC suppression list matching.

## Overview

This tool processes raw refinance lead data files through a cleaning pipeline, removing invalid entries, matching against Do Not Call (DNC) lists, suppressing phone numbers, deduplicating across files, and filtering by state. The application supports two workflows:

- **Single-File Workflow (Clean 1 File)** — An 8-step pipeline for processing one data file at a time
- **Multi-File Workflow (Clean 5 Files)** — A 10-step pipeline for processing 5 weekly files together with cross-file deduplication and master phone suppression

## Home Page

On launch, the app displays a home page with the company logo and two workflow options:
- **📄 Clean 1 File** — Single-file workflow
- **📁 Clean 5 Files** — Multi-file workflow
- **🗑️ Clear and Start Over** — Resets all workflow state (shown when existing data is present)

---

## Single-File Workflow (8 Steps)

### Step 1: Upload Raw Data
- Upload Excel (.xlsx, .xls) or CSV files
- Validates required columns (see below)
- Automatically drops extra columns not in the required set

### Step 2: Clean Bad Data
- Removes highlighted rows from Excel files (cells with background color)
- Filters invalid last names (must start with a letter)
- Filters empty or invalid phone numbers (must be exactly 10 digits, not starting with 1)
- Filters invalid emails (must contain exactly one @ with characters before and after)
- Removes rows containing "TEST" in first or last name
- Filters placeholder emails (N/A, No, Nada, Na, NoEmail, None)
- Detects and removes fake/suspicious emails (gibberish patterns, disposable domains, refusal patterns)
- Filters prohibited content ("loan depot", profanity)
- Validates UUID format for Universal_LeadId (8-4-4-4-12 hex pattern)

### Step 3: TCPA DNC File
- Upload TCPA LD DNC suppression file
- Filters by DNC phone numbers (exact match)
- Filters by blocked area codes (first 3 digits)
- Filters by name match (FirstName + LastName concatenation)

### Step 4: Zip Code Removal
- Upload zip codes suppression file
- Removes rows with matching zip codes (first 5 digits)

### Step 5: Phone Number Removal
- Upload TCPA phones suppression file
- Removes rows with matching phone numbers
- Deduplicates phone numbers within the file (keeps one random row per unique phone)
- Provides aggregated removal summary across all steps
- Download cleaned data and removed rows with highlighted problem cells

### Step 6: Cross-File Deduplication
- Deduplicate phone numbers across 5 weekly files
- File 1 (newest, from Steps 1-5) keeps all rows as reference
- Upload 4 additional pre-cleaned files (Files 2-5)
- Each older file has duplicates removed against all newer files:
  - File 5 (oldest): removes phones in Files 1-4
  - File 4: removes phones in Files 1-3
  - File 3: removes phones in Files 1-2
  - File 2: removes phones in File 1
- Download all 5 deduplicated files

### Step 7: Bad States
- Remove rows where the State column matches a configurable list of "bad" states (e.g., AZ, DE, TX)
- Upload a custom bad-states list or use the default set

### Step 8: Clean against Billing
- Upload 2 billing Excel files
- Removes rows from the cleaned data where the phone number matches any phone in either billing file
- Download final cleaned file

---

## Multi-File Workflow (10 Steps + Final Download)

### Step 1: Upload 5 Files
- Upload 5 data files ordered from newest (File 1) to oldest (File 5)
- Each file is validated for required columns
- One file per upload slot; all 5 must be uploaded before proceeding

### Step 2: Clean Bad Data
- Applies the same cleaning operations as the single-file Step 2 to all 5 files simultaneously
- Highlighted row removal (File 1 only — requires raw bytes for openpyxl detection)
- Per-file removal statistics displayed in a summary table

### Step 3: TCPA DNC File
- Upload one TCPA LD DNC suppression file (shared across all 5 files)
- Filters all 5 files by DNC phone numbers, area codes, and name matches

### Step 4: Zip Code Removal
- Upload one zip codes suppression file (shared across all 5 files)
- Removes matching zip codes from all 5 files

### Step 5: Phone Number Removal
- Upload one TCPA phones suppression file (shared across all 5 files)
- Removes matching phone numbers and deduplicates within each file

### Step 6: Download Cleaned Files
- Intermediate download checkpoint
- Download all 5 cleaned files and their removed rows as a single ZIP archive
- Files named with original filename + "(CLEANED)" or "(REMOVED)" suffix

### Step 7: Master Phone Suppression
- Upload a master phone list Excel file (supports multiple tabs)
- Extracts and normalizes phone numbers from ALL tabs
- Filters all 5 files against the master phone list

### Step 8: Cross-File Dedupe
- Deduplicates phone numbers across all 5 files
- File 1 (newest) keeps all rows as reference
- Each older file has duplicates removed against all newer files

### Step 9: Bad States
- Remove rows matching configurable bad states from all 5 files
- Per-file removal statistics

### Step 10: Clean against Billing
- Upload 2 billing Excel files
- Removes rows from File 1 (newest) where the phone number matches any phone in either billing file
- Files 2-5 are not modified in this step

### Final Download
- Download all 5 final files as a single ZIP archive
- ZIP contains final cleaned files at root and a `removed_rows/` folder with removed rows (including a Reason column and yellow-highlighted problem cells)

---

## Required Columns

Each data file must contain these columns:
- DateReceived
- FirstName
- LastName
- Email
- Phone1
- StreetAddress
- City
- State
- ZipCode
- DesiredLoanAmount
- FirstMortgageBalance
- ExistingPropertyValue
- Universal_LeadId

## Project Structure

```
├── app.py              # Streamlit UI and workflow orchestration (single + multi-file)
├── cleaning.py         # Data cleaning functions (validation, filtering, deduplication)
├── file_io.py          # File reading/writing, Excel highlight detection, ZIP export
├── matching.py         # TCPA/DNC matching functions (phones, area codes, names, zips)
├── models.py           # Data models (ColumnMapping, CleanResult, StepResult, MultiFileState, MultiFileWorkflowState)
├── requirements.txt    # Python dependencies
├── suppression_files/  # Suppression list files (DNC, phones, zip codes)
│   ├── dnc/
│   ├── master_phones/
│   ├── phones/
│   └── zip_codes/
├── final_testing/      # Test files for validation
│   ├── original/
│   ├── cleaned/
│   ├── cleaned_by_tool/
│   └── dnc_files/
└── tests/
    └── test_properties.py  # Property-based tests using Hypothesis
```

## Local Development

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd refinance-data-cleansing
   ```

2. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running Locally

```bash
streamlit run app.py
```

The app will open at `http://localhost:8501`

### Dependencies

- **streamlit** (>=1.28.0) — Web UI framework
- **pandas** (>=2.0.0) — Data manipulation
- **openpyxl** (>=3.1.0) — Excel reading/writing and highlight detection
- **python-calamine** (>=0.2.0) — Fast Excel reading engine (optional, falls back to openpyxl)
- **hypothesis** (>=6.0.0) — Property-based testing
- **pytest** (>=7.0.0) — Test runner

## Running Tests

The project uses property-based testing with Hypothesis to verify correctness:

```bash
pytest tests/
```

Tests cover:
- File format validation
- Phone number normalization and validation
- Last name validation
- Email validation
- Highlighted row removal
- Area code, name, phone, and zip code matching
- TEST entry detection
- Placeholder email detection
- Prohibited content detection
- Duplicate phone removal
- UUID format validation

## Deployment

This app can be deployed on [Streamlit Community Cloud](https://streamlit.io/cloud) or any platform supporting Python web applications.
