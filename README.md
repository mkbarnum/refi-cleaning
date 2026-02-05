# Refinance Data Cleansing

A Streamlit web application for cleaning and validating refinance lead data through a multi-step pipeline with TCPA/DNC suppression list matching.

## Overview

This tool processes raw refinance lead data files through a 6-step cleaning pipeline, removing invalid entries, matching against Do Not Call (DNC) lists, and deduplicating phone numbers across multiple weekly files.

## Features

### Step 1: Upload Raw Data
- Upload Excel (.xlsx, .xls) or CSV files
- Validates required columns: DateReceived, FirstName, LastName, Email, Phone1, StreetAddress, City, State, ZipCode, DesiredLoanAmount, FirstMortgageBalance, ExistingPropertyValue, Universal_LeadId
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

## Project Structure

```
├── app.py              # Streamlit UI and workflow orchestration
├── cleaning.py         # Data cleaning functions (validation, filtering)
├── file_io.py          # File reading/writing, Excel highlight detection
├── matching.py         # TCPA/DNC matching functions
├── models.py           # Data models (ColumnMapping, CleanResult, StepResult)
├── requirements.txt    # Python dependencies
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
   git clone https://github.com/YOUR_USERNAME/refinance-data-cleansing.git
   cd refinance-data-cleansing
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
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
