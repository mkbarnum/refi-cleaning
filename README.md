# Refinance Data Cleansing

A Streamlit web application for cleaning refinance lead data through a multi-step pipeline with TCPA suppression list matching.

## Live App

Access the app at: [leena.becomeselfless.org](https://leena.becomeselfless.org) (or your Streamlit Cloud URL)

## Features

- Upload and validate raw lead data files (Excel/CSV)
- Multi-step cleaning pipeline:
  - Remove highlighted rows from Excel files
  - Filter invalid names, phones, and emails
  - Filter fake/placeholder emails
  - Remove prohibited content
  - Validate UUIDs
- TCPA DNC list matching
- Zip code filtering
- Phone number filtering and deduplication
- Cross-file deduplication
- Download cleaned data and removed rows at each step

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

## Deployment

This app is deployed on [Streamlit Community Cloud](https://streamlit.io/cloud).

## Running Tests

```bash
pytest
```
