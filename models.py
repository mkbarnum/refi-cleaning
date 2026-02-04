from dataclasses import dataclass
from typing import Optional
import pandas as pd


@dataclass
class ColumnMapping:
    """Maps raw data columns to expected field names.
    
    Expected columns from raw data file:
    - Phone: Phone1 (primary), Phone2 (secondary optional)
    - Names: FirstName, LastName
    - Email: Email
    - Location: ZipCode
    - ID: Universal_LeadId
    """
    phone: Optional[str] = None          # Maps to Phone1
    first_name: Optional[str] = None     # Maps to FirstName
    last_name: Optional[str] = None      # Maps to LastName
    email: Optional[str] = None          # Maps to Email
    zip_code: Optional[str] = None       # Maps to ZipCode
    lead_id: Optional[str] = None        # Maps to Universal_LeadId


# Common column name patterns for auto-detection
COLUMN_PATTERNS = {
    'phone': ['Phone1', 'Phone', 'PhoneNumber', 'phone1', 'phone'],
    'first_name': ['FirstName', 'First_Name', 'First Name', 'firstname'],
    'last_name': ['LastName', 'Last_Name', 'Last Name', 'lastname'],
    'email': ['Email', 'EmailAddress', 'email', 'E-mail'],
    'zip_code': ['ZipCode', 'Zip_Code', 'Zip Code', 'Zip', 'zipcode', 'zip'],
    'lead_id': ['Universal_LeadId', 'LeadId', 'Lead_Id', 'UUID', 'ID'],
}


@dataclass
class CleanResult:
    """Result of a single cleaning operation."""
    cleaned_df: pd.DataFrame
    removed_df: pd.DataFrame
    removed_count: int
    reason: str


@dataclass
class StepResult:
    """Result of a complete cleaning step (Step 1 or Step 2)."""
    cleaned_df: pd.DataFrame
    all_removed_df: pd.DataFrame
    before_count: int
    after_count: int
    removal_summary: dict[str, int]  # reason -> count
