from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
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


@dataclass
class MultiFileState:
    """State for one file in multi-file workflow.
    
    Tracks the state of a single file through the multi-file workflow,
    including raw data, cleaned data, removed rows, and per-step results.
    
    Attributes:
        raw_df: Original uploaded data
        cleaned_df: Current cleaned state after processing
        removed_df: Accumulated removed rows across all steps
        filename: Original filename from upload
        is_uploaded: Whether a file has been uploaded to this slot
        step_results: Per-step results for detailed tracking (step number -> StepResult)
    """
    raw_df: Optional[pd.DataFrame] = None       # Original uploaded data
    cleaned_df: Optional[pd.DataFrame] = None   # Current cleaned state
    removed_df: Optional[pd.DataFrame] = None   # Accumulated removed rows
    filename: Optional[str] = None              # Original filename
    is_uploaded: bool = False                   # Upload status
    
    # Per-step results for detailed tracking
    step_results: Dict[int, StepResult] = field(default_factory=dict)


@dataclass
class MultiFileWorkflowState:
    """Complete state for multi-file workflow.
    
    Tracks the complete state of the multi-file workflow, including all 5 file states,
    current workflow step, shared suppression data, and column mapping.
    
    Attributes:
        files: List of 5 MultiFileState instances (index 0-4)
        current_step: Current workflow step (1-8)
        workflow_mode: Workflow mode - "single" or "multi"
        tcpa_dnc_data: Shared TCPA DNC suppression data
        tcpa_zips_data: Shared TCPA zip codes suppression data
        tcpa_phones_data: Shared TCPA phones suppression data
        master_phone_list: Set of normalized phone numbers from master phone list
        column_mapping: Column mapping shared across all files
    
    Requirements: 9.1, 9.4
    """
    files: List[MultiFileState] = field(default_factory=list)  # 5 file states (index 0-4)
    current_step: int = 1                                       # Current workflow step (1-8)
    workflow_mode: str = "multi"                                # "single" or "multi"
    
    # Shared suppression data
    tcpa_dnc_data: Optional[pd.DataFrame] = None
    tcpa_zips_data: Optional[pd.DataFrame] = None
    tcpa_phones_data: Optional[pd.DataFrame] = None
    master_phone_list: Optional[Set[str]] = None
    
    # Column mapping (shared across all files)
    column_mapping: ColumnMapping = field(default_factory=ColumnMapping)
