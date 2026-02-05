"""Refinance Data Cleansing - Streamlit Application."""

import streamlit as st
import pandas as pd
import time
from io import BytesIO
from typing import Callable, List

from models import ColumnMapping, CleanResult, StepResult, MultiFileState, MultiFileWorkflowState
from file_io import (
    read_uploaded_file, read_excel_with_highlights,
    export_to_excel, export_removed_rows_to_excel,
    export_to_zip, is_valid_file_format, get_file_extension
)
from cleaning import (
    remove_highlighted_rows, filter_invalid_last_names,
    filter_invalid_phones, filter_empty_phones, filter_invalid_emails,
    filter_test_entries, filter_placeholder_emails, filter_prohibited_content,
    remove_duplicate_phones, filter_invalid_uuid, normalize_phone,
    validate_required_columns, filter_to_required_columns, REQUIRED_COLUMNS,
    filter_fake_emails, dedupe_against_files
)
from matching import (
    load_tcpa_phones, load_tcpa_zipcodes, load_ld_dnc,
    filter_by_area_code, filter_by_name_match, filter_by_dnc_phones,
    filter_by_tcpa_phones, filter_by_tcpa_zips, load_phones_from_all_tabs
)


def load_file_with_progress(file_bytes: bytes, filename: str, container=None) -> pd.DataFrame:
    """Load a file with progress bar during parsing.
    
    Args:
        file_bytes: Raw file bytes
        filename: Original filename (for format detection)
        container: Optional Streamlit container to render progress in
        
    Returns:
        Loaded DataFrame
    """
    ctx = container if container else st
    ext = get_file_extension(filename)
    file_size_mb = len(file_bytes) / (1024 * 1024)
    
    progress_bar = ctx.progress(0)
    status_text = ctx.empty()
    
    status_text.write(f"📂 Reading file ({file_size_mb:.1f} MB)...")
    progress_bar.progress(10)
    
    # Simulate staged progress during parsing
    status_text.write("⏳ Parsing file structure...")
    progress_bar.progress(30)
    
    # Actually read the file
    df = read_uploaded_file(BytesIO(file_bytes), filename)
    
    progress_bar.progress(70)
    status_text.write(f"📊 Processing {len(df):,} rows...")
    
    progress_bar.progress(90)
    status_text.write("✅ Finalizing...")
    
    progress_bar.progress(100)
    time.sleep(0.3)  # Brief pause to show completion
    
    # Clear progress elements
    progress_bar.empty()
    status_text.empty()
    
    return df


def init_session_state():
    """Initialize session state variables."""
    # Workflow mode: None (home page), "single", or "multi"
    if 'workflow_mode' not in st.session_state:
        st.session_state.workflow_mode = None
    if 'raw_data' not in st.session_state:
        st.session_state.raw_data = None
    # Multi-file workflow state (Requirement 9.1)
    if 'multi_file_state' not in st.session_state:
        st.session_state.multi_file_state = None
    if 'do_cleaning' not in st.session_state:
        st.session_state.do_cleaning = False
    if 'cleaning_done' not in st.session_state:
        st.session_state.cleaning_done = False
    if 'raw_file_bytes' not in st.session_state:
        st.session_state.raw_file_bytes = None
    if 'raw_file_ext' not in st.session_state:
        st.session_state.raw_file_ext = None
    if 'tcpa_phones_data' not in st.session_state:
        st.session_state.tcpa_phones_data = None
    if 'tcpa_ld_dnc_data' not in st.session_state:
        st.session_state.tcpa_ld_dnc_data = None
    if 'tcpa_zips_data' not in st.session_state:
        st.session_state.tcpa_zips_data = None
    if 'column_mapping' not in st.session_state:
        st.session_state.column_mapping = ColumnMapping()
    if 'step1_result' not in st.session_state:
        st.session_state.step1_result = None
    if 'step2_result' not in st.session_state:
        st.session_state.step2_result = None
    if 'step3_result' not in st.session_state:
        st.session_state.step3_result = None
    if 'step4_result' not in st.session_state:
        st.session_state.step4_result = None
    if 'current_step' not in st.session_state:
        st.session_state.current_step = "1. Upload Raw Data"
    # Step 6: Cross-file deduplication state
    if 'file2_data' not in st.session_state:
        st.session_state.file2_data = None
    if 'file3_data' not in st.session_state:
        st.session_state.file3_data = None
    if 'file4_data' not in st.session_state:
        st.session_state.file4_data = None
    if 'file5_data' not in st.session_state:
        st.session_state.file5_data = None
    if 'file2_deduped' not in st.session_state:
        st.session_state.file2_deduped = None
    if 'file3_deduped' not in st.session_state:
        st.session_state.file3_deduped = None
    if 'file4_deduped' not in st.session_state:
        st.session_state.file4_deduped = None
    if 'file5_deduped' not in st.session_state:
        st.session_state.file5_deduped = None
    # Deduplication before/after counts
    if 'file2_dedupe_counts' not in st.session_state:
        st.session_state.file2_dedupe_counts = None
    if 'file3_dedupe_counts' not in st.session_state:
        st.session_state.file3_dedupe_counts = None
    if 'file4_dedupe_counts' not in st.session_state:
        st.session_state.file4_dedupe_counts = None
    if 'file5_dedupe_counts' not in st.session_state:
        st.session_state.file5_dedupe_counts = None


# Single-file workflow steps (existing behavior)
SINGLE_FILE_STEPS = [
    "1. Upload Raw Data",
    "2. Clean Bad Data",
    "3. TCPA DNC File",
    "4. Zip Code Removal",
    "5. Phone Number Removal",
    "6. Cross-File Dedupe"
]

# Multi-file workflow steps (new 8-step workflow)
MULTI_FILE_STEPS = [
    "Home",
    "1. Upload 5 Files",
    "2. Clean Bad Data",
    "3. TCPA DNC File",
    "4. Zip Code Removal",
    "5. Phone Number Removal",
    "6. Download Cleaned Files",
    "7. Master Phone Suppression",
    "8. Cross-File Dedupe",
    "Final Download"
]

# Alias for backward compatibility
STEPS = SINGLE_FILE_STEPS

# Logo configuration - path to company logo image
LOGO_PATH = "2422678.png"


def clear_single_file_state():
    """Clear all single-file workflow state.
    
    Clears session state variables specific to the single-file workflow,
    ensuring workflow state isolation when switching between workflows.
    
    Requirements: 9.3, 9.4
    """
    # Clear single-file workflow data
    st.session_state.raw_data = None
    st.session_state.raw_file_bytes = None
    st.session_state.raw_file_ext = None
    st.session_state.step1_result = None
    st.session_state.step2_result = None
    st.session_state.step3_result = None
    st.session_state.step4_result = None
    
    # Clear single-file TCPA data
    st.session_state.tcpa_phones_data = None
    st.session_state.tcpa_ld_dnc_data = None
    st.session_state.tcpa_zips_data = None
    
    # Clear cross-file dedupe state (single-file workflow)
    st.session_state.file2_data = None
    st.session_state.file3_data = None
    st.session_state.file4_data = None
    st.session_state.file5_data = None
    st.session_state.file2_deduped = None
    st.session_state.file3_deduped = None
    st.session_state.file4_deduped = None
    st.session_state.file5_deduped = None
    st.session_state.file2_dedupe_counts = None
    st.session_state.file3_dedupe_counts = None
    st.session_state.file4_dedupe_counts = None
    st.session_state.file5_dedupe_counts = None
    
    # Clear cleaning flags
    st.session_state.do_cleaning = False
    st.session_state.cleaning_done = False
    
    # Reset column mapping
    st.session_state.column_mapping = ColumnMapping()


def clear_multi_file_state():
    """Clear all multi-file workflow state.
    
    Clears session state variables specific to the multi-file workflow,
    ensuring workflow state isolation when switching between workflows.
    
    Requirements: 9.3, 9.4
    """
    # Clear multi-file workflow state
    st.session_state.multi_file_state = None
    
    # Clear any cached Excel exports for multi-file workflow
    keys_to_clear = [key for key in st.session_state.keys() 
                     if key.startswith('excel_cache_multi_')]
    for key in keys_to_clear:
        del st.session_state[key]
    
    # Clear multi-file cleaning flags
    if 'do_multi_cleaning' in st.session_state:
        st.session_state.do_multi_cleaning = False
    if 'do_multi_dnc' in st.session_state:
        st.session_state.do_multi_dnc = False
    if 'do_multi_zip' in st.session_state:
        st.session_state.do_multi_zip = False
    if 'do_multi_phone' in st.session_state:
        st.session_state.do_multi_phone = False
    if 'do_multi_suppression' in st.session_state:
        st.session_state.do_multi_suppression = False
    if 'do_multi_dedupe' in st.session_state:
        st.session_state.do_multi_dedupe = False


def clear_all_workflow_state():
    """Clear all workflow state (both single-file and multi-file).
    
    Provides a complete reset of the application state, useful for
    the "Clear and Start Over" functionality on the home page.
    
    Requirements: 9.3
    """
    clear_single_file_state()
    clear_multi_file_state()
    st.session_state.workflow_mode = None
    st.session_state.current_step = "1. Upload Raw Data"


def has_existing_workflow_state() -> bool:
    """Check if there is any existing workflow state.
    
    Returns True if either single-file or multi-file workflow has
    data that would be lost if starting a new workflow.
    
    Returns:
        bool: True if existing workflow state exists
    """
    # Check single-file workflow state
    has_single_file_state = (
        st.session_state.raw_data is not None or
        st.session_state.step1_result is not None or
        st.session_state.step2_result is not None or
        st.session_state.step3_result is not None or
        st.session_state.step4_result is not None
    )
    
    # Check multi-file workflow state
    has_multi_file_state = (
        st.session_state.multi_file_state is not None and
        any(f.is_uploaded for f in st.session_state.multi_file_state.files)
    )
    
    return has_single_file_state or has_multi_file_state


def render_home_page():
    """Render the home page with workflow selection.
    
    Displays the company logo and provides buttons to choose between
    single-file workflow (Clean 1 File) or multi-file workflow (Clean 5 Files).
    Also provides a "Clear and Start Over" option when existing workflow state exists.
    
    Requirements: 1.1, 1.2, 1.3, 1.4, 9.3
    """
    # Center the content
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Display company logo (Requirement 1.1)
        try:
            st.image(LOGO_PATH, use_container_width=True)
        except Exception:
            # If logo file not found, show a placeholder header
            st.title("🏠 Refinance Data Cleansing")
        
        st.markdown("---")
        
        # Check for existing workflow state and offer to clear (Requirement 9.3)
        if has_existing_workflow_state():
            st.warning("⚠️ You have existing workflow data that will be preserved.")
            st.write("Choose a workflow to continue, or clear all data to start fresh.")
            
            if st.button(
                "🗑️ Clear and Start Over",
                type="secondary",
                use_container_width=True,
                help="Clear all existing workflow data and start fresh"
            ):
                clear_all_workflow_state()
                st.success("✓ All workflow data cleared!")
                st.rerun()
            
            st.markdown("---")
        
        # Welcome message
        st.markdown("### Welcome! Choose your workflow:")
        st.write("")
        
        # Navigation buttons (Requirement 1.2)
        button_col1, button_col2 = st.columns(2)
        
        with button_col1:
            # Clean 1 File button (Requirement 1.3)
            if st.button(
                "📄 Clean 1 File",
                type="primary",
                use_container_width=True,
                help="Process a single file through the cleaning pipeline"
            ):
                # Clear multi-file state when switching to single-file (Requirement 9.4)
                clear_multi_file_state()
                st.session_state.workflow_mode = "single"
                st.session_state.current_step = "1. Upload Raw Data"
                st.rerun()
        
        with button_col2:
            # Clean 5 Files button (Requirement 1.4)
            if st.button(
                "📁 Clean 5 Files",
                type="primary",
                use_container_width=True,
                help="Process 5 files through the complete multi-file workflow"
            ):
                # Clear single-file state when switching to multi-file (Requirement 9.4)
                clear_single_file_state()
                st.session_state.workflow_mode = "multi"
                st.session_state.current_step = "1. Upload 5 Files"
                # Initialize multi-file workflow state (Requirement 9.1)
                init_multi_file_workflow_state()
                st.rerun()
        
        st.write("")
        st.markdown("---")
        
        # Workflow descriptions
        with st.expander("ℹ️ About the workflows"):
            st.markdown("""
            **Clean 1 File** - Single-file workflow:
            - Upload one data file
            - Clean bad data, filter by DNC, zip codes, and phone numbers
            - Cross-file deduplication with additional files
            
            **Clean 5 Files** - Multi-file workflow:
            - Upload 5 data files at once
            - Process all files through the complete cleaning pipeline
            - Master phone list suppression
            - Cross-file deduplication across all 5 files
            - Download intermediate and final results
            """)


def go_to_step(step_name: str):
    """Navigate to a specific step."""
    st.session_state.current_step = step_name


def init_multi_file_workflow_state():
    """Initialize or get the multi-file workflow state.
    
    Creates a new MultiFileWorkflowState with 5 empty MultiFileState objects
    if one doesn't exist in session state.
    
    Returns:
        MultiFileWorkflowState: The workflow state object
        
    Requirements: 9.1
    """
    if st.session_state.multi_file_state is None:
        # Initialize with 5 empty file states (Requirement 9.1)
        files = [MultiFileState() for _ in range(5)]
        st.session_state.multi_file_state = MultiFileWorkflowState(files=files)
    return st.session_state.multi_file_state


def validate_multi_file_state_for_step(required_step: int) -> tuple[bool, str]:
    """Validate that multi-file workflow state is ready for a given step.
    
    Checks that all prerequisite steps have been completed before allowing
    access to a given step. Returns validation status and redirect step if invalid.
    
    Args:
        required_step: The step number that requires validation (2-8)
        
    Returns:
        Tuple of (is_valid, redirect_step_name):
        - is_valid: True if state is valid for the requested step
        - redirect_step_name: Step name to redirect to if invalid, or empty string if valid
        
    Requirements: 9.2, 9.5
    """
    workflow_state = st.session_state.multi_file_state
    
    # Check if workflow state exists at all
    if workflow_state is None:
        return False, "1. Upload 5 Files"
    
    # Check if all 5 files are uploaded (required for steps 2+)
    if required_step >= 2:
        uploaded_count = sum(1 for f in workflow_state.files if f.is_uploaded)
        if uploaded_count < 5:
            return False, "1. Upload 5 Files"
    
    # Check step-specific prerequisites
    if required_step >= 3:
        # Step 3 requires Step 2 (Clean Bad Data) to be complete
        cleaning_done = all(
            2 in file_state.step_results 
            for file_state in workflow_state.files 
            if file_state.is_uploaded
        )
        if not cleaning_done:
            return False, "2. Clean Bad Data"
    
    if required_step >= 4:
        # Step 4 requires Step 3 (TCPA DNC) to be complete
        dnc_done = all(
            3 in file_state.step_results 
            for file_state in workflow_state.files 
            if file_state.is_uploaded
        )
        if not dnc_done:
            return False, "3. TCPA DNC File"
    
    if required_step >= 5:
        # Step 5 requires Step 4 (Zip Code Removal) to be complete
        zip_done = all(
            4 in file_state.step_results 
            for file_state in workflow_state.files 
            if file_state.is_uploaded
        )
        if not zip_done:
            return False, "4. Zip Code Removal"
    
    if required_step >= 6:
        # Step 6 requires Step 5 (Phone Number Removal) to be complete
        phone_done = all(
            5 in file_state.step_results 
            for file_state in workflow_state.files 
            if file_state.is_uploaded
        )
        if not phone_done:
            return False, "5. Phone Number Removal"
    
    # Steps 7 and 8 require Step 5 to be complete (Step 6 is download, doesn't modify data)
    if required_step >= 7:
        phone_done = all(
            5 in file_state.step_results 
            for file_state in workflow_state.files 
            if file_state.is_uploaded
        )
        if not phone_done:
            return False, "5. Phone Number Removal"
    
    if required_step >= 8:
        # Step 8 requires Step 7 (Master Phone Suppression) to be complete
        suppression_done = all(
            7 in file_state.step_results 
            for file_state in workflow_state.files 
            if file_state.is_uploaded
        )
        if not suppression_done:
            return False, "7. Master Phone Suppression"
    
    return True, ""


def redirect_if_invalid_state(required_step: int) -> bool:
    """Check state validity and redirect if invalid.
    
    Validates the multi-file workflow state for the given step and
    automatically redirects to the appropriate step if state is missing.
    
    Args:
        required_step: The step number that requires validation
        
    Returns:
        True if state is valid and can proceed, False if redirected
        
    Requirements: 9.2, 9.5
    """
    is_valid, redirect_step = validate_multi_file_state_for_step(required_step)
    
    if not is_valid:
        st.warning(f"Please complete the previous steps first.")
        if st.button(f"← Go to {redirect_step}"):
            go_to_step(redirect_step)
            st.rerun()
        return False
    
    return True


def apply_cleaning_to_all_files(
    cleaning_func: Callable[[pd.DataFrame], CleanResult],
    file_states: List[MultiFileState],
    reason_code: str
) -> List[StepResult]:
    """Apply a cleaning function to all files in the multi-file workflow.
    
    This helper function applies a single cleaning operation to each file's
    cleaned_df, updates the file state with the results, and returns per-file
    StepResult objects for display.
    
    Args:
        cleaning_func: A function that takes a DataFrame and returns a CleanResult.
                      The function should have signature: (df: pd.DataFrame) -> CleanResult
        file_states: List of MultiFileState objects (typically 5 files)
        reason_code: A string identifier for the removal reason (e.g., 'invalid_last_name')
    
    Returns:
        List of StepResult objects, one per file, containing:
        - cleaned_df: The cleaned DataFrame after applying the cleaning function
        - all_removed_df: DataFrame of removed rows with _removal_reason column
        - before_count: Row count before cleaning
        - after_count: Row count after cleaning
        - removal_summary: Dict mapping reason_code to removed count
    
    Side Effects:
        Updates each MultiFileState in place:
        - cleaned_df is updated to the result of cleaning
        - removed_df accumulates the newly removed rows
    
    Requirements: 3.1, 3.3
    """
    step_results = []
    
    for file_state in file_states:
        # Skip files that aren't uploaded or don't have data
        if not file_state.is_uploaded or file_state.cleaned_df is None:
            # Return an empty StepResult for unprocessed files
            step_results.append(StepResult(
                cleaned_df=pd.DataFrame(),
                all_removed_df=pd.DataFrame(),
                before_count=0,
                after_count=0,
                removal_summary={}
            ))
            continue
        
        # Get the current cleaned DataFrame
        df = file_state.cleaned_df.copy()
        before_count = len(df)
        
        # Apply the cleaning function
        result = cleaning_func(df)
        
        # Update the file state's cleaned_df
        file_state.cleaned_df = result.cleaned_df
        
        # Prepare removed rows with reason code
        removed_df = pd.DataFrame()
        removal_summary = {}
        
        if result.removed_count > 0:
            # Add removal reason to removed rows
            result.removed_df['_removal_reason'] = reason_code
            removed_df = result.removed_df
            removal_summary[reason_code] = result.removed_count
            
            # Accumulate removed rows in file state
            if file_state.removed_df is None or len(file_state.removed_df) == 0:
                file_state.removed_df = removed_df.copy()
            else:
                file_state.removed_df = pd.concat(
                    [file_state.removed_df, removed_df],
                    ignore_index=True
                )
        
        # Create StepResult for this file
        step_result = StepResult(
            cleaned_df=result.cleaned_df,
            all_removed_df=removed_df,
            before_count=before_count,
            after_count=len(result.cleaned_df),
            removal_summary=removal_summary
        )
        step_results.append(step_result)
    
    return step_results


def render_multi_step1_upload():
    """Step 1: Upload 5 data files for multi-file workflow.
    
    Displays 5 file upload widgets in a grid layout. For each upload:
    - Validates that the file contains all required columns
    - Stores the DataFrame in MultiFileState
    - Displays upload status (filename, row count) for each slot
    
    The Next button is only enabled when all 5 files are uploaded and validated.
    
    Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7
    """
    st.header("Step 1: Upload 5 Files")
    
    # Initialize multi-file workflow state
    workflow_state = init_multi_file_workflow_state()
    
    # Show required columns info
    with st.expander("Required Columns", expanded=False):
        st.write("Each file must contain these columns:")
        for col in REQUIRED_COLUMNS:
            st.write(f"- {col}")
    
    st.write("Upload 5 data files to process through the cleaning pipeline.")
    st.write("Files should be ordered from **newest (File 1)** to **oldest (File 5)**.")
    
    st.divider()
    
    # Display 5 file upload widgets in a grid layout (Requirement 2.1)
    # Use 3 columns for first row, 2 columns for second row
    row1_cols = st.columns(3)
    row2_cols = st.columns([1, 1, 1])  # 3 columns, but we'll only use first 2
    
    # Map file index to column
    file_columns = [
        row1_cols[0], row1_cols[1], row1_cols[2],
        row2_cols[0], row2_cols[1]
    ]
    
    for i in range(5):
        file_num = i + 1
        file_state = workflow_state.files[i]
        
        with file_columns[i]:
            st.subheader(f"📁 File {file_num}")
            
            # Show current status if file is already uploaded (Requirement 2.3)
            if file_state.is_uploaded and file_state.cleaned_df is not None:
                st.success(f"✓ **{file_state.filename}**")
                st.write(f"Rows: **{len(file_state.cleaned_df):,}**")
                
                # Option to clear and re-upload
                if st.button(f"Clear File {file_num}", key=f"clear_file_{file_num}"):
                    workflow_state.files[i] = MultiFileState()
                    st.rerun()
            else:
                # File uploader widget (Requirement 2.1)
                uploaded_file = st.file_uploader(
                    f"Upload File {file_num}",
                    type=['xlsx', 'xls', 'csv'],
                    key=f'multi_file_{file_num}_upload',
                    label_visibility="collapsed"
                )
                
                if uploaded_file is not None:
                    try:
                        # Read the file
                        file_bytes = uploaded_file.read()
                        df = load_file_with_progress(file_bytes, uploaded_file.name, st)
                        
                        # Validate required columns (Requirement 2.2)
                        is_valid, missing_cols = validate_required_columns(df)
                        
                        if not is_valid:
                            # Display error for invalid file (Requirement 2.4)
                            st.error(f"❌ Missing columns: {', '.join(missing_cols)}")
                        else:
                            # Filter to only required columns
                            df, dropped_cols = filter_to_required_columns(df)
                            
                            # Store in MultiFileState (Requirement 2.7)
                            workflow_state.files[i].raw_df = df.copy()
                            workflow_state.files[i].cleaned_df = df.copy()
                            workflow_state.files[i].filename = uploaded_file.name
                            workflow_state.files[i].is_uploaded = True
                            
                            st.success(f"✓ Loaded {len(df):,} rows")
                            if dropped_cols:
                                st.info(f"Dropped {len(dropped_cols)} extra columns")
                            
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"Error loading file: {e}")
    
    st.divider()
    
    # Display upload summary and status
    st.subheader("📊 Upload Summary")
    
    # Count uploaded files
    uploaded_count = sum(1 for f in workflow_state.files if f.is_uploaded)
    
    # Create summary table
    summary_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        if file_state.is_uploaded and file_state.cleaned_df is not None:
            summary_data.append({
                "File": f"File {file_num}",
                "Status": "✓ Uploaded",
                "Filename": file_state.filename,
                "Rows": f"{len(file_state.cleaned_df):,}"
            })
        else:
            summary_data.append({
                "File": f"File {file_num}",
                "Status": "⏳ Pending",
                "Filename": "—",
                "Rows": "—"
            })
    
    # Display as a table
    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    # Show progress message
    if uploaded_count == 5:
        st.success(f"✓ All {uploaded_count} files uploaded! Ready to proceed.")
    else:
        st.info(f"📁 {uploaded_count}/5 files uploaded. Please upload all 5 files to continue.")
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Home", use_container_width=True):
            st.session_state.workflow_mode = None
            st.rerun()
    
    with col2:
        # Next button - only enabled when all 5 files are uploaded (Requirements 2.5, 2.6)
        next_disabled = uploaded_count < 5
        
        if st.button(
            "Next → Step 2: Clean Bad Data",
            type="primary",
            use_container_width=True,
            disabled=next_disabled
        ):
            # Set up column mapping for multi-file workflow
            mapping = workflow_state.column_mapping
            mapping.phone = 'Phone1'
            mapping.first_name = 'FirstName'
            mapping.last_name = 'LastName'
            mapping.email = 'Email'
            mapping.zip_code = 'ZipCode'
            mapping.lead_id = 'Universal_LeadId'
            workflow_state.column_mapping = mapping
            
            go_to_step("2. Clean Bad Data")
            st.rerun()


def render_multi_step2_clean():
    """Step 2: Clean Bad Data for all 5 files in multi-file workflow.
    
    Applies the same cleaning operations as the single-file workflow to all 5 files:
    - Filter invalid last names
    - Filter empty phones
    - Filter invalid phones
    - Filter invalid emails
    - Filter TEST entries
    - Filter placeholder emails
    - Filter fake/suspicious emails
    - Filter prohibited content
    - Filter invalid UUIDs
    
    Displays per-file removal stats in a summary table and stores results
    in MultiFileWorkflowState.
    
    Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
    """
    st.header("Step 2: Clean Bad Data (Multi-File)")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if all files are uploaded
    uploaded_count = sum(1 for f in workflow_state.files if f.is_uploaded)
    if uploaded_count < 5:
        st.warning(f"Only {uploaded_count}/5 files uploaded. Please complete Step 1 first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    mapping = workflow_state.column_mapping
    
    # Display current file status
    st.subheader("📁 Files to Clean")
    
    # Build summary of files before cleaning
    before_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        row_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        before_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Rows": f"{row_count:,}"
        })
    
    before_df = pd.DataFrame(before_data)
    st.dataframe(before_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Check if cleaning has already been done (check step_results for step 2)
    cleaning_done = all(
        2 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    # Check if we need to do cleaning
    if st.session_state.get('do_multi_cleaning', False):
        st.session_state.do_multi_cleaning = False
        
        # Define cleaning steps for progress display
        cleaning_steps = [
            "Filter invalid last names",
            "Filter empty phones",
            "Filter invalid phones",
            "Filter invalid emails",
            "Filter TEST entries",
            "Filter placeholder emails",
            "Filter fake/suspicious emails",
            "Filter prohibited content",
            "Filter invalid UUIDs"
        ]
        
        # Full-width progress display
        st.subheader("🧹 Cleaning all 5 files...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track removal stats per file
        file_removal_summaries = [{} for _ in range(5)]
        file_before_counts = []
        file_after_counts = []
        
        # Store before counts
        for file_state in workflow_state.files:
            file_before_counts.append(len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0)
        
        def update_progress(step_idx, step_name):
            progress_bar.progress((step_idx + 1) / len(cleaning_steps))
            with status_placeholder.container():
                for i, s in enumerate(cleaning_steps):
                    if i < step_idx:
                        st.write(f"✅ {s}")
                    elif i == step_idx:
                        st.write(f"⏳ {s}...")
                    else:
                        st.write(f"⬜ {s}")
            time.sleep(0.05)
        
        # Show initial state
        with status_placeholder.container():
            st.write(f"⏳ {cleaning_steps[0]}...")
            for s in cleaning_steps[1:]:
                st.write(f"⬜ {s}")
        
        # 1. Filter invalid last names (Requirement 3.4 - reuse existing logic)
        update_progress(0, cleaning_steps[0])
        results = apply_cleaning_to_all_files(
            lambda df: filter_invalid_last_names(df, mapping.last_name),
            workflow_state.files,
            'invalid_last_name'
        )
        for i, result in enumerate(results):
            if result.removal_summary:
                file_removal_summaries[i]['Invalid last name'] = result.removal_summary.get('invalid_last_name', 0)
        
        # 2. Filter empty phones
        update_progress(1, cleaning_steps[1])
        results = apply_cleaning_to_all_files(
            lambda df: filter_empty_phones(df, mapping.phone),
            workflow_state.files,
            'empty_phone'
        )
        for i, result in enumerate(results):
            if result.removal_summary:
                file_removal_summaries[i]['Empty phone'] = result.removal_summary.get('empty_phone', 0)
        
        # 3. Filter invalid phones
        update_progress(2, cleaning_steps[2])
        results = apply_cleaning_to_all_files(
            lambda df: filter_invalid_phones(df, mapping.phone),
            workflow_state.files,
            'invalid_phone'
        )
        for i, result in enumerate(results):
            if result.removal_summary:
                file_removal_summaries[i]['Invalid phone'] = result.removal_summary.get('invalid_phone', 0)
        
        # 4. Filter invalid emails
        update_progress(3, cleaning_steps[3])
        results = apply_cleaning_to_all_files(
            lambda df: filter_invalid_emails(df, mapping.email),
            workflow_state.files,
            'invalid_email'
        )
        for i, result in enumerate(results):
            if result.removal_summary:
                file_removal_summaries[i]['Invalid email'] = result.removal_summary.get('invalid_email', 0)
        
        # 5. Filter TEST entries
        update_progress(4, cleaning_steps[4])
        results = apply_cleaning_to_all_files(
            lambda df: filter_test_entries(df, mapping.first_name, mapping.last_name),
            workflow_state.files,
            'contains_test'
        )
        for i, result in enumerate(results):
            if result.removal_summary:
                file_removal_summaries[i]['Contains TEST'] = result.removal_summary.get('contains_test', 0)
        
        # 6. Filter placeholder emails
        update_progress(5, cleaning_steps[5])
        if mapping.email:
            results = apply_cleaning_to_all_files(
                lambda df: filter_placeholder_emails(df, mapping.email),
                workflow_state.files,
                'placeholder_email'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['Placeholder email'] = result.removal_summary.get('placeholder_email', 0)
        
        # 7. Filter fake/suspicious emails
        update_progress(6, cleaning_steps[6])
        if mapping.email:
            results = apply_cleaning_to_all_files(
                lambda df: filter_fake_emails(df, mapping.email),
                workflow_state.files,
                'fake_email'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['Fake email'] = result.removal_summary.get('fake_email', 0)
        
        # 8. Filter prohibited content
        update_progress(7, cleaning_steps[7])
        results = apply_cleaning_to_all_files(
            lambda df: filter_prohibited_content(df),
            workflow_state.files,
            'prohibited_content'
        )
        for i, result in enumerate(results):
            if result.removal_summary:
                file_removal_summaries[i]['Prohibited content'] = result.removal_summary.get('prohibited_content', 0)
        
        # 9. Filter invalid UUIDs
        update_progress(8, cleaning_steps[8])
        if mapping.lead_id:
            results = apply_cleaning_to_all_files(
                lambda df: filter_invalid_uuid(df, mapping.lead_id),
                workflow_state.files,
                'invalid_uuid'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['Invalid UUID'] = result.removal_summary.get('invalid_uuid', 0)
        
        # Store after counts and create StepResults for each file
        for i, file_state in enumerate(workflow_state.files):
            after_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
            file_after_counts.append(after_count)
            
            # Create StepResult for this file (Requirement 3.3)
            file_state.step_results[2] = StepResult(
                cleaned_df=file_state.cleaned_df,
                all_removed_df=file_state.removed_df if file_state.removed_df is not None else pd.DataFrame(),
                before_count=file_before_counts[i],
                after_count=after_count,
                removal_summary=file_removal_summaries[i]
            )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        st.rerun()
    
    # Show Apply Cleaning button if not done yet
    if not cleaning_done:
        if st.button("🧹 Apply Cleaning to All Files", type="primary"):
            st.session_state.do_multi_cleaning = True
            st.rerun()
    
    # Display results if cleaning has been done (Requirement 3.2, 3.5)
    if cleaning_done:
        st.success("✅ Cleaning complete for all 5 files!")
        
        st.divider()
        st.subheader("📊 Cleaning Results Summary")
        
        # Build summary table with before/after counts for all 5 files (Requirement 3.5)
        summary_data = []
        total_before = 0
        total_after = 0
        total_removed = 0
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(2)
            
            if step_result:
                before = step_result.before_count
                after = step_result.after_count
                removed = before - after
                
                total_before += before
                total_after += after
                total_removed += removed
                
                summary_data.append({
                    "File": f"File {file_num}",
                    "Filename": file_state.filename or "—",
                    "Before": f"{before:,}",
                    "Removed": f"{removed:,}",
                    "After": f"{after:,}"
                })
        
        # Add totals row
        summary_data.append({
            "File": "**TOTAL**",
            "Filename": "—",
            "Before": f"**{total_before:,}**",
            "Removed": f"**{total_removed:,}**",
            "After": f"**{total_after:,}**"
        })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show per-file removal details in expanders (Requirement 3.2)
        st.divider()
        st.subheader("📋 Per-File Removal Details")
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(2)
            
            if step_result and step_result.removal_summary:
                with st.expander(f"File {file_num}: {file_state.filename or 'Unknown'}"):
                    # Display removal summary for this file
                    for reason, count in step_result.removal_summary.items():
                        if count > 0:
                            st.write(f"- {reason}: {count} rows")
                    
                    # Show total for this file
                    removed = step_result.before_count - step_result.after_count
                    st.write(f"**Total removed: {removed:,} rows**")
                    
                    # Preview cleaned data
                    if step_result.cleaned_df is not None and len(step_result.cleaned_df) > 0:
                        st.write("**Cleaned Data Preview:**")
                        st.dataframe(step_result.cleaned_df.head(10))
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 1", use_container_width=True):
            go_to_step("1. Upload 5 Files")
            st.rerun()
    
    with col2:
        # Next button - only enabled when cleaning is done
        if st.button(
            "Next → Step 3: TCPA DNC File",
            type="primary",
            use_container_width=True,
            disabled=not cleaning_done
        ):
            go_to_step("3. TCPA DNC File")
            st.rerun()


def render_multi_step3_dnc():
    """Step 3: TCPA DNC File filtering for all 5 files in multi-file workflow.
    
    Uploads the TCPA LD DNC file and filters all 5 files against:
    - Phone numbers matching the DNC list
    - Phone numbers with blocked area codes
    - Names matching the DNC list (FirstName + LastName)
    
    Reuses existing DNC logic from render_step3_dnc() and applies to all 5 files.
    Displays per-file removal stats in a summary table.
    
    Requirements: 3.1, 3.2, 3.3, 3.4
    """
    st.header("Step 3: TCPA DNC File (Multi-File)")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 2 cleaning has been done
    cleaning_done = all(
        2 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not cleaning_done:
        st.warning("Please complete Step 2 (Clean Bad Data) first.")
        if st.button("← Go to Step 2"):
            go_to_step("2. Clean Bad Data")
            st.rerun()
        return
    
    mapping = workflow_state.column_mapping
    
    st.write("Upload the TCPA LD DNC file. This will filter out from **all 5 files**:")
    st.write("- Phone numbers matching the DNC list")
    st.write("- Phone numbers with blocked area codes")
    st.write("- Names matching the DNC list (FirstName + LastName)")
    
    # File uploader for DNC file
    dnc_file = st.file_uploader(
        "Upload TCPA LD DNC File (Excel)",
        type=['xlsx', 'xls'],
        key='multi_dnc_upload'
    )
    
    # Process file if newly uploaded (not already loaded in workflow state)
    if dnc_file and workflow_state.tcpa_dnc_data is None:
        try:
            file_bytes = dnc_file.read()
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.write(f"📂 Reading DNC file ({file_size_mb:.1f} MB)...")
            progress_bar.progress(20)
            
            # Read specifically from Sheet1 (2)
            xl = pd.ExcelFile(BytesIO(file_bytes))
            sheet_name = 'Sheet1 (2)' if 'Sheet1 (2)' in xl.sheet_names else xl.sheet_names[-1]
            
            status_text.write(f"⏳ Parsing sheet '{sheet_name}'...")
            progress_bar.progress(50)
            
            df = pd.read_excel(xl, sheet_name=sheet_name)
            
            progress_bar.progress(80)
            status_text.write("📊 Processing DNC data...")
            
            # Store in workflow state
            workflow_state.tcpa_dnc_data = df
            
            progress_bar.progress(100)
            time.sleep(0.3)
            progress_bar.empty()
            status_text.empty()
            
            # Preview what was loaded
            dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(df)
            st.success(f"✓ Loaded DNC file from sheet '{sheet_name}'")
            st.write(f"- {len(dnc_phones):,} phone numbers")
            st.write(f"- {len(dnc_area_codes)} area codes: {', '.join(sorted(dnc_area_codes))}")
            st.write(f"- {len(dnc_names):,} names")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show loaded status if already loaded
    elif workflow_state.tcpa_dnc_data is not None:
        dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(workflow_state.tcpa_dnc_data)
        st.success(f"✓ DNC file loaded")
        st.write(f"- {len(dnc_phones):,} phone numbers")
        st.write(f"- {len(dnc_area_codes)} area codes")
        st.write(f"- {len(dnc_names):,} names")
    
    st.divider()
    
    # Display current file status
    st.subheader("📁 Files to Filter")
    
    # Build summary of files before DNC filtering
    before_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        row_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        before_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Current Rows": f"{row_count:,}"
        })
    
    before_df = pd.DataFrame(before_data)
    st.dataframe(before_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Check if DNC filtering has already been done (check step_results for step 3)
    dnc_done = all(
        3 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    # Check if we need to do DNC filtering
    if st.session_state.get('do_multi_dnc', False):
        st.session_state.do_multi_dnc = False
        
        # Get DNC data
        dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(workflow_state.tcpa_dnc_data)
        
        # Define filtering steps for progress display
        filtering_steps = [
            "Filter by DNC phone numbers",
            "Filter by blocked area codes",
            "Filter by DNC names"
        ]
        
        # Full-width progress display
        st.subheader("🔍 DNC Filtering all 5 files...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track removal stats per file
        file_removal_summaries = [{} for _ in range(5)]
        file_before_counts = []
        file_after_counts = []
        
        # Store before counts
        for file_state in workflow_state.files:
            file_before_counts.append(len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0)
        
        def update_progress(step_idx, step_name):
            progress_bar.progress((step_idx + 1) / len(filtering_steps))
            with status_placeholder.container():
                for i, s in enumerate(filtering_steps):
                    if i < step_idx:
                        st.write(f"✅ {s}")
                    elif i == step_idx:
                        st.write(f"⏳ {s}...")
                    else:
                        st.write(f"⬜ {s}")
            time.sleep(0.05)
        
        # Show initial state
        with status_placeholder.container():
            st.write(f"⏳ {filtering_steps[0]}...")
            for s in filtering_steps[1:]:
                st.write(f"⬜ {s}")
        
        # 1. Filter by DNC phone numbers (Requirement 3.4 - reuse existing logic)
        update_progress(0, filtering_steps[0])
        if mapping.phone and dnc_phones:
            results = apply_cleaning_to_all_files(
                lambda df: filter_by_dnc_phones(df, mapping.phone, dnc_phones),
                workflow_state.files,
                'dnc_phone_match'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['DNC phone match'] = result.removal_summary.get('dnc_phone_match', 0)
        
        # 2. Filter by blocked area codes
        update_progress(1, filtering_steps[1])
        if mapping.phone and dnc_area_codes:
            results = apply_cleaning_to_all_files(
                lambda df: filter_by_area_code(df, mapping.phone, dnc_area_codes),
                workflow_state.files,
                'dnc_area_code'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['DNC area code'] = result.removal_summary.get('dnc_area_code', 0)
        
        # 3. Filter by DNC names
        update_progress(2, filtering_steps[2])
        if mapping.first_name and mapping.last_name and dnc_names:
            results = apply_cleaning_to_all_files(
                lambda df: filter_by_name_match(df, mapping.first_name, mapping.last_name, dnc_names),
                workflow_state.files,
                'dnc_name_match'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['DNC name match'] = result.removal_summary.get('dnc_name_match', 0)
        
        # Store after counts and create StepResults for each file
        for i, file_state in enumerate(workflow_state.files):
            after_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
            file_after_counts.append(after_count)
            
            # Create StepResult for this file (Requirement 3.3)
            file_state.step_results[3] = StepResult(
                cleaned_df=file_state.cleaned_df,
                all_removed_df=file_state.removed_df if file_state.removed_df is not None else pd.DataFrame(),
                before_count=file_before_counts[i],
                after_count=after_count,
                removal_summary=file_removal_summaries[i]
            )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        st.rerun()
    
    # Show Run DNC button if DNC file is loaded and filtering not done yet
    if workflow_state.tcpa_dnc_data is not None and not dnc_done:
        if st.button("🔍 Run DNC Filter on All Files", type="primary"):
            st.session_state.do_multi_dnc = True
            st.rerun()
    
    # Display results if DNC filtering has been done (Requirement 3.2)
    if dnc_done:
        st.success("✅ DNC filtering complete for all 5 files!")
        
        st.divider()
        st.subheader("📊 DNC Filtering Results Summary")
        
        # Build summary table with before/after counts for all 5 files
        summary_data = []
        total_before = 0
        total_after = 0
        total_removed = 0
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(3)
            
            if step_result:
                before = step_result.before_count
                after = step_result.after_count
                removed = before - after
                
                total_before += before
                total_after += after
                total_removed += removed
                
                summary_data.append({
                    "File": f"File {file_num}",
                    "Filename": file_state.filename or "—",
                    "Before": f"{before:,}",
                    "Removed": f"{removed:,}",
                    "After": f"{after:,}"
                })
        
        # Add totals row
        summary_data.append({
            "File": "**TOTAL**",
            "Filename": "—",
            "Before": f"**{total_before:,}**",
            "Removed": f"**{total_removed:,}**",
            "After": f"**{total_after:,}**"
        })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show per-file removal details in expanders (Requirement 3.2)
        st.divider()
        st.subheader("📋 Per-File Removal Details")
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(3)
            
            if step_result and step_result.removal_summary:
                with st.expander(f"File {file_num}: {file_state.filename or 'Unknown'}"):
                    # Display removal summary for this file
                    for reason, count in step_result.removal_summary.items():
                        if count > 0:
                            st.write(f"- {reason}: {count} rows")
                    
                    # Show total for this file
                    removed = step_result.before_count - step_result.after_count
                    st.write(f"**Total removed: {removed:,} rows**")
                    
                    # Preview cleaned data
                    if step_result.cleaned_df is not None and len(step_result.cleaned_df) > 0:
                        st.write("**Cleaned Data Preview:**")
                        st.dataframe(step_result.cleaned_df.head(10))
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 2", use_container_width=True):
            go_to_step("2. Clean Bad Data")
            st.rerun()
    
    with col2:
        # Next button - only enabled when DNC filtering is done
        if st.button(
            "Next → Step 4: Zip Code Removal",
            type="primary",
            use_container_width=True,
            disabled=not dnc_done
        ):
            go_to_step("4. Zip Code Removal")
            st.rerun()


def render_multi_step4_zipcode():
    """Step 4: Zip Code Removal for all 5 files in multi-file workflow.
    
    Uploads the TCPA Zip Codes file and filters all 5 files against:
    - Zip codes matching the TCPA zip code list
    
    Reuses existing zip code logic from render_step4_zipcode() and applies to all 5 files.
    Displays per-file removal stats in a summary table.
    
    Requirements: 3.1, 3.2, 3.3, 3.4
    """
    st.header("Step 4: Zip Code Removal (Multi-File)")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 3 DNC filtering has been done
    dnc_done = all(
        3 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not dnc_done:
        st.warning("Please complete Step 3 (TCPA DNC File) first.")
        if st.button("← Go to Step 3"):
            go_to_step("3. TCPA DNC File")
            st.rerun()
        return
    
    mapping = workflow_state.column_mapping
    
    st.write("Upload the TCPA Zip Codes file. This will filter out from **all 5 files**:")
    st.write("- Rows with zip codes matching the TCPA zip code list")
    
    # File uploader for Zip Codes file
    zips_file = st.file_uploader(
        "Upload TCPA Zip Codes File (Excel)",
        type=['xlsx', 'xls'],
        key='multi_zips_upload'
    )
    
    # Process file if newly uploaded (not already loaded in workflow state)
    if zips_file and workflow_state.tcpa_zips_data is None:
        try:
            file_bytes = zips_file.read()
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.write(f"📂 Reading Zip Codes file ({file_size_mb:.1f} MB)...")
            progress_bar.progress(20)
            
            status_text.write("⏳ Parsing zip codes...")
            progress_bar.progress(50)
            
            df = load_file_with_progress(file_bytes, zips_file.name)
            
            progress_bar.progress(80)
            status_text.write("📊 Processing zip code data...")
            
            # Store in workflow state
            workflow_state.tcpa_zips_data = df
            
            progress_bar.progress(100)
            time.sleep(0.3)
            progress_bar.empty()
            status_text.empty()
            
            # Preview what was loaded
            tcpa_zips = load_tcpa_zipcodes(df)
            st.success(f"✓ Loaded {len(tcpa_zips):,} zip codes")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show loaded status if already loaded
    elif workflow_state.tcpa_zips_data is not None:
        tcpa_zips = load_tcpa_zipcodes(workflow_state.tcpa_zips_data)
        st.success(f"✓ {len(tcpa_zips):,} zip codes loaded")
    
    st.divider()
    
    # Display current file status
    st.subheader("📁 Files to Filter")
    
    # Build summary of files before zip code filtering
    before_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        row_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        before_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Current Rows": f"{row_count:,}"
        })
    
    before_df = pd.DataFrame(before_data)
    st.dataframe(before_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Check if zip code filtering has already been done (check step_results for step 4)
    zip_done = all(
        4 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    # Check if we need to do zip code filtering
    if st.session_state.get('do_multi_zip', False):
        st.session_state.do_multi_zip = False
        
        # Get zip code data
        tcpa_zips = load_tcpa_zipcodes(workflow_state.tcpa_zips_data)
        
        # Define filtering steps for progress display
        filtering_steps = [
            "Filter by TCPA zip codes"
        ]
        
        # Full-width progress display
        st.subheader("🔍 Zip Code Filtering all 5 files...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track removal stats per file
        file_removal_summaries = [{} for _ in range(5)]
        file_before_counts = []
        file_after_counts = []
        
        # Store before counts
        for file_state in workflow_state.files:
            file_before_counts.append(len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0)
        
        def update_progress(step_idx, step_name):
            progress_bar.progress((step_idx + 1) / len(filtering_steps))
            with status_placeholder.container():
                for i, s in enumerate(filtering_steps):
                    if i < step_idx:
                        st.write(f"✅ {s}")
                    elif i == step_idx:
                        st.write(f"⏳ {s}...")
                    else:
                        st.write(f"⬜ {s}")
            time.sleep(0.05)
        
        # Show initial state
        with status_placeholder.container():
            st.write(f"⏳ {filtering_steps[0]}...")
        
        # 1. Filter by TCPA zip codes (Requirement 3.4 - reuse existing logic)
        update_progress(0, filtering_steps[0])
        if mapping.zip_code and tcpa_zips:
            results = apply_cleaning_to_all_files(
                lambda df: filter_by_tcpa_zips(df, mapping.zip_code, tcpa_zips),
                workflow_state.files,
                'tcpa_zip_match'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['Zip code match'] = result.removal_summary.get('tcpa_zip_match', 0)
        
        # Store after counts and create StepResults for each file
        for i, file_state in enumerate(workflow_state.files):
            after_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
            file_after_counts.append(after_count)
            
            # Create StepResult for this file (Requirement 3.3)
            file_state.step_results[4] = StepResult(
                cleaned_df=file_state.cleaned_df,
                all_removed_df=file_state.removed_df if file_state.removed_df is not None else pd.DataFrame(),
                before_count=file_before_counts[i],
                after_count=after_count,
                removal_summary=file_removal_summaries[i]
            )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        st.rerun()
    
    # Show Run Zip Filter button if zip file is loaded and filtering not done yet
    if workflow_state.tcpa_zips_data is not None and not zip_done:
        if st.button("🔍 Run Zip Code Filter on All Files", type="primary"):
            st.session_state.do_multi_zip = True
            st.rerun()
    
    # Display results if zip code filtering has been done (Requirement 3.2)
    if zip_done:
        st.success("✅ Zip code filtering complete for all 5 files!")
        
        st.divider()
        st.subheader("📊 Zip Code Filtering Results Summary")
        
        # Build summary table with before/after counts for all 5 files
        summary_data = []
        total_before = 0
        total_after = 0
        total_removed = 0
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(4)
            
            if step_result:
                before = step_result.before_count
                after = step_result.after_count
                removed = before - after
                
                total_before += before
                total_after += after
                total_removed += removed
                
                summary_data.append({
                    "File": f"File {file_num}",
                    "Filename": file_state.filename or "—",
                    "Before": f"{before:,}",
                    "Removed": f"{removed:,}",
                    "After": f"{after:,}"
                })
        
        # Add totals row
        summary_data.append({
            "File": "**TOTAL**",
            "Filename": "—",
            "Before": f"**{total_before:,}**",
            "Removed": f"**{total_removed:,}**",
            "After": f"**{total_after:,}**"
        })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show per-file removal details in expanders (Requirement 3.2)
        st.divider()
        st.subheader("📋 Per-File Removal Details")
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(4)
            
            if step_result and step_result.removal_summary:
                with st.expander(f"File {file_num}: {file_state.filename or 'Unknown'}"):
                    # Display removal summary for this file
                    for reason, count in step_result.removal_summary.items():
                        if count > 0:
                            st.write(f"- {reason}: {count} rows")
                    
                    # Show total for this file
                    removed = step_result.before_count - step_result.after_count
                    st.write(f"**Total removed: {removed:,} rows**")
                    
                    # Preview cleaned data
                    if step_result.cleaned_df is not None and len(step_result.cleaned_df) > 0:
                        st.write("**Cleaned Data Preview:**")
                        st.dataframe(step_result.cleaned_df.head(10))
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 3", use_container_width=True):
            go_to_step("3. TCPA DNC File")
            st.rerun()
    
    with col2:
        # Next button - only enabled when zip code filtering is done
        if st.button(
            "Next → Step 5: Phone Number Removal",
            type="primary",
            use_container_width=True,
            disabled=not zip_done
        ):
            go_to_step("5. Phone Number Removal")
            st.rerun()


def render_multi_step5_phones():
    """Step 5: Phone Number Removal for all 5 files in multi-file workflow.
    
    Uploads the TCPA Phones file and filters all 5 files against:
    - Phone numbers matching the TCPA phone list
    - Duplicate phone numbers within each file
    
    Reuses existing phone logic from render_step5_phones() and applies to all 5 files.
    Displays per-file removal stats in a summary table.
    
    Requirements: 3.1, 3.2, 3.3, 3.4
    """
    st.header("Step 5: Phone Number Removal (Multi-File)")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 4 zip code filtering has been done
    zip_done = all(
        4 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not zip_done:
        st.warning("Please complete Step 4 (Zip Code Removal) first.")
        if st.button("← Go to Step 4"):
            go_to_step("4. Zip Code Removal")
            st.rerun()
        return
    
    mapping = workflow_state.column_mapping
    
    st.write("Upload the TCPA Phones file. This will filter out from **all 5 files**:")
    st.write("- Rows with phone numbers matching the TCPA phone list")
    st.write("- Duplicate phone numbers within each file")
    
    # File uploader for Phones file
    phones_file = st.file_uploader(
        "Upload TCPA Phones File (Excel/CSV)",
        type=['xlsx', 'xls', 'csv'],
        key='multi_phones_upload'
    )
    
    # Process file if newly uploaded (not already loaded in workflow state)
    if phones_file and workflow_state.tcpa_phones_data is None:
        try:
            file_bytes = phones_file.read()
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.write(f"📂 Reading Phones file ({file_size_mb:.1f} MB)...")
            progress_bar.progress(20)
            
            status_text.write("⏳ Parsing phone numbers...")
            progress_bar.progress(50)
            
            df = load_file_with_progress(file_bytes, phones_file.name)
            
            progress_bar.progress(80)
            status_text.write("📊 Processing phone data...")
            
            # Store in workflow state
            workflow_state.tcpa_phones_data = df
            
            progress_bar.progress(100)
            time.sleep(0.3)
            progress_bar.empty()
            status_text.empty()
            
            # Preview what was loaded
            tcpa_phones = load_tcpa_phones(df)
            st.success(f"✓ Loaded {len(tcpa_phones):,} phone numbers")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show loaded status if already loaded
    elif workflow_state.tcpa_phones_data is not None:
        tcpa_phones = load_tcpa_phones(workflow_state.tcpa_phones_data)
        st.success(f"✓ {len(tcpa_phones):,} phone numbers loaded")
    
    st.divider()
    
    # Display current file status
    st.subheader("📁 Files to Filter")
    
    # Build summary of files before phone filtering
    before_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        row_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        before_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Current Rows": f"{row_count:,}"
        })
    
    before_df = pd.DataFrame(before_data)
    st.dataframe(before_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Check if phone filtering has already been done (check step_results for step 5)
    phone_done = all(
        5 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    # Check if we need to do phone filtering
    if st.session_state.get('do_multi_phone', False):
        st.session_state.do_multi_phone = False
        
        # Get phone data
        tcpa_phones = load_tcpa_phones(workflow_state.tcpa_phones_data)
        
        # Define filtering steps for progress display
        filtering_steps = [
            "Filter by TCPA phone numbers",
            "Remove duplicate phone numbers"
        ]
        
        # Full-width progress display
        st.subheader("🔍 Phone Filtering all 5 files...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track removal stats per file
        file_removal_summaries = [{} for _ in range(5)]
        file_before_counts = []
        file_after_counts = []
        
        # Store before counts
        for file_state in workflow_state.files:
            file_before_counts.append(len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0)
        
        def update_progress(step_idx, step_name):
            progress_bar.progress((step_idx + 1) / len(filtering_steps))
            with status_placeholder.container():
                for i, s in enumerate(filtering_steps):
                    if i < step_idx:
                        st.write(f"✅ {s}")
                    elif i == step_idx:
                        st.write(f"⏳ {s}...")
                    else:
                        st.write(f"⬜ {s}")
            time.sleep(0.05)
        
        # Show initial state
        with status_placeholder.container():
            st.write(f"⏳ {filtering_steps[0]}...")
            for s in filtering_steps[1:]:
                st.write(f"⬜ {s}")
        
        # 1. Filter by TCPA phone numbers (Requirement 3.4 - reuse existing logic)
        update_progress(0, filtering_steps[0])
        if mapping.phone and tcpa_phones:
            results = apply_cleaning_to_all_files(
                lambda df: filter_by_tcpa_phones(df, mapping.phone, tcpa_phones),
                workflow_state.files,
                'tcpa_phone_match'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['TCPA phone match'] = result.removal_summary.get('tcpa_phone_match', 0)
        
        # 2. Remove duplicate phone numbers (Requirement 3.4 - reuse existing logic)
        update_progress(1, filtering_steps[1])
        if mapping.phone:
            results = apply_cleaning_to_all_files(
                lambda df: remove_duplicate_phones(df, mapping.phone),
                workflow_state.files,
                'duplicate_phone'
            )
            for i, result in enumerate(results):
                if result.removal_summary:
                    file_removal_summaries[i]['Duplicate phone'] = result.removal_summary.get('duplicate_phone', 0)
        
        # Store after counts and create StepResults for each file
        for i, file_state in enumerate(workflow_state.files):
            after_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
            file_after_counts.append(after_count)
            
            # Create StepResult for this file (Requirement 3.3)
            file_state.step_results[5] = StepResult(
                cleaned_df=file_state.cleaned_df,
                all_removed_df=file_state.removed_df if file_state.removed_df is not None else pd.DataFrame(),
                before_count=file_before_counts[i],
                after_count=after_count,
                removal_summary=file_removal_summaries[i]
            )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        st.rerun()
    
    # Show Run Phone Filter button if phone file is loaded and filtering not done yet
    if workflow_state.tcpa_phones_data is not None and not phone_done:
        if st.button("🔍 Run Phone Filter on All Files", type="primary"):
            st.session_state.do_multi_phone = True
            st.rerun()
    
    # Display results if phone filtering has been done (Requirement 3.2)
    if phone_done:
        st.success("✅ Phone filtering complete for all 5 files!")
        
        st.divider()
        st.subheader("📊 Phone Filtering Results Summary")
        
        # Build summary table with before/after counts for all 5 files
        summary_data = []
        total_before = 0
        total_after = 0
        total_removed = 0
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(5)
            
            if step_result:
                before = step_result.before_count
                after = step_result.after_count
                removed = before - after
                
                total_before += before
                total_after += after
                total_removed += removed
                
                summary_data.append({
                    "File": f"File {file_num}",
                    "Filename": file_state.filename or "—",
                    "Before": f"{before:,}",
                    "Removed": f"{removed:,}",
                    "After": f"{after:,}"
                })
        
        # Add totals row
        summary_data.append({
            "File": "**TOTAL**",
            "Filename": "—",
            "Before": f"**{total_before:,}**",
            "Removed": f"**{total_removed:,}**",
            "After": f"**{total_after:,}**"
        })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show per-file removal details in expanders (Requirement 3.2)
        st.divider()
        st.subheader("📋 Per-File Removal Details")
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(5)
            
            if step_result and step_result.removal_summary:
                with st.expander(f"File {file_num}: {file_state.filename or 'Unknown'}"):
                    # Display removal summary for this file
                    for reason, count in step_result.removal_summary.items():
                        if count > 0:
                            st.write(f"- {reason}: {count} rows")
                    
                    # Show total for this file
                    removed = step_result.before_count - step_result.after_count
                    st.write(f"**Total removed: {removed:,} rows**")
                    
                    # Preview cleaned data
                    if step_result.cleaned_df is not None and len(step_result.cleaned_df) > 0:
                        st.write("**Cleaned Data Preview:**")
                        st.dataframe(step_result.cleaned_df.head(10))
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 4", use_container_width=True):
            go_to_step("4. Zip Code Removal")
            st.rerun()
    
    with col2:
        # Next button - only enabled when phone filtering is done
        if st.button(
            "Next → Step 6: Download Cleaned Files",
            type="primary",
            use_container_width=True,
            disabled=not phone_done
        ):
            go_to_step("6. Download Cleaned Files")
            st.rerun()


def render_multi_step6_download():
    """Step 6: Download cleaned files and removed rows for multi-file workflow.
    
    Provides download buttons for:
    - Each of the 5 cleaned files (file1_cleaned.xlsx through file5_cleaned.xlsx)
    - Each of the 5 removed-rows files (file1_removed.xlsx through file5_removed.xlsx)
    - A ZIP archive containing all 10 files
    
    Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
    """
    st.header("Step 6: Download Cleaned Files")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 5 phone filtering has been done
    phone_done = all(
        5 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not phone_done:
        st.warning("Please complete Step 5 (Phone Number Removal) first.")
        if st.button("← Go to Step 5"):
            go_to_step("5. Phone Number Removal")
            st.rerun()
        return
    
    st.write("Download your cleaned files and removed rows from the initial cleaning steps.")
    st.write("You can download individual files or all files as a ZIP archive.")
    
    st.divider()
    
    # Display summary of files
    st.subheader("📊 File Summary")
    
    summary_data = []
    total_cleaned_rows = 0
    total_removed_rows = 0
    
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        cleaned_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        removed_count = len(file_state.removed_df) if file_state.removed_df is not None else 0
        
        total_cleaned_rows += cleaned_count
        total_removed_rows += removed_count
        
        summary_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Cleaned Rows": f"{cleaned_count:,}",
            "Removed Rows": f"{removed_count:,}"
        })
    
    # Add totals row
    summary_data.append({
        "File": "**TOTAL**",
        "Filename": "—",
        "Cleaned Rows": f"**{total_cleaned_rows:,}**",
        "Removed Rows": f"**{total_removed_rows:,}**"
    })
    
    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Download All as ZIP section (Requirement 4.4, 4.5)
    st.subheader("📦 Download All Files as ZIP")
    
    # Build the files dictionary for ZIP export
    zip_files = {}
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        
        # Add cleaned file (Requirement 4.5 - descriptive naming)
        if file_state.cleaned_df is not None and len(file_state.cleaned_df) > 0:
            zip_files[f"file{file_num}_cleaned.xlsx"] = file_state.cleaned_df
        
        # Add removed file (Requirement 4.5 - descriptive naming)
        if file_state.removed_df is not None and len(file_state.removed_df) > 0:
            zip_files[f"file{file_num}_removed.xlsx"] = file_state.removed_df
    
    # Cache key for ZIP file
    cache_key_zip = "excel_cache_multi_step6_zip"
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if len(zip_files) > 0:
            # Only generate ZIP if not cached
            if cache_key_zip not in st.session_state:
                with st.spinner("Preparing ZIP archive..."):
                    st.session_state[cache_key_zip] = export_to_zip(zip_files)
            
            st.download_button(
                label="📥 Download All as ZIP",
                data=st.session_state[cache_key_zip],
                file_name="cleaned_files.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True,
                key="download_all_zip"
            )
            st.caption(f"Contains {len(zip_files)} files ({len([f for f in zip_files if 'cleaned' in f])} cleaned + {len([f for f in zip_files if 'removed' in f])} removed)")
        else:
            st.warning("No files available for download.")
    
    st.divider()
    
    # Individual file downloads section (Requirements 4.1, 4.2, 4.3)
    st.subheader("📄 Download Individual Files")
    
    # Create tabs for Cleaned Files and Removed Files
    tab_cleaned, tab_removed = st.tabs(["🧹 Cleaned Files", "🗑️ Removed Rows"])
    
    mapping = workflow_state.column_mapping
    
    with tab_cleaned:
        st.write("Download the cleaned data files (after all cleaning steps):")
        
        # Display download buttons for each of 5 cleaned files (Requirement 4.1)
        cols = st.columns(5)
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            
            with cols[i]:
                st.write(f"**File {file_num}**")
                
                if file_state.cleaned_df is not None and len(file_state.cleaned_df) > 0:
                    row_count = len(file_state.cleaned_df)
                    st.write(f"{row_count:,} rows")
                    
                    # Cache key for this file's Excel
                    cache_key = f"excel_cache_multi_step6_cleaned_{file_num}"
                    
                    # Only generate Excel if not cached
                    if cache_key not in st.session_state:
                        st.session_state[cache_key] = export_to_excel(file_state.cleaned_df)
                    
                    # Download button (Requirement 4.3 - export to Excel format)
                    st.download_button(
                        label="📥 Download",
                        data=st.session_state[cache_key],
                        file_name=f"file{file_num}_cleaned.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"download_cleaned_{file_num}",
                        use_container_width=True
                    )
                else:
                    st.write("No data")
                    st.button(
                        "📥 Download",
                        disabled=True,
                        key=f"download_cleaned_{file_num}_disabled",
                        use_container_width=True
                    )
    
    with tab_removed:
        st.write("Download the removed rows files (rows removed during cleaning):")
        
        # Display download buttons for each of 5 removed-rows files (Requirement 4.2)
        cols = st.columns(5)
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            
            with cols[i]:
                st.write(f"**File {file_num}**")
                
                if file_state.removed_df is not None and len(file_state.removed_df) > 0:
                    row_count = len(file_state.removed_df)
                    st.write(f"{row_count:,} rows")
                    
                    # Cache key for this file's Excel
                    cache_key = f"excel_cache_multi_step6_removed_{file_num}"
                    
                    # Only generate Excel if not cached
                    if cache_key not in st.session_state:
                        st.session_state[cache_key] = export_removed_rows_to_excel(
                            file_state.removed_df, 
                            mapping
                        )
                    
                    # Download button (Requirement 4.3 - export to Excel format)
                    st.download_button(
                        label="📥 Download",
                        data=st.session_state[cache_key],
                        file_name=f"file{file_num}_removed.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"download_removed_{file_num}",
                        use_container_width=True
                    )
                else:
                    st.write("No rows removed")
                    st.button(
                        "📥 Download",
                        disabled=True,
                        key=f"download_removed_{file_num}_disabled",
                        use_container_width=True
                    )
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 5", use_container_width=True):
            go_to_step("5. Phone Number Removal")
            st.rerun()
    
    with col2:
        if st.button(
            "Next → Step 7: Master Phone Suppression",
            type="primary",
            use_container_width=True
        ):
            go_to_step("7. Master Phone Suppression")
            st.rerun()


def render_multi_step7_master_suppression():
    """Step 7: Master Phone List Suppression for all 5 files in multi-file workflow.
    
    Uploads a master phone list Excel file with multiple tabs and filters all 5 files
    against the extracted phone numbers. Phone numbers are normalized to 10 digits.
    
    Features:
    - File uploader for master phone list Excel (multi-tab support)
    - Extracts and normalizes phone numbers from ALL tabs using load_phones_from_all_tabs()
    - Displays count of extracted phone numbers
    - "Apply Suppression" button to filter all 5 files
    - Per-file removal statistics display
    - Handles invalid data gracefully with warnings
    
    Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7
    """
    st.header("Step 7: Master Phone Suppression (Multi-File)")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 5 phone filtering has been done (Step 6 is download, doesn't modify data)
    phone_done = all(
        5 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not phone_done:
        st.warning("Please complete Step 5 (Phone Number Removal) first.")
        if st.button("← Go to Step 5"):
            go_to_step("5. Phone Number Removal")
            st.rerun()
        return
    
    mapping = workflow_state.column_mapping
    
    st.write("Upload a master phone list Excel file with multiple tabs.")
    st.write("Phone numbers from **all tabs** will be extracted and used to filter **all 5 files**.")
    
    st.divider()
    
    # File uploader for master phone list (Requirement 5.1)
    st.subheader("📤 Upload Master Phone List")
    
    master_file = st.file_uploader(
        "Upload Master Phone List (Excel with multiple tabs)",
        type=['xlsx', 'xls'],
        key='multi_master_phone_upload',
        help="Excel file containing phone numbers across multiple tabs"
    )
    
    # Process file if newly uploaded (not already loaded in workflow state)
    if master_file and workflow_state.master_phone_list is None:
        try:
            file_bytes = master_file.read()
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.write(f"📂 Reading Master Phone List ({file_size_mb:.1f} MB)...")
            progress_bar.progress(20)
            
            status_text.write("⏳ Extracting phone numbers from all tabs...")
            progress_bar.progress(50)
            
            # Extract phones from all tabs (Requirement 5.2, 5.3)
            master_phones = load_phones_from_all_tabs(BytesIO(file_bytes))
            
            progress_bar.progress(80)
            status_text.write("📊 Processing phone data...")
            
            # Store in workflow state
            workflow_state.master_phone_list = master_phones
            
            progress_bar.progress(100)
            time.sleep(0.3)
            progress_bar.empty()
            status_text.empty()
            
            # Display count of extracted phone numbers (Requirement 5.3)
            if len(master_phones) > 0:
                st.success(f"✓ Extracted {len(master_phones):,} unique phone numbers from all tabs")
            else:
                # Handle invalid data gracefully (Requirement 5.7)
                st.warning("⚠️ No valid phone numbers found in the uploaded file. Please check the file format.")
                workflow_state.master_phone_list = None
                
        except Exception as e:
            # Handle invalid data gracefully (Requirement 5.7)
            st.error(f"Error loading file: {e}")
            st.warning("Please ensure the file is a valid Excel file with phone numbers.")
    
    # Show loaded status if already loaded
    elif workflow_state.master_phone_list is not None:
        phone_count = len(workflow_state.master_phone_list)
        if phone_count > 0:
            st.success(f"✓ {phone_count:,} unique phone numbers loaded from master list")
        else:
            st.warning("⚠️ Master phone list is empty. Please upload a new file.")
            # Allow re-upload by clearing the state
            if st.button("Clear and Re-upload"):
                workflow_state.master_phone_list = None
                st.rerun()
    
    st.divider()
    
    # Display current file status
    st.subheader("📁 Files to Filter")
    
    # Build summary of files before suppression
    before_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        row_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        before_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Current Rows": f"{row_count:,}"
        })
    
    before_df = pd.DataFrame(before_data)
    st.dataframe(before_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Check if suppression has already been done (check step_results for step 7)
    suppression_done = all(
        7 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    # Check if we need to do suppression (Requirement 5.4)
    if st.session_state.get('do_multi_suppression', False):
        st.session_state.do_multi_suppression = False
        
        # Get master phone list
        master_phones = workflow_state.master_phone_list
        
        if not master_phones:
            st.error("No master phone list loaded. Please upload a file first.")
            st.rerun()
            return
        
        # Full-width progress display
        st.subheader("🔍 Applying Master Phone Suppression to all 5 files...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track removal stats per file (Requirement 5.5)
        file_removal_summaries = [{} for _ in range(5)]
        file_before_counts = []
        file_after_counts = []
        
        # Store before counts
        for file_state in workflow_state.files:
            file_before_counts.append(len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0)
        
        # Process each file (Requirement 5.4)
        total_files = len(workflow_state.files)
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            
            # Update progress
            progress_bar.progress((i + 1) / total_files)
            with status_placeholder.container():
                st.write(f"⏳ Processing File {file_num}...")
            
            # Skip files that aren't uploaded or don't have data
            if not file_state.is_uploaded or file_state.cleaned_df is None:
                file_removal_summaries[i]['master_phone_match'] = 0
                file_after_counts.append(0)
                continue
            
            # Get the current cleaned DataFrame
            df = file_state.cleaned_df.copy()
            before_count = len(df)
            
            # Filter rows where phone matches master list (Requirement 5.4)
            def matches_master_phone(phone_val) -> bool:
                normalized = normalize_phone(phone_val)
                return normalized in master_phones
            
            try:
                # Apply suppression filter
                match_mask = df[mapping.phone].apply(matches_master_phone)
                
                cleaned_df = df[~match_mask].copy()
                removed_df = df[match_mask].copy()
                removed_count = len(removed_df)
                
                # Update the file state's cleaned_df
                file_state.cleaned_df = cleaned_df
                
                # Track removal stats (Requirement 5.5)
                file_removal_summaries[i]['master_phone_match'] = removed_count
                
                # Accumulate removed rows in file state (Requirement 5.6)
                if removed_count > 0:
                    removed_df['_removal_reason'] = 'master_phone_match'
                    
                    if file_state.removed_df is None or len(file_state.removed_df) == 0:
                        file_state.removed_df = removed_df.copy()
                    else:
                        file_state.removed_df = pd.concat(
                            [file_state.removed_df, removed_df],
                            ignore_index=True
                        )
                
                file_after_counts.append(len(cleaned_df))
                
            except Exception as e:
                # Handle invalid data gracefully (Requirement 5.7)
                st.warning(f"⚠️ Warning for File {file_num}: {e}")
                file_removal_summaries[i]['master_phone_match'] = 0
                file_after_counts.append(before_count)
        
        # Create StepResults for each file (Requirement 5.6)
        for i, file_state in enumerate(workflow_state.files):
            after_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
            
            file_state.step_results[7] = StepResult(
                cleaned_df=file_state.cleaned_df,
                all_removed_df=file_state.removed_df if file_state.removed_df is not None else pd.DataFrame(),
                before_count=file_before_counts[i],
                after_count=after_count,
                removal_summary=file_removal_summaries[i]
            )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        st.rerun()
    
    # Show Apply Suppression button if master phone list is loaded and suppression not done yet
    if workflow_state.master_phone_list is not None and len(workflow_state.master_phone_list) > 0 and not suppression_done:
        if st.button("🔍 Apply Suppression to All Files", type="primary"):
            st.session_state.do_multi_suppression = True
            st.rerun()
    
    # Display results if suppression has been done (Requirement 5.5)
    if suppression_done:
        st.success("✅ Master phone suppression complete for all 5 files!")
        
        st.divider()
        st.subheader("📊 Suppression Results Summary")
        
        # Build summary table with before/after counts for all 5 files
        summary_data = []
        total_before = 0
        total_after = 0
        total_removed = 0
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(7)
            
            if step_result:
                before = step_result.before_count
                after = step_result.after_count
                removed = before - after
                
                total_before += before
                total_after += after
                total_removed += removed
                
                summary_data.append({
                    "File": f"File {file_num}",
                    "Filename": file_state.filename or "—",
                    "Before": f"{before:,}",
                    "Removed": f"{removed:,}",
                    "After": f"{after:,}"
                })
        
        # Add totals row
        summary_data.append({
            "File": "**TOTAL**",
            "Filename": "—",
            "Before": f"**{total_before:,}**",
            "Removed": f"**{total_removed:,}**",
            "After": f"**{total_after:,}**"
        })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show per-file removal details in expanders
        st.divider()
        st.subheader("📋 Per-File Removal Details")
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(7)
            
            if step_result:
                removed = step_result.before_count - step_result.after_count
                with st.expander(f"File {file_num}: {file_state.filename or 'Unknown'} ({removed:,} rows removed)"):
                    # Display removal summary for this file
                    if step_result.removal_summary:
                        for reason, count in step_result.removal_summary.items():
                            if count > 0:
                                st.write(f"- Master phone match: {count:,} rows")
                    else:
                        st.write("No rows removed from this file.")
                    
                    # Preview cleaned data
                    if step_result.cleaned_df is not None and len(step_result.cleaned_df) > 0:
                        st.write("**Cleaned Data Preview:**")
                        st.dataframe(step_result.cleaned_df.head(10))
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 6", use_container_width=True):
            go_to_step("6. Download Cleaned Files")
            st.rerun()
    
    with col2:
        # Next button - only enabled when suppression is done
        if st.button(
            "Next → Step 8: Cross-File Dedupe",
            type="primary",
            use_container_width=True,
            disabled=not suppression_done
        ):
            go_to_step("8. Cross-File Dedupe")
            st.rerun()


def render_multi_step8_crossfile_dedupe():
    """Step 8: Cross-File Deduplication for all 5 files in multi-file workflow.
    
    Performs cross-file deduplication where:
    - File 1 keeps all rows (newest file, reference)
    - File 2: removes phones that exist in File 1
    - File 3: removes phones that exist in Files 1-2
    - File 4: removes phones that exist in Files 1-3
    - File 5: removes phones that exist in Files 1-4
    
    Uses files from the previous step (no re-upload required).
    Displays before/after row counts for each file.
    
    Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8
    """
    st.header("Step 8: Cross-File Deduplication (Multi-File)")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 7 master suppression has been done
    suppression_done = all(
        7 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not suppression_done:
        st.warning("Please complete Step 7 (Master Phone Suppression) first.")
        if st.button("← Go to Step 7"):
            go_to_step("7. Master Phone Suppression")
            st.rerun()
        return
    
    mapping = workflow_state.column_mapping
    
    st.write("Cross-file deduplication removes duplicate phone numbers across all 5 files.")
    st.write("**File 1 (newest)** keeps all rows. Older files have duplicates removed.")
    
    st.divider()
    
    # Display current file status (Requirement 6.1, 6.7)
    st.subheader("📁 Current Row Counts")
    
    # Build summary of files before deduplication
    before_data = []
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        row_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        priority_note = "(Reference - keeps all rows)" if file_num == 1 else f"(Dedupe against Files 1-{file_num-1})"
        before_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Current Rows": f"{row_count:,}",
            "Deduplication": priority_note
        })
    
    before_df = pd.DataFrame(before_data)
    st.dataframe(before_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Check if deduplication has already been done (check step_results for step 8)
    dedupe_done = all(
        8 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    # Check if we need to do deduplication (Requirement 6.1)
    if st.session_state.get('do_multi_crossfile_dedupe', False):
        st.session_state.do_multi_crossfile_dedupe = False
        
        # Full-width progress display
        st.subheader("🔄 Running Cross-File Deduplication...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track removal stats per file
        file_before_counts = []
        file_after_counts = []
        file_removal_summaries = [{} for _ in range(5)]
        
        # Store before counts
        for file_state in workflow_state.files:
            file_before_counts.append(len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0)
        
        # File 1 keeps all rows (Requirement 6.2)
        with status_placeholder.container():
            st.write("✅ File 1: Reference file (keeps all rows)")
            st.write("⏳ Processing File 2...")
        progress_bar.progress(10)
        
        # File 1 - no changes, just record the result
        file_state_1 = workflow_state.files[0]
        file_removal_summaries[0]['crossfile_dedupe'] = 0
        file_after_counts.append(file_before_counts[0])
        
        # File 2: remove phones in File 1 (Requirement 6.3)
        with status_placeholder.container():
            st.write("✅ File 1: Reference file (keeps all rows)")
            st.write("⏳ Processing File 2: Removing phones in File 1...")
        progress_bar.progress(25)
        
        file_state_2 = workflow_state.files[1]
        if file_state_2.is_uploaded and file_state_2.cleaned_df is not None:
            reference_dfs = [workflow_state.files[0].cleaned_df]
            result = dedupe_against_files(file_state_2.cleaned_df, reference_dfs, mapping.phone)
            
            file_state_2.cleaned_df = result.cleaned_df
            file_removal_summaries[1]['crossfile_dedupe'] = result.removed_count
            file_after_counts.append(len(result.cleaned_df))
            
            # Accumulate removed rows
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'crossfile_dedupe'
                if file_state_2.removed_df is None or len(file_state_2.removed_df) == 0:
                    file_state_2.removed_df = result.removed_df.copy()
                else:
                    file_state_2.removed_df = pd.concat(
                        [file_state_2.removed_df, result.removed_df],
                        ignore_index=True
                    )
        else:
            file_removal_summaries[1]['crossfile_dedupe'] = 0
            file_after_counts.append(0)
        
        # File 3: remove phones in Files 1-2 (Requirement 6.4)
        with status_placeholder.container():
            st.write("✅ File 1: Reference file (keeps all rows)")
            st.write("✅ File 2: Deduped against File 1")
            st.write("⏳ Processing File 3: Removing phones in Files 1-2...")
        progress_bar.progress(45)
        
        file_state_3 = workflow_state.files[2]
        if file_state_3.is_uploaded and file_state_3.cleaned_df is not None:
            reference_dfs = [workflow_state.files[0].cleaned_df, workflow_state.files[1].cleaned_df]
            result = dedupe_against_files(file_state_3.cleaned_df, reference_dfs, mapping.phone)
            
            file_state_3.cleaned_df = result.cleaned_df
            file_removal_summaries[2]['crossfile_dedupe'] = result.removed_count
            file_after_counts.append(len(result.cleaned_df))
            
            # Accumulate removed rows
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'crossfile_dedupe'
                if file_state_3.removed_df is None or len(file_state_3.removed_df) == 0:
                    file_state_3.removed_df = result.removed_df.copy()
                else:
                    file_state_3.removed_df = pd.concat(
                        [file_state_3.removed_df, result.removed_df],
                        ignore_index=True
                    )
        else:
            file_removal_summaries[2]['crossfile_dedupe'] = 0
            file_after_counts.append(0)
        
        # File 4: remove phones in Files 1-3 (Requirement 6.5)
        with status_placeholder.container():
            st.write("✅ File 1: Reference file (keeps all rows)")
            st.write("✅ File 2: Deduped against File 1")
            st.write("✅ File 3: Deduped against Files 1-2")
            st.write("⏳ Processing File 4: Removing phones in Files 1-3...")
        progress_bar.progress(65)
        
        file_state_4 = workflow_state.files[3]
        if file_state_4.is_uploaded and file_state_4.cleaned_df is not None:
            reference_dfs = [
                workflow_state.files[0].cleaned_df,
                workflow_state.files[1].cleaned_df,
                workflow_state.files[2].cleaned_df
            ]
            result = dedupe_against_files(file_state_4.cleaned_df, reference_dfs, mapping.phone)
            
            file_state_4.cleaned_df = result.cleaned_df
            file_removal_summaries[3]['crossfile_dedupe'] = result.removed_count
            file_after_counts.append(len(result.cleaned_df))
            
            # Accumulate removed rows
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'crossfile_dedupe'
                if file_state_4.removed_df is None or len(file_state_4.removed_df) == 0:
                    file_state_4.removed_df = result.removed_df.copy()
                else:
                    file_state_4.removed_df = pd.concat(
                        [file_state_4.removed_df, result.removed_df],
                        ignore_index=True
                    )
        else:
            file_removal_summaries[3]['crossfile_dedupe'] = 0
            file_after_counts.append(0)
        
        # File 5: remove phones in Files 1-4 (Requirement 6.6)
        with status_placeholder.container():
            st.write("✅ File 1: Reference file (keeps all rows)")
            st.write("✅ File 2: Deduped against File 1")
            st.write("✅ File 3: Deduped against Files 1-2")
            st.write("✅ File 4: Deduped against Files 1-3")
            st.write("⏳ Processing File 5: Removing phones in Files 1-4...")
        progress_bar.progress(85)
        
        file_state_5 = workflow_state.files[4]
        if file_state_5.is_uploaded and file_state_5.cleaned_df is not None:
            reference_dfs = [
                workflow_state.files[0].cleaned_df,
                workflow_state.files[1].cleaned_df,
                workflow_state.files[2].cleaned_df,
                workflow_state.files[3].cleaned_df
            ]
            result = dedupe_against_files(file_state_5.cleaned_df, reference_dfs, mapping.phone)
            
            file_state_5.cleaned_df = result.cleaned_df
            file_removal_summaries[4]['crossfile_dedupe'] = result.removed_count
            file_after_counts.append(len(result.cleaned_df))
            
            # Accumulate removed rows
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'crossfile_dedupe'
                if file_state_5.removed_df is None or len(file_state_5.removed_df) == 0:
                    file_state_5.removed_df = result.removed_df.copy()
                else:
                    file_state_5.removed_df = pd.concat(
                        [file_state_5.removed_df, result.removed_df],
                        ignore_index=True
                    )
        else:
            file_removal_summaries[4]['crossfile_dedupe'] = 0
            file_after_counts.append(0)
        
        # Create StepResults for each file (Requirement 6.8)
        for i, file_state in enumerate(workflow_state.files):
            after_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
            
            file_state.step_results[8] = StepResult(
                cleaned_df=file_state.cleaned_df,
                all_removed_df=file_state.removed_df if file_state.removed_df is not None else pd.DataFrame(),
                before_count=file_before_counts[i],
                after_count=after_count,
                removal_summary=file_removal_summaries[i]
            )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        st.rerun()
    
    # Show Run Deduplication button if not done yet (Requirement 6.1)
    if not dedupe_done:
        if st.button("🔄 Run Deduplication", type="primary"):
            st.session_state.do_multi_crossfile_dedupe = True
            st.rerun()
    
    # Display results if deduplication has been done (Requirement 6.7)
    if dedupe_done:
        st.success("✅ Cross-file deduplication complete for all 5 files!")
        
        st.divider()
        st.subheader("📊 Deduplication Results Summary")
        
        # Build summary table with before/after counts for all 5 files (Requirement 6.7)
        summary_data = []
        total_before = 0
        total_after = 0
        total_removed = 0
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(8)
            
            if step_result:
                before = step_result.before_count
                after = step_result.after_count
                removed = before - after
                
                total_before += before
                total_after += after
                total_removed += removed
                
                # Add note for File 1
                note = "(Reference)" if file_num == 1 else ""
                
                summary_data.append({
                    "File": f"File {file_num} {note}".strip(),
                    "Filename": file_state.filename or "—",
                    "Before": f"{before:,}",
                    "Removed": f"{removed:,}",
                    "After": f"{after:,}"
                })
        
        # Add totals row
        summary_data.append({
            "File": "**TOTAL**",
            "Filename": "—",
            "Before": f"**{total_before:,}**",
            "Removed": f"**{total_removed:,}**",
            "After": f"**{total_after:,}**"
        })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show per-file removal details in expanders
        st.divider()
        st.subheader("📋 Per-File Deduplication Details")
        
        for i, file_state in enumerate(workflow_state.files):
            file_num = i + 1
            step_result = file_state.step_results.get(8)
            
            if step_result:
                removed = step_result.before_count - step_result.after_count
                
                if file_num == 1:
                    label = f"File {file_num}: {file_state.filename or 'Unknown'} (Reference - no rows removed)"
                else:
                    label = f"File {file_num}: {file_state.filename or 'Unknown'} ({removed:,} duplicates removed)"
                
                with st.expander(label):
                    if file_num == 1:
                        st.write("File 1 is the reference file (newest). All rows are kept.")
                    else:
                        st.write(f"Removed {removed:,} rows with phone numbers found in Files 1-{file_num-1}.")
                    
                    # Preview cleaned data
                    if step_result.cleaned_df is not None and len(step_result.cleaned_df) > 0:
                        st.write(f"**Final Row Count:** {len(step_result.cleaned_df):,}")
                        st.write("**Data Preview:**")
                        st.dataframe(step_result.cleaned_df.head(10))
    
    st.divider()
    
    # Navigation buttons
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 7", use_container_width=True):
            go_to_step("7. Master Phone Suppression")
            st.rerun()
    
    with col2:
        # Next button - only enabled when deduplication is done
        if st.button(
            "Next → Final Download",
            type="primary",
            use_container_width=True,
            disabled=not dedupe_done
        ):
            go_to_step("Final Download")
            st.rerun()


def render_multi_final_download():
    """Final Download: Download all 5 final files after all processing is complete.
    
    Provides download buttons for:
    - Each of the 5 final files (file1_final.xlsx through file5_final.xlsx)
    - A ZIP archive containing all 5 final files
    
    Displays the final row count for each file before download.
    
    Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
    """
    st.header("Final Download")
    
    # Get workflow state
    workflow_state = st.session_state.multi_file_state
    
    if workflow_state is None:
        st.warning("Please complete Step 1 (Upload 5 Files) first.")
        if st.button("← Go to Step 1"):
            go_to_step("1. Upload 5 Files")
            st.rerun()
        return
    
    # Check if Step 8 cross-file deduplication has been done
    dedupe_done = all(
        8 in file_state.step_results 
        for file_state in workflow_state.files 
        if file_state.is_uploaded
    )
    
    if not dedupe_done:
        st.warning("Please complete Step 8 (Cross-File Deduplication) first.")
        if st.button("← Go to Step 8"):
            go_to_step("8. Cross-File Dedupe")
            st.rerun()
        return
    
    st.success("🎉 All processing complete! Your files are ready for download.")
    st.write("Download your final cleaned files after all processing steps (cleaning, suppression, and deduplication).")
    
    st.divider()
    
    # Display final row count for each file (Requirement 7.5)
    st.subheader("📊 Final File Summary")
    
    summary_data = []
    total_final_rows = 0
    
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        final_count = len(file_state.cleaned_df) if file_state.cleaned_df is not None else 0
        total_final_rows += final_count
        
        summary_data.append({
            "File": f"File {file_num}",
            "Filename": file_state.filename or "—",
            "Final Row Count": f"{final_count:,}"
        })
    
    # Add totals row
    summary_data.append({
        "File": "**TOTAL**",
        "Filename": "—",
        "Final Row Count": f"**{total_final_rows:,}**"
    })
    
    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Download All as ZIP section (Requirement 7.2, 7.4)
    st.subheader("📦 Download All Final Files as ZIP")
    
    # Build the files dictionary for ZIP export
    zip_files = {}
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        
        # Add final file (Requirement 7.4 - descriptive naming: file1_final.xlsx, etc.)
        if file_state.cleaned_df is not None and len(file_state.cleaned_df) > 0:
            zip_files[f"file{file_num}_final.xlsx"] = file_state.cleaned_df
    
    # Cache key for ZIP file
    cache_key_zip = "excel_cache_multi_final_zip"
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if len(zip_files) > 0:
            # Only generate ZIP if not cached
            if cache_key_zip not in st.session_state:
                with st.spinner("Preparing ZIP archive..."):
                    st.session_state[cache_key_zip] = export_to_zip(zip_files)
            
            st.download_button(
                label="📥 Download All as ZIP",
                data=st.session_state[cache_key_zip],
                file_name="final_files.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True,
                key="download_final_zip"
            )
            st.caption(f"Contains {len(zip_files)} final files")
        else:
            st.warning("No files available for download.")
    
    st.divider()
    
    # Individual file downloads section (Requirements 7.1, 7.3)
    st.subheader("📄 Download Individual Final Files")
    
    st.write("Download each final file individually:")
    
    # Display download buttons for each of 5 final files (Requirement 7.1)
    cols = st.columns(5)
    
    for i, file_state in enumerate(workflow_state.files):
        file_num = i + 1
        
        with cols[i]:
            st.write(f"**File {file_num}**")
            
            if file_state.cleaned_df is not None and len(file_state.cleaned_df) > 0:
                row_count = len(file_state.cleaned_df)
                st.write(f"{row_count:,} rows")
                
                # Cache key for this file's Excel
                cache_key = f"excel_cache_multi_final_{file_num}"
                
                # Only generate Excel if not cached
                if cache_key not in st.session_state:
                    st.session_state[cache_key] = export_to_excel(file_state.cleaned_df)
                
                # Download button (Requirement 7.3 - export to Excel format)
                st.download_button(
                    label="📥 Download",
                    data=st.session_state[cache_key],
                    file_name=f"file{file_num}_final.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_final_{file_num}",
                    use_container_width=True
                )
            else:
                st.write("No data")
                st.button(
                    "📥 Download",
                    disabled=True,
                    key=f"download_final_{file_num}_disabled",
                    use_container_width=True
                )
    
    st.divider()
    
    # Navigation button - back to Step 8
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("← Back to Step 8", use_container_width=True):
            go_to_step("8. Cross-File Dedupe")
            st.rerun()
    
    with col2:
        if st.button("🏠 Return to Home", type="primary", use_container_width=True):
            st.session_state.workflow_mode = None
            st.rerun()


def main():
    """Main entry point - renders the wizard UI.
    
    Routes to the appropriate workflow based on workflow_mode in session state:
    - None: Show home page for workflow selection
    - "single": Show single-file workflow (existing 6-step process)
    - "multi": Show multi-file workflow (new 8-step process)
    
    Requirements: 1.3, 1.4, 10.4
    """
    st.set_page_config(page_title="Refinance Data Cleansing", layout="wide")
    
    init_session_state()
    
    # Route based on workflow mode
    workflow_mode = st.session_state.workflow_mode
    
    if workflow_mode is None:
        # Show home page when workflow_mode is not set (Requirement 1.1, 1.2)
        st.title("Refinance Data Cleansing")
        render_home_page()
    
    elif workflow_mode == "single":
        # Single-file workflow (Requirement 1.3, 10.4)
        st.title("Refinance Data Cleansing - Single File")
        
        # Sidebar navigation for single-file workflow
        st.sidebar.title("Navigation")
        
        # Home button to return to workflow selection
        if st.sidebar.button("🏠 Home", use_container_width=True):
            st.session_state.workflow_mode = None
            st.rerun()
        
        st.sidebar.divider()
        
        # Step navigation using single-file steps
        step_index = SINGLE_FILE_STEPS.index(st.session_state.current_step) if st.session_state.current_step in SINGLE_FILE_STEPS else 0
        step = st.sidebar.radio(
            "Select Step",
            SINGLE_FILE_STEPS,
            index=step_index
        )
        st.session_state.current_step = step
        
        # Route to appropriate single-file step
        if step == "1. Upload Raw Data":
            render_step1_upload()
        elif step == "2. Clean Bad Data":
            render_step2_clean()
        elif step == "3. TCPA DNC File":
            render_step3_dnc()
        elif step == "4. Zip Code Removal":
            render_step4_zipcode()
        elif step == "5. Phone Number Removal":
            render_step5_phones()
        elif step == "6. Cross-File Dedupe":
            render_step6_crossfile_dedupe()
    
    elif workflow_mode == "multi":
        # Multi-file workflow (Requirement 1.4, 10.4)
        st.title("Refinance Data Cleansing - Multi File")
        
        # Ensure multi-file workflow state is initialized (Requirement 9.1)
        init_multi_file_workflow_state()
        
        # Sidebar navigation for multi-file workflow
        st.sidebar.title("Navigation")
        
        # Home button to return to workflow selection
        if st.sidebar.button("🏠 Home", use_container_width=True):
            st.session_state.workflow_mode = None
            st.rerun()
        
        st.sidebar.divider()
        
        # Get available multi-file steps (excluding "Home" which is index 0)
        multi_steps_display = MULTI_FILE_STEPS[1:]  # Skip "Home" entry
        
        # Step navigation using multi-file steps
        current_step = st.session_state.current_step
        step_index = multi_steps_display.index(current_step) if current_step in multi_steps_display else 0
        step = st.sidebar.radio(
            "Select Step",
            multi_steps_display,
            index=step_index
        )
        st.session_state.current_step = step
        
        # Route to appropriate multi-file step
        # Note: Multi-file step render functions will be implemented in subsequent tasks
        if step == "1. Upload 5 Files":
            render_multi_step1_upload()
        elif step == "2. Clean Bad Data":
            render_multi_step2_clean()
        elif step == "3. TCPA DNC File":
            render_multi_step3_dnc()
        elif step == "4. Zip Code Removal":
            render_multi_step4_zipcode()
        elif step == "5. Phone Number Removal":
            render_multi_step5_phones()
        elif step == "6. Download Cleaned Files":
            render_multi_step6_download()
        elif step == "7. Master Phone Suppression":
            render_multi_step7_master_suppression()
        elif step == "8. Cross-File Dedupe":
            render_multi_step8_crossfile_dedupe()
        elif step == "Final Download":
            render_multi_final_download()


def render_step1_upload():
    """Step 1: Upload raw data file and validate columns."""
    st.header("Step 1: Upload Raw Data File")
    
    # Show required columns info
    with st.expander("Required Columns", expanded=False):
        st.write("Your file must contain these columns:")
        for col in REQUIRED_COLUMNS:
            st.write(f"- {col}")
    
    raw_file = st.file_uploader(
        "Upload Raw Data (Excel/CSV)",
        type=['xlsx', 'xls', 'csv'],
        key='raw_upload'
    )
    
    # Only process file if it's new (not already loaded)
    if raw_file and st.session_state.raw_data is None:
        try:
            ext = get_file_extension(raw_file.name)
            file_bytes = raw_file.read()
            
            # Store file bytes for later highlight detection
            st.session_state.raw_file_bytes = file_bytes
            st.session_state.raw_file_ext = ext
            
            # Load with progress bar
            df = load_file_with_progress(file_bytes, raw_file.name)
            
            # Validate required columns exist
            is_valid, missing_cols = validate_required_columns(df)
            if not is_valid:
                st.error(f"Missing required columns: {', '.join(missing_cols)}")
                st.stop()
            
            # Filter to only required columns (drop extras)
            df, dropped_cols = filter_to_required_columns(df)
            
            st.session_state.raw_data = df
            
            # Auto-set column mapping based on known column names
            mapping = st.session_state.column_mapping
            mapping.phone = 'Phone1'
            mapping.first_name = 'FirstName'
            mapping.last_name = 'LastName'
            mapping.email = 'Email'
            mapping.zip_code = 'ZipCode'
            mapping.lead_id = 'Universal_LeadId'
            st.session_state.column_mapping = mapping
            
            st.success(f"✓ Loaded {len(df)} rows, {len(df.columns)} columns")
            
            if dropped_cols:
                st.info(f"Dropped {len(dropped_cols)} extra columns: {', '.join(dropped_cols)}")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show data preview if data is loaded
    if st.session_state.raw_data is not None:
        st.divider()
        st.subheader("Data Preview")
        st.dataframe(st.session_state.raw_data.head(10))
        
        st.divider()
        if st.button("Next → Step 2: Clean Bad Data", type="primary"):
            go_to_step("2. Clean Bad Data")
            st.rerun()


def render_step2_clean():
    """Step 2: Clean Bad Data with preview and apply button."""
    st.header("Step 2: Clean Bad Data")
    
    if st.session_state.raw_data is None:
        st.warning("Please upload the raw data file first (Step 1).")
        return
    
    mapping = st.session_state.column_mapping
    df = st.session_state.raw_data.copy()
    before_count = len(df)
    
    st.write(f"**Original rows:** {before_count}")
    
    # Remove completely empty columns (all NaN/empty)
    non_empty_cols = [col for col in df.columns if df[col].notna().any()]
    df = df[non_empty_cols].copy()
    
    # Check if we need to do cleaning
    if st.session_state.get('do_cleaning', False):
        st.session_state.do_cleaning = False
        
        # Define cleaning steps for progress display
        cleaning_steps = [
            "Scan for highlighted rows",
            "Filter invalid last names", 
            "Filter empty phones",
            "Filter invalid phones",
            "Filter invalid emails",
            "Filter TEST entries",
            "Filter placeholder emails",
            "Filter fake/suspicious emails",
            "Filter prohibited content",
            "Filter invalid UUIDs"
        ]
        
        # Full-width progress display
        st.subheader("🧹 Cleaning in progress...")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        all_removed = []
        removal_summary = {}
        completed_steps = []
        
        def update_progress(step_idx, step_name, sub_status=None):
            completed_steps.append(step_name)
            progress_bar.progress((step_idx + 1) / len(cleaning_steps))
            with status_placeholder.container():
                for i, s in enumerate(cleaning_steps):
                    if s in completed_steps:
                        st.write(f"✅ {s}")
                    elif i == step_idx + 1 and step_idx + 1 < len(cleaning_steps):
                        st.write(f"⏳ {s}...")
                    elif i > step_idx:
                        st.write(f"⬜ {s}")
            time.sleep(0.05)  # Force Streamlit to flush UI updates
        
        def show_status_with_detail(detail_msg):
            """Show the checklist with a detail message on the first (in-progress) step."""
            with status_placeholder.container():
                st.write(f"⏳ {cleaning_steps[0]}... {detail_msg}")
                for s in cleaning_steps[1:]:
                    st.write(f"⬜ {s}")
            time.sleep(0.01)
        
        # Show initial state
        show_status_with_detail("")
        
        # 1. Remove highlighted rows (for Excel files) - this is the slow step
        if st.session_state.raw_file_ext in ['.xlsx', '.xls'] and st.session_state.raw_file_bytes:
            def highlight_progress(pct, msg):
                progress_bar.progress(pct / 1000)  # Scale to 0-10% of total
                show_status_with_detail(f"↳ {msg}")
            
            _, highlighted_cells = read_excel_with_highlights(
                BytesIO(st.session_state.raw_file_bytes),
                progress_callback=highlight_progress
            )
            
            result = remove_highlighted_rows(df, highlighted_cells)
            df = result.cleaned_df
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'highlighted_cells'
                all_removed.append(result.removed_df)
                removal_summary['Highlighted cells'] = result.removed_count
        update_progress(0, cleaning_steps[0])
        
        # 2. Filter invalid last names
        result = filter_invalid_last_names(df, mapping.last_name)
        df = result.cleaned_df
        if result.removed_count > 0:
            result.removed_df['_removal_reason'] = 'invalid_last_name'
            all_removed.append(result.removed_df)
            removal_summary['Invalid last name'] = result.removed_count
        update_progress(1, cleaning_steps[1])
        
        # 3. Filter empty phones
        result = filter_empty_phones(df, mapping.phone)
        df = result.cleaned_df
        if result.removed_count > 0:
            result.removed_df['_removal_reason'] = 'empty_phone'
            all_removed.append(result.removed_df)
            removal_summary['Empty phone'] = result.removed_count
        update_progress(2, cleaning_steps[2])
        
        # 4. Filter invalid phones
        result = filter_invalid_phones(df, mapping.phone)
        df = result.cleaned_df
        if result.removed_count > 0:
            result.removed_df['_removal_reason'] = 'invalid_phone'
            all_removed.append(result.removed_df)
            removal_summary['Invalid phone'] = result.removed_count
        update_progress(3, cleaning_steps[3])
        
        # 5. Filter invalid emails
        result = filter_invalid_emails(df, mapping.email)
        df = result.cleaned_df
        if result.removed_count > 0:
            result.removed_df['_removal_reason'] = 'invalid_email'
            all_removed.append(result.removed_df)
            removal_summary['Invalid email'] = result.removed_count
        update_progress(4, cleaning_steps[4])
        
        # 6. Filter TEST in first/last name
        result = filter_test_entries(df, mapping.first_name, mapping.last_name)
        df = result.cleaned_df
        if result.removed_count > 0:
            result.removed_df['_removal_reason'] = 'contains_test'
            all_removed.append(result.removed_df)
            removal_summary['Contains TEST in name'] = result.removed_count
        update_progress(5, cleaning_steps[5])
        
        # 7. Filter placeholder emails
        if mapping.email:
            result = filter_placeholder_emails(df, mapping.email)
            df = result.cleaned_df
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'placeholder_email'
                all_removed.append(result.removed_df)
                removal_summary['Placeholder email'] = result.removed_count
        update_progress(6, cleaning_steps[6])
        
        # 8. Filter fake/suspicious emails
        if mapping.email:
            result = filter_fake_emails(df, mapping.email)
            df = result.cleaned_df
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'fake_email'
                all_removed.append(result.removed_df)
                removal_summary['Fake/suspicious email'] = result.removed_count
        update_progress(7, cleaning_steps[7])
        
        # 9. Filter prohibited content
        result = filter_prohibited_content(df)
        df = result.cleaned_df
        if result.removed_count > 0:
            result.removed_df['_removal_reason'] = 'prohibited_content'
            all_removed.append(result.removed_df)
            removal_summary['Prohibited content'] = result.removed_count
        update_progress(8, cleaning_steps[8])
        
        # 10. Filter invalid UUIDs
        if mapping.lead_id:
            result = filter_invalid_uuid(df, mapping.lead_id)
            df = result.cleaned_df
            if result.removed_count > 0:
                result.removed_df['_removal_reason'] = 'invalid_uuid'
                all_removed.append(result.removed_df)
                removal_summary['Invalid/empty UUID'] = result.removed_count
        update_progress(9, cleaning_steps[9])
        
        # Combine removed rows
        removed_df = pd.concat(all_removed, ignore_index=True) if all_removed else pd.DataFrame()
        
        st.session_state.step1_result = StepResult(
            cleaned_df=df,
            all_removed_df=removed_df,
            before_count=before_count,
            after_count=len(df),
            removal_summary=removal_summary
        )
        
        time.sleep(1)  # Brief pause to show completion
        st.rerun()
    
    if st.button("Apply Cleaning", type="primary"):
        st.session_state.do_cleaning = True
        st.rerun()
    
    # Display results
    if st.session_state.step1_result:
        result = st.session_state.step1_result
        
        st.divider()
        col1, col2, col3 = st.columns(3)
        col1.metric("Before", result.before_count)
        col2.metric("Removed", result.before_count - result.after_count)
        col3.metric("After", result.after_count)
        
        st.subheader("Removal Summary")
        
        # Map display names to internal reason codes
        reason_code_map = {
            'Highlighted cells': 'highlighted_cells',
            'Invalid last name': 'invalid_last_name',
            'Empty phone': 'empty_phone',
            'Invalid phone': 'invalid_phone',
            'Invalid email': 'invalid_email',
            'Contains TEST in name': 'contains_test',
            'Placeholder email': 'placeholder_email',
            'Fake/suspicious email': 'fake_email',
            'Prohibited content': 'prohibited_content',
            'Invalid/empty UUID': 'invalid_uuid',
        }
        
        for reason, count in result.removal_summary.items():
            reason_code = reason_code_map.get(reason, reason)
            with st.expander(f"{reason}: {count} rows"):
                # Filter removed rows by this reason
                if len(result.all_removed_df) > 0 and '_removal_reason' in result.all_removed_df.columns:
                    reason_rows = result.all_removed_df[result.all_removed_df['_removal_reason'] == reason_code]
                    if len(reason_rows) > 0:
                        # Show without the internal columns, convert to string to avoid Arrow errors
                        display_cols = [c for c in reason_rows.columns if not c.startswith('_')]
                        display_df = reason_rows[display_cols].head(100).astype(str)
                        st.dataframe(display_df)
                        if len(reason_rows) > 100:
                            st.caption(f"Showing first 100 of {len(reason_rows)} rows")
                    else:
                        st.write("No rows to display")
                else:
                    st.write("No data available")
        
        st.subheader("Cleaned Data Preview")
        st.dataframe(result.cleaned_df.head(25))
        
        # Next button
        st.divider()
        if st.button("Next → Step 3: TCPA DNC File", type="primary"):
            go_to_step("3. TCPA DNC File")
            st.rerun()


def render_step3_dnc():
    """Step 3: Upload TCPA DNC file and run against Step 2 data."""
    st.header("Step 3: TCPA DNC File")
    
    if st.session_state.step1_result is None:
        st.warning("Please complete Step 2 (Clean Bad Data) first.")
        return
    
    st.write("Upload the TCPA LD DNC file. This will filter out:")
    st.write("- Phone numbers matching the DNC list")
    st.write("- Phone numbers with blocked area codes")
    st.write("- Names matching the DNC list (FirstName + LastName)")
    
    dnc_file = st.file_uploader(
        "Upload TCPA LD DNC File (Excel)",
        type=['xlsx', 'xls'],
        key='dnc_upload'
    )
    
    # Only process file if newly uploaded (not already loaded)
    if dnc_file and st.session_state.tcpa_ld_dnc_data is None:
        try:
            file_bytes = dnc_file.read()
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.write(f"📂 Reading DNC file ({file_size_mb:.1f} MB)...")
            progress_bar.progress(20)
            
            # Read specifically from Sheet1 (2)
            xl = pd.ExcelFile(BytesIO(file_bytes))
            sheet_name = 'Sheet1 (2)' if 'Sheet1 (2)' in xl.sheet_names else xl.sheet_names[-1]
            
            status_text.write(f"⏳ Parsing sheet '{sheet_name}'...")
            progress_bar.progress(50)
            
            df = pd.read_excel(xl, sheet_name=sheet_name)
            
            progress_bar.progress(80)
            status_text.write("📊 Processing DNC data...")
            
            st.session_state.tcpa_ld_dnc_data = df
            
            progress_bar.progress(100)
            time.sleep(0.3)
            progress_bar.empty()
            status_text.empty()
            
            # Preview what was loaded
            dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(df)
            st.success(f"✓ Loaded DNC file from sheet '{sheet_name}'")
            st.write(f"- {len(dnc_phones)} phone numbers")
            st.write(f"- {len(dnc_area_codes)} area codes: {', '.join(sorted(dnc_area_codes))}")
            st.write(f"- {len(dnc_names)} names")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show loaded status if already loaded (but not just uploaded)
    elif st.session_state.tcpa_ld_dnc_data is not None:
        dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(st.session_state.tcpa_ld_dnc_data)
        st.success(f"✓ DNC file loaded")
        st.write(f"- {len(dnc_phones)} phone numbers")
        st.write(f"- {len(dnc_area_codes)} area codes")
        st.write(f"- {len(dnc_names)} names")
    
    # Show current data count
    if st.session_state.step1_result:
        st.divider()
        st.write(f"**Input rows from Step 2:** {len(st.session_state.step1_result.cleaned_df)}")
    
    # Run DNC button
    if st.session_state.tcpa_ld_dnc_data is not None and st.session_state.step1_result is not None:
        if st.button("Run DNC against Step 2 data file", type="primary"):
            mapping = st.session_state.column_mapping
            df = st.session_state.step1_result.cleaned_df.copy()
            before_count = len(df)
            
            # Progress display
            st.subheader("🔍 DNC Filtering in progress...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_removed = []
            removal_summary = {}
            
            dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(st.session_state.tcpa_ld_dnc_data)
            
            # 1. Filter by DNC phone numbers
            status_text.write(f"⏳ Checking {before_count:,} rows against {len(dnc_phones):,} DNC phone numbers...")
            progress_bar.progress(10)
            
            if mapping.phone and dnc_phones:
                result = filter_by_dnc_phones(df, mapping.phone, dnc_phones)
                df = result.cleaned_df
                if result.removed_count > 0:
                    result.removed_df['_removal_reason'] = 'dnc_phone_match'
                    all_removed.append(result.removed_df)
                    removal_summary['DNC phone match'] = result.removed_count
            
            status_text.write(f"✅ DNC phone check complete: {removal_summary.get('DNC phone match', 0):,} matches removed")
            progress_bar.progress(40)
            
            # 2. Filter by area code
            status_text.write(f"⏳ Checking area codes against {len(dnc_area_codes)} blocked area codes...")
            
            if mapping.phone and dnc_area_codes:
                result = filter_by_area_code(df, mapping.phone, dnc_area_codes)
                df = result.cleaned_df
                if result.removed_count > 0:
                    result.removed_df['_removal_reason'] = 'dnc_area_code'
                    all_removed.append(result.removed_df)
                    removal_summary['DNC area code'] = result.removed_count
            
            status_text.write(f"✅ Area code check complete: {removal_summary.get('DNC area code', 0):,} matches removed")
            progress_bar.progress(70)
            
            # 3. Filter by name match
            status_text.write(f"⏳ Checking names against {len(dnc_names):,} DNC names...")
            
            if mapping.first_name and mapping.last_name and dnc_names:
                result = filter_by_name_match(df, mapping.first_name, mapping.last_name, dnc_names)
                df = result.cleaned_df
                if result.removed_count > 0:
                    result.removed_df['_removal_reason'] = 'dnc_name_match'
                    all_removed.append(result.removed_df)
                    removal_summary['DNC name match'] = result.removed_count
            
            progress_bar.progress(100)
            total_removed = before_count - len(df)
            status_text.write(f"✅ Complete! Removed {total_removed:,} rows total ({len(df):,} remaining)")
            
            # Combine removed rows
            removed_df = pd.concat(all_removed, ignore_index=True) if all_removed else pd.DataFrame()
            
            st.session_state.step2_result = StepResult(
                cleaned_df=df,
                all_removed_df=removed_df,
                before_count=before_count,
                after_count=len(df),
                removal_summary=removal_summary
            )
            
            time.sleep(1)  # Brief pause to show completion
            st.rerun()
    
    # Display results
    if st.session_state.step2_result:
        result = st.session_state.step2_result
        
        st.divider()
        col1, col2, col3 = st.columns(3)
        col1.metric("Before", result.before_count)
        col2.metric("Removed", result.before_count - result.after_count)
        col3.metric("After", result.after_count)
        
        st.subheader("Removal Summary")
        for reason, count in result.removal_summary.items():
            st.write(f"- {reason}: {count} rows")
        
        st.subheader("Data Preview")
        st.dataframe(result.cleaned_df.head(25))
        
        st.divider()
        if st.button("Next → Step 4: Zip Code Removal", type="primary"):
            go_to_step("4. Zip Code Removal")
            st.rerun()


def render_step4_zipcode():
    """Step 4: Upload Zip Code file and filter."""
    st.header("Step 4: Zip Code Removal")
    
    if st.session_state.step2_result is None:
        st.warning("Please complete Step 3 (TCPA DNC File) first.")
        return
    
    st.write("Upload the Zip Codes file to remove rows with matching zip codes.")
    
    zips_file = st.file_uploader(
        "Upload Zip Codes File (Excel)",
        type=['xlsx', 'xls'],
        key='zips_upload'
    )
    
    # Only process file if newly uploaded (not already loaded)
    if zips_file and st.session_state.tcpa_zips_data is None:
        try:
            file_bytes = zips_file.read()
            df = load_file_with_progress(file_bytes, zips_file.name)
            st.session_state.tcpa_zips_data = df
            tcpa_zips = load_tcpa_zipcodes(df)
            st.success(f"✓ Loaded {len(tcpa_zips)} zip codes")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show loaded status if already loaded (but not just uploaded)
    elif st.session_state.tcpa_zips_data is not None:
        tcpa_zips = load_tcpa_zipcodes(st.session_state.tcpa_zips_data)
        st.success(f"✓ {len(tcpa_zips)} zip codes loaded")
    
    # Show current data count
    if st.session_state.step2_result:
        st.divider()
        st.write(f"**Input rows from Step 3:** {len(st.session_state.step2_result.cleaned_df)}")
    
    # Run zip filter button
    if st.session_state.tcpa_zips_data is not None and st.session_state.step2_result is not None:
        if st.button("Run Zip Code Filter", type="primary"):
            mapping = st.session_state.column_mapping
            df = st.session_state.step2_result.cleaned_df.copy()
            before_count = len(df)
            
            # Progress display
            st.subheader("🔍 Zip Code Filtering in progress...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_removed = []
            removal_summary = {}
            
            tcpa_zips = load_tcpa_zipcodes(st.session_state.tcpa_zips_data)
            
            status_text.write(f"⏳ Checking {before_count:,} rows against {len(tcpa_zips):,} zip codes...")
            progress_bar.progress(25)
            
            if mapping.zip_code and tcpa_zips:
                result = filter_by_tcpa_zips(df, mapping.zip_code, tcpa_zips)
                df = result.cleaned_df
                if result.removed_count > 0:
                    result.removed_df['_removal_reason'] = 'tcpa_zip_match'
                    all_removed.append(result.removed_df)
                    removal_summary['Zip code match'] = result.removed_count
            
            progress_bar.progress(100)
            total_removed = before_count - len(df)
            status_text.write(f"✅ Complete! Removed {total_removed:,} rows ({len(df):,} remaining)")
            
            removed_df = pd.concat(all_removed, ignore_index=True) if all_removed else pd.DataFrame()
            
            st.session_state.step3_result = StepResult(
                cleaned_df=df,
                all_removed_df=removed_df,
                before_count=before_count,
                after_count=len(df),
                removal_summary=removal_summary
            )
            
            time.sleep(1)  # Brief pause to show completion
            st.rerun()
    
    # Display results
    if st.session_state.step3_result:
        result = st.session_state.step3_result
        
        st.divider()
        col1, col2, col3 = st.columns(3)
        col1.metric("Before", result.before_count)
        col2.metric("Removed", result.before_count - result.after_count)
        col3.metric("After", result.after_count)
        
        st.subheader("Removal Summary")
        for reason, count in result.removal_summary.items():
            st.write(f"- {reason}: {count} rows")
        
        st.subheader("Data Preview")
        st.dataframe(result.cleaned_df.head(25))
        
        st.divider()
        if st.button("Next → Step 5: Phone Number Removal", type="primary"):
            go_to_step("5. Phone Number Removal")
            st.rerun()


def render_step5_phones():
    """Step 5: Upload TCPA Phones file and filter + dedupe."""
    st.header("Step 5: Phone Number Removal")
    
    if st.session_state.step3_result is None:
        st.warning("Please complete Step 4 (Zip Code Removal) first.")
        return
    
    st.write("Upload the TCPA Phones file to remove matching phone numbers and duplicates.")
    
    phones_file = st.file_uploader(
        "Upload TCPA Phones File (Excel)",
        type=['xlsx', 'xls', 'csv'],
        key='phones_upload'
    )
    
    # Only process file if newly uploaded (not already loaded)
    if phones_file and st.session_state.tcpa_phones_data is None:
        try:
            file_bytes = phones_file.read()
            df = load_file_with_progress(file_bytes, phones_file.name)
            st.session_state.tcpa_phones_data = df
            tcpa_phones = load_tcpa_phones(df)
            st.success(f"✓ Loaded {len(tcpa_phones)} phone numbers")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    
    # Show loaded status if already loaded (but not just uploaded)
    elif st.session_state.tcpa_phones_data is not None:
        tcpa_phones = load_tcpa_phones(st.session_state.tcpa_phones_data)
        st.success(f"✓ {len(tcpa_phones)} phone numbers loaded")
    
    # Show current data count
    if st.session_state.step3_result:
        st.divider()
        st.write(f"**Input rows from Step 4:** {len(st.session_state.step3_result.cleaned_df)}")
    
    # Run phone filter button
    if st.session_state.tcpa_phones_data is not None and st.session_state.step3_result is not None:
        if st.button("Run Phone Number Filter", type="primary"):
            mapping = st.session_state.column_mapping
            df = st.session_state.step3_result.cleaned_df.copy()
            before_count = len(df)
            
            # Progress display
            st.subheader("🔍 Filtering in progress...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_removed = []
            removal_summary = {}
            
            tcpa_phones = load_tcpa_phones(st.session_state.tcpa_phones_data)
            
            # 1. Filter by TCPA phones
            status_text.write(f"⏳ Checking {before_count:,} rows against {len(tcpa_phones):,} TCPA phone numbers...")
            progress_bar.progress(25)
            
            if mapping.phone and tcpa_phones:
                result = filter_by_tcpa_phones(df, mapping.phone, tcpa_phones)
                df = result.cleaned_df
                if result.removed_count > 0:
                    result.removed_df['_removal_reason'] = 'tcpa_phone_match'
                    all_removed.append(result.removed_df)
                    removal_summary['TCPA phone match'] = result.removed_count
            
            status_text.write(f"✅ TCPA check complete: {removal_summary.get('TCPA phone match', 0):,} matches removed")
            progress_bar.progress(50)
            
            # 2. Remove duplicate phones (always last)
            status_text.write(f"⏳ Removing duplicate phone numbers from {len(df):,} rows...")
            progress_bar.progress(75)
            
            if mapping.phone:
                result = remove_duplicate_phones(df, mapping.phone)
                df = result.cleaned_df
                if result.removed_count > 0:
                    result.removed_df['_removal_reason'] = 'duplicate_phone'
                    all_removed.append(result.removed_df)
                    removal_summary['Duplicate phone'] = result.removed_count
            
            progress_bar.progress(100)
            total_removed = before_count - len(df)
            status_text.write(f"✅ Complete! Removed {total_removed:,} rows total ({len(df):,} remaining)")
            
            removed_df = pd.concat(all_removed, ignore_index=True) if all_removed else pd.DataFrame()
            
            st.session_state.step4_result = StepResult(
                cleaned_df=df,
                all_removed_df=removed_df,
                before_count=before_count,
                after_count=len(df),
                removal_summary=removal_summary
            )
            
            time.sleep(1)  # Brief pause to show completion
            st.rerun()
    
    # Display results
    if st.session_state.step4_result:
        result = st.session_state.step4_result
        
        st.divider()
        
        # Calculate total removed across all steps
        original_count = st.session_state.step1_result.before_count if st.session_state.step1_result else result.before_count
        total_removed = original_count - result.after_count
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Original", original_count)
        col2.metric("Total Removed", total_removed)
        col3.metric("Final", result.after_count)
        
        st.subheader("Removal Summary (All Steps)")
        
        # Aggregate removal summaries from all steps
        all_removal_summary = {}
        if st.session_state.step1_result:
            all_removal_summary.update(st.session_state.step1_result.removal_summary)
        if st.session_state.step2_result:
            all_removal_summary.update(st.session_state.step2_result.removal_summary)
        if st.session_state.step3_result:
            all_removal_summary.update(st.session_state.step3_result.removal_summary)
        all_removal_summary.update(result.removal_summary)
        
        for reason, count in all_removal_summary.items():
            st.write(f"- {reason}: {count} rows")
        
        st.subheader("Final Cleaned Data Preview")
        st.dataframe(result.cleaned_df.head(25))
        
        # Aggregate all removed rows from Steps 2-5
        all_steps_removed = []
        if st.session_state.step1_result and len(st.session_state.step1_result.all_removed_df) > 0:
            all_steps_removed.append(st.session_state.step1_result.all_removed_df)
        if st.session_state.step2_result and len(st.session_state.step2_result.all_removed_df) > 0:
            all_steps_removed.append(st.session_state.step2_result.all_removed_df)
        if st.session_state.step3_result and len(st.session_state.step3_result.all_removed_df) > 0:
            all_steps_removed.append(st.session_state.step3_result.all_removed_df)
        if result.all_removed_df is not None and len(result.all_removed_df) > 0:
            all_steps_removed.append(result.all_removed_df)
        
        combined_removed_df = pd.concat(all_steps_removed, ignore_index=True) if all_steps_removed else pd.DataFrame()
        
        render_download_section(result.cleaned_df, combined_removed_df, "final", st.session_state.column_mapping)
        
        st.success("🎉 Data cleansing complete! Download your final results above.")
        
        # Next button to Step 6
        st.divider()
        if st.button("Next → Step 6: Cross-File Dedupe", type="primary"):
            go_to_step("6. Cross-File Dedupe")
            st.rerun()


def render_step6_crossfile_dedupe():
    """Step 6: Cross-file deduplication across 5 weekly files."""
    st.header("Step 6: Cross-File Deduplication")
    
    if st.session_state.step4_result is None:
        st.warning("Please complete Step 5 (Phone Number Removal) first.")
        return
    
    st.write("Remove duplicate phone numbers across multiple weekly files.")
    st.write("File 1 (newest) keeps all rows. Older files have duplicates removed.")
    
    st.divider()
    
    # --- File 1: Cleaned data from Steps 1-5 (Requirements 5.1, 5.3) ---
    st.subheader("📁 File 1 (Newest) - From Steps 1-5")
    file1_df = st.session_state.step4_result.cleaned_df
    st.success(f"✓ File 1 loaded: **{len(file1_df):,}** rows")
    
    st.divider()
    
    # --- File uploaders for Files 2-5 (Requirements 5.2) ---
    st.subheader("📤 Upload Pre-Cleaned Weekly Files (Files 2-5)")
    st.write("Upload 4 additional pre-cleaned files, from newest to oldest.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # File 2 uploader
        st.write("**File 2 (2nd Newest)**")
        file2_upload = st.file_uploader(
            "Upload File 2",
            type=['xlsx', 'xls', 'csv'],
            key='file2_upload',
            label_visibility="collapsed"
        )
        if file2_upload and st.session_state.file2_data is None:
            try:
                file_bytes = file2_upload.read()
                df = load_file_with_progress(file_bytes, file2_upload.name)
                st.session_state.file2_data = df
            except Exception as e:
                st.error(f"Error loading File 2: {e}")
        
        # File 3 uploader
        st.write("**File 3 (Middle)**")
        file3_upload = st.file_uploader(
            "Upload File 3",
            type=['xlsx', 'xls', 'csv'],
            key='file3_upload',
            label_visibility="collapsed"
        )
        if file3_upload and st.session_state.file3_data is None:
            try:
                file_bytes = file3_upload.read()
                df = load_file_with_progress(file_bytes, file3_upload.name)
                st.session_state.file3_data = df
            except Exception as e:
                st.error(f"Error loading File 3: {e}")
    
    with col2:
        # File 4 uploader
        st.write("**File 4 (2nd Oldest)**")
        file4_upload = st.file_uploader(
            "Upload File 4",
            type=['xlsx', 'xls', 'csv'],
            key='file4_upload',
            label_visibility="collapsed"
        )
        if file4_upload and st.session_state.file4_data is None:
            try:
                file_bytes = file4_upload.read()
                df = load_file_with_progress(file_bytes, file4_upload.name)
                st.session_state.file4_data = df
            except Exception as e:
                st.error(f"Error loading File 4: {e}")
        
        # File 5 uploader
        st.write("**File 5 (Oldest)**")
        file5_upload = st.file_uploader(
            "Upload File 5",
            type=['xlsx', 'xls', 'csv'],
            key='file5_upload',
            label_visibility="collapsed"
        )
        if file5_upload and st.session_state.file5_data is None:
            try:
                file_bytes = file5_upload.read()
                df = load_file_with_progress(file_bytes, file5_upload.name)
                st.session_state.file5_data = df
            except Exception as e:
                st.error(f"Error loading File 5: {e}")
    
    st.divider()
    
    # --- Display row counts for all 5 files (Requirements 5.3) ---
    st.subheader("📊 File Summary")
    
    # Build file info list
    files_info = [
        ("File 1 (Newest)", file1_df, "From Steps 1-5"),
        ("File 2", st.session_state.file2_data, "Pre-cleaned"),
        ("File 3", st.session_state.file3_data, "Pre-cleaned"),
        ("File 4", st.session_state.file4_data, "Pre-cleaned"),
        ("File 5 (Oldest)", st.session_state.file5_data, "Pre-cleaned"),
    ]
    
    # Display in columns
    cols = st.columns(5)
    all_files_loaded = True
    
    for i, (name, df, source) in enumerate(files_info):
        with cols[i]:
            if df is not None:
                st.metric(name, f"{len(df):,}")
                st.caption(source)
            else:
                st.metric(name, "—")
                st.caption("Not loaded")
                if i > 0:  # File 1 is always loaded
                    all_files_loaded = False
    
    # Show status message
    if all_files_loaded:
        st.success("✓ All 5 files loaded! Ready for deduplication.")
    else:
        loaded_count = sum(1 for _, df, _ in files_info if df is not None)
        st.info(f"📁 {loaded_count}/5 files loaded. Upload remaining files to proceed.")
    
    # --- Deduplication Section (Requirements 5.4-5.9) ---
    if all_files_loaded:
        st.divider()
        st.subheader("🔄 Deduplication")
        st.write("Remove duplicate phone numbers from older files. Process from oldest to newest.")
        
        mapping = st.session_state.column_mapping
        phone_col = mapping.phone
        
        # --- Dedupe File 5 (oldest) - removes phones in Files 1-4 (Requirement 5.4) ---
        st.write("---")
        st.write("**File 5 (Oldest)** - Remove phones that exist in Files 1-4")
        
        if st.session_state.file5_deduped is not None:
            # Display before/after counts (Requirement 5.9)
            counts = st.session_state.file5_dedupe_counts
            if counts:
                col1, col2, col3 = st.columns(3)
                col1.metric("Before", f"{counts['before']:,}")
                col2.metric("Removed", f"{counts['removed']:,}")
                col3.metric("After", f"{counts['after']:,}")
            st.success(f"✓ File 5 deduped: {len(st.session_state.file5_deduped):,} rows remaining")
        else:
            if st.button("Dedupe File 5", key="dedupe_file5", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.write("⏳ Building reference phone list from Files 1-4...")
                progress_bar.progress(20)
                
                before_count = len(st.session_state.file5_data)
                reference_dfs = [
                    file1_df,
                    st.session_state.file2_data,
                    st.session_state.file3_data,
                    st.session_state.file4_data
                ]
                
                status_text.write(f"⏳ Checking {before_count:,} rows against reference files...")
                progress_bar.progress(50)
                
                result = dedupe_against_files(st.session_state.file5_data, reference_dfs, phone_col)
                
                progress_bar.progress(100)
                status_text.write(f"✅ Complete! Removed {result.removed_count:,} duplicates ({len(result.cleaned_df):,} remaining)")
                
                st.session_state.file5_deduped = result.cleaned_df
                st.session_state.file5_dedupe_counts = {
                    'before': before_count,
                    'removed': result.removed_count,
                    'after': len(result.cleaned_df)
                }
                time.sleep(1)
                st.rerun()
        
        # --- Dedupe File 4 - removes phones in Files 1-3 (Requirement 5.5) ---
        st.write("---")
        st.write("**File 4** - Remove phones that exist in Files 1-3")
        
        if st.session_state.file4_deduped is not None:
            # Display before/after counts (Requirement 5.9)
            counts = st.session_state.file4_dedupe_counts
            if counts:
                col1, col2, col3 = st.columns(3)
                col1.metric("Before", f"{counts['before']:,}")
                col2.metric("Removed", f"{counts['removed']:,}")
                col3.metric("After", f"{counts['after']:,}")
            st.success(f"✓ File 4 deduped: {len(st.session_state.file4_deduped):,} rows remaining")
        else:
            if st.button("Dedupe File 4", key="dedupe_file4", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.write("⏳ Building reference phone list from Files 1-3...")
                progress_bar.progress(20)
                
                before_count = len(st.session_state.file4_data)
                reference_dfs = [
                    file1_df,
                    st.session_state.file2_data,
                    st.session_state.file3_data
                ]
                
                status_text.write(f"⏳ Checking {before_count:,} rows against reference files...")
                progress_bar.progress(50)
                
                result = dedupe_against_files(st.session_state.file4_data, reference_dfs, phone_col)
                
                progress_bar.progress(100)
                status_text.write(f"✅ Complete! Removed {result.removed_count:,} duplicates ({len(result.cleaned_df):,} remaining)")
                
                st.session_state.file4_deduped = result.cleaned_df
                st.session_state.file4_dedupe_counts = {
                    'before': before_count,
                    'removed': result.removed_count,
                    'after': len(result.cleaned_df)
                }
                time.sleep(1)
                st.rerun()
        
        # --- Dedupe File 3 - removes phones in Files 1-2 (Requirement 5.6) ---
        st.write("---")
        st.write("**File 3** - Remove phones that exist in Files 1-2")
        
        if st.session_state.file3_deduped is not None:
            # Display before/after counts (Requirement 5.9)
            counts = st.session_state.file3_dedupe_counts
            if counts:
                col1, col2, col3 = st.columns(3)
                col1.metric("Before", f"{counts['before']:,}")
                col2.metric("Removed", f"{counts['removed']:,}")
                col3.metric("After", f"{counts['after']:,}")
            st.success(f"✓ File 3 deduped: {len(st.session_state.file3_deduped):,} rows remaining")
        else:
            if st.button("Dedupe File 3", key="dedupe_file3", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.write("⏳ Building reference phone list from Files 1-2...")
                progress_bar.progress(20)
                
                before_count = len(st.session_state.file3_data)
                reference_dfs = [
                    file1_df,
                    st.session_state.file2_data
                ]
                
                status_text.write(f"⏳ Checking {before_count:,} rows against reference files...")
                progress_bar.progress(50)
                
                result = dedupe_against_files(st.session_state.file3_data, reference_dfs, phone_col)
                
                progress_bar.progress(100)
                status_text.write(f"✅ Complete! Removed {result.removed_count:,} duplicates ({len(result.cleaned_df):,} remaining)")
                
                st.session_state.file3_deduped = result.cleaned_df
                st.session_state.file3_dedupe_counts = {
                    'before': before_count,
                    'removed': result.removed_count,
                    'after': len(result.cleaned_df)
                }
                time.sleep(1)
                st.rerun()
        
        # --- Dedupe File 2 - removes phones in File 1 (Requirement 5.7) ---
        st.write("---")
        st.write("**File 2** - Remove phones that exist in File 1")
        
        if st.session_state.file2_deduped is not None:
            # Display before/after counts (Requirement 5.9)
            counts = st.session_state.file2_dedupe_counts
            if counts:
                col1, col2, col3 = st.columns(3)
                col1.metric("Before", f"{counts['before']:,}")
                col2.metric("Removed", f"{counts['removed']:,}")
                col3.metric("After", f"{counts['after']:,}")
            st.success(f"✓ File 2 deduped: {len(st.session_state.file2_deduped):,} rows remaining")
        else:
            if st.button("Dedupe File 2", key="dedupe_file2", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.write("⏳ Building reference phone list from File 1...")
                progress_bar.progress(20)
                
                before_count = len(st.session_state.file2_data)
                reference_dfs = [file1_df]
                
                status_text.write(f"⏳ Checking {before_count:,} rows against File 1...")
                progress_bar.progress(50)
                
                result = dedupe_against_files(st.session_state.file2_data, reference_dfs, phone_col)
                
                progress_bar.progress(100)
                status_text.write(f"✅ Complete! Removed {result.removed_count:,} duplicates ({len(result.cleaned_df):,} remaining)")
                
                st.session_state.file2_deduped = result.cleaned_df
                st.session_state.file2_dedupe_counts = {
                    'before': before_count,
                    'removed': result.removed_count,
                    'after': len(result.cleaned_df)
                }
                time.sleep(1)
                st.rerun()
        
        # --- File 1 note (Requirement 5.8) ---
        st.write("---")
        st.write("**File 1 (Newest)** - No deduplication needed (reference file)")
        st.info(f"File 1 keeps all {len(file1_df):,} rows as the reference.")
        
        # --- Download Section (Requirement 5.10) ---
        # Check if all deduplication is complete
        all_deduped = (
            st.session_state.file2_deduped is not None and
            st.session_state.file3_deduped is not None and
            st.session_state.file4_deduped is not None and
            st.session_state.file5_deduped is not None
        )
        
        if all_deduped:
            st.divider()
            st.subheader("📥 Download Deduplicated Files")
            st.success("🎉 All files deduplicated! Download your results below.")
            
            # Create download buttons for all 5 files
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.write("**File 1**")
                st.write(f"{len(file1_df):,} rows")
                # Cache key for File 1
                cache_key_file1 = "excel_cache_crossfile_file1"
                if cache_key_file1 not in st.session_state:
                    st.session_state[cache_key_file1] = export_to_excel(file1_df)
                st.download_button(
                    label="📥 Download",
                    data=st.session_state[cache_key_file1],
                    file_name="file1_deduped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_file1"
                )
            
            with col2:
                st.write("**File 2**")
                st.write(f"{len(st.session_state.file2_deduped):,} rows")
                cache_key_file2 = "excel_cache_crossfile_file2"
                if cache_key_file2 not in st.session_state:
                    st.session_state[cache_key_file2] = export_to_excel(st.session_state.file2_deduped)
                st.download_button(
                    label="📥 Download",
                    data=st.session_state[cache_key_file2],
                    file_name="file2_deduped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_file2"
                )
            
            with col3:
                st.write("**File 3**")
                st.write(f"{len(st.session_state.file3_deduped):,} rows")
                cache_key_file3 = "excel_cache_crossfile_file3"
                if cache_key_file3 not in st.session_state:
                    st.session_state[cache_key_file3] = export_to_excel(st.session_state.file3_deduped)
                st.download_button(
                    label="📥 Download",
                    data=st.session_state[cache_key_file3],
                    file_name="file3_deduped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_file3"
                )
            
            with col4:
                st.write("**File 4**")
                st.write(f"{len(st.session_state.file4_deduped):,} rows")
                cache_key_file4 = "excel_cache_crossfile_file4"
                if cache_key_file4 not in st.session_state:
                    st.session_state[cache_key_file4] = export_to_excel(st.session_state.file4_deduped)
                st.download_button(
                    label="📥 Download",
                    data=st.session_state[cache_key_file4],
                    file_name="file4_deduped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_file4"
                )
            
            with col5:
                st.write("**File 5**")
                st.write(f"{len(st.session_state.file5_deduped):,} rows")
                cache_key_file5 = "excel_cache_crossfile_file5"
                if cache_key_file5 not in st.session_state:
                    st.session_state[cache_key_file5] = export_to_excel(st.session_state.file5_deduped)
                st.download_button(
                    label="📥 Download",
                    data=st.session_state[cache_key_file5],
                    file_name="file5_deduped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_file5"
                )



def render_download_section(cleaned_df: pd.DataFrame, removed_df: pd.DataFrame, step_name: str, column_mapping):
    """Render download buttons for cleaned data and removed rows."""
    st.divider()
    st.subheader("Download Results")
    
    # Cache keys for this step's Excel files
    cache_key_cleaned = f"excel_cache_{step_name}_cleaned"
    cache_key_removed = f"excel_cache_{step_name}_removed"
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Cleaned Data**")
        
        # Only generate Excel if not cached
        if cache_key_cleaned not in st.session_state:
            with st.spinner("Preparing Excel file..."):
                st.session_state[cache_key_cleaned] = export_to_excel(cleaned_df)
        
        st.download_button(
            label="📥 Download Excel",
            data=st.session_state[cache_key_cleaned],
            file_name=f"cleaned_{step_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    with col2:
        st.write("**Removed Rows**")
        
        if len(removed_df) > 0:
            # Only generate Excel if not cached
            if cache_key_removed not in st.session_state:
                with st.spinner("Preparing Excel file..."):
                    st.session_state[cache_key_removed] = export_removed_rows_to_excel(removed_df, column_mapping)
            
            st.download_button(
                label="📥 Download Excel",
                data=st.session_state[cache_key_removed],
                file_name=f"removed_{step_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.write("No rows were removed.")



if __name__ == "__main__":
    main()
