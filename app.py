"""Refinance Data Cleansing - Streamlit Application."""

import streamlit as st
import pandas as pd
import time
from io import BytesIO

from models import ColumnMapping, CleanResult, StepResult
from file_io import (
    read_uploaded_file, read_excel_with_highlights,
    export_to_excel, export_removed_rows_to_excel,
    is_valid_file_format, get_file_extension
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
    filter_by_tcpa_phones, filter_by_tcpa_zips
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
    if 'raw_data' not in st.session_state:
        st.session_state.raw_data = None
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


STEPS = ["1. Upload Raw Data", "2. Clean Bad Data", "3. TCPA DNC File", "4. Zip Code Removal", "5. Phone Number Removal", "6. Cross-File Dedupe"]


def go_to_step(step_name: str):
    """Navigate to a specific step."""
    st.session_state.current_step = step_name


def main():
    """Main entry point - renders the wizard UI."""
    st.set_page_config(page_title="Refinance Data Cleansing", layout="wide")
    st.title("Refinance Data Cleansing")
    
    init_session_state()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    step_index = STEPS.index(st.session_state.current_step) if st.session_state.current_step in STEPS else 0
    step = st.sidebar.radio(
        "Select Step",
        STEPS,
        index=step_index
    )
    st.session_state.current_step = step
    
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
        col1, col2, col3 = st.columns(3)
        col1.metric("Before", result.before_count)
        col2.metric("Removed", result.before_count - result.after_count)
        col3.metric("After", result.after_count)
        
        st.subheader("Removal Summary")
        for reason, count in result.removal_summary.items():
            st.write(f"- {reason}: {count} rows")
        
        st.subheader("Final Cleaned Data Preview")
        st.dataframe(result.cleaned_df.head(25))
        
        render_download_section(result.cleaned_df, result.all_removed_df, "final", st.session_state.column_mapping)
        
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
