"""Data cleaning module for refinance data cleansing."""

from __future__ import annotations
import re
from typing import Optional, Union, Set, Tuple, List
import pandas as pd
from models import CleanResult


# Required columns for the refinance data
REQUIRED_COLUMNS = [
    'DateReceived',
    'FirstName',
    'LastName',
    'Email',
    'Phone1',
    'StreetAddress',
    'City',
    'State',
    'ZipCode',
    'DesiredLoanAmount',
    'FirstMortgageBalance',
    'ExistingPropertyValue',
    'Universal_LeadId'
]


def validate_required_columns(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate that all required columns exist in the DataFrame.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, missing_columns)
    """
    existing_cols = set(df.columns)
    required_set = set(REQUIRED_COLUMNS)
    missing = required_set - existing_cols
    return len(missing) == 0, sorted(list(missing))


def filter_to_required_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Filter DataFrame to only include required columns.
    
    Args:
        df: DataFrame to filter
        
    Returns:
        Tuple of (filtered_df, dropped_columns)
    """
    existing_cols = set(df.columns)
    required_set = set(REQUIRED_COLUMNS)
    
    # Find columns to keep (intersection of existing and required)
    cols_to_keep = [col for col in REQUIRED_COLUMNS if col in existing_cols]
    
    # Find columns that will be dropped
    dropped_cols = sorted([col for col in existing_cols if col not in required_set])
    
    return df[cols_to_keep].copy(), dropped_cols


def normalize_phone(phone: Optional[Union[str, float, int]]) -> str:
    """Normalize phone to digits only.
    
    Args:
        phone: Phone number in any format (string, float, int)
        
    Returns:
        String containing only digits
    """
    if phone is None or (isinstance(phone, float) and pd.isna(phone)):
        return ''
    
    # Convert to string and extract digits only
    phone_str = str(phone)
    # Handle scientific notation (e.g., 4.056133e+09)
    if 'e' in phone_str.lower() or isinstance(phone, float):
        try:
            phone_str = str(int(float(phone)))
        except (ValueError, OverflowError):
            pass
    
    return re.sub(r'\D', '', phone_str)



def remove_highlighted_rows(df: pd.DataFrame, highlighted_cells: Set[Tuple[int, int]]) -> CleanResult:
    """Remove rows where any cell is highlighted.
    
    Args:
        df: DataFrame to filter
        highlighted_cells: Set of (row_index, col_index) tuples for highlighted cells
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    if not highlighted_cells:
        return CleanResult(
            cleaned_df=df.copy(),
            removed_df=pd.DataFrame(columns=df.columns),
            removed_count=0,
            reason="highlighted_cells"
        )
    
    # Get unique row indices that have highlighted cells
    highlighted_rows = {row_idx for row_idx, _ in highlighted_cells}
    
    # Create mask for rows to keep
    keep_mask = ~df.index.isin(highlighted_rows)
    
    cleaned_df = df[keep_mask].copy()
    removed_df = df[~keep_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="highlighted_cells"
    )



def is_valid_last_name(value) -> bool:
    """Check if last name is valid.
    
    Valid if:
    - Not empty/whitespace-only
    - Not boolean TRUE (actual boolean type, not string)
    - Starts with a letter (A-Z, a-z)
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    
    # Check for boolean TRUE (actual boolean type only)
    if isinstance(value, bool) and value is True:
        return False
    
    # Convert to string and check
    str_value = str(value).strip()
    
    # Check for empty/whitespace
    if not str_value:
        return False
    
    # Check if starts with letter
    return str_value[0].isalpha()


def filter_invalid_last_names(df: pd.DataFrame, last_name_col: str) -> CleanResult:
    """Remove rows where last name is invalid.
    
    Invalid if:
    - First character is not a letter (A-Z, a-z)
    - Value is empty/whitespace-only
    - Value is boolean TRUE
    
    Args:
        df: DataFrame to filter
        last_name_col: Name of the last name column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    valid_mask = df[last_name_col].apply(is_valid_last_name)
    
    cleaned_df = df[valid_mask].copy()
    removed_df = df[~valid_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="invalid_last_name"
    )



def is_valid_phone(phone_value) -> bool:
    """Check if phone number is valid.
    
    Valid if:
    - Normalized phone is exactly 10 digits
    - Does not start with digit 1
    """
    normalized = normalize_phone(phone_value)
    
    if len(normalized) != 10:
        return False
    
    if normalized.startswith('1'):
        return False
    
    return True


def filter_invalid_phones(df: pd.DataFrame, phone_col: str) -> CleanResult:
    """Remove rows with invalid phone numbers.
    
    Invalid if:
    - Normalized phone is not exactly 10 digits
    - Phone starts with digit 1
    
    Args:
        df: DataFrame to filter
        phone_col: Name of the phone column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    valid_mask = df[phone_col].apply(is_valid_phone)
    
    cleaned_df = df[valid_mask].copy()
    removed_df = df[~valid_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="invalid_phone"
    )


def filter_empty_phones(df: pd.DataFrame, phone_col: str) -> CleanResult:
    """Remove rows with empty/missing phone numbers.
    
    Args:
        df: DataFrame to filter
        phone_col: Name of the phone column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    def is_empty_phone(value) -> bool:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return True
        normalized = normalize_phone(value)
        return len(normalized) == 0
    
    empty_mask = df[phone_col].apply(is_empty_phone)
    
    cleaned_df = df[~empty_mask].copy()
    removed_df = df[empty_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="empty_phone"
    )



def is_valid_email(email_value) -> bool:
    """Check if email is valid.
    
    Valid if:
    - Not empty
    - Contains exactly one @ symbol
    - Has characters before and after @
    """
    if email_value is None or (isinstance(email_value, float) and pd.isna(email_value)):
        return False
    
    email_str = str(email_value).strip()
    
    if not email_str:
        return False
    
    # Check for exactly one @
    if email_str.count('@') != 1:
        return False
    
    # Check for characters before and after @
    parts = email_str.split('@')
    if len(parts[0]) == 0 or len(parts[1]) == 0:
        return False
    
    return True


def filter_invalid_emails(df: pd.DataFrame, email_col: str) -> CleanResult:
    """Remove rows with invalid or empty emails.
    
    Invalid if:
    - Empty/missing
    - Does not contain exactly one @ symbol
    - No characters before or after @
    
    Args:
        df: DataFrame to filter
        email_col: Name of the email column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    valid_mask = df[email_col].apply(is_valid_email)
    
    cleaned_df = df[valid_mask].copy()
    removed_df = df[~valid_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="invalid_email"
    )


# Fake/invalid email detection patterns
FAKE_EMAIL_LOCAL_PARTS = {
    'none', 'noemail', 'no', 'nope', 'nothanks', 'nothing', 'noway', 'nowmail',
    'test', 'testing', 'asdf', 'asdfa', 'asdfasdf', 'qwerty', 'abc', 'xyz',
    'fake', 'fakeemail', 'noemail', 'nomail', 'notgiving', 'notgonna',
    'johndoe', 'janedoe', 'john', 'jane', 'example', 'sample', 'demo',
    'null', 'void', 'blank', 'empty', 'unknown', 'anonymous', 'anon',
    'temp', 'temporary', 'disposable', 'throwaway', 'trash',
    'notapplicable', 'na', 'n/a', 'nada', 'nil', 'noone', 'nobody',
    'nomorehackingallowed', 'notgoingtotellyou', 'notsay', 'notready',
}

FAKE_EMAIL_LOCAL_PREFIXES = [
    'none', 'noemail', 'no@', 'nope', 'not@', 'not.', 'nothanks', 'nothing',
    'noway', 'test', 'asdf', 'qwer', 'fake', 'johndoe', 'janedoe',
    'nomail', 'notgiving', 'notgonna', 'utest', 'testtest',
]

FAKE_EMAIL_DOMAINS = {
    'noemail.com', 'nomail.com', 'none.com', 'nope.com', 'fake.com',
    'fakeemail.com', 'nothanks.com', 'thanks.com', 'noway.com',
    'nonya.com', 'nospampls.com', 'nospam.com', 'comfortable.com',
    'happening.com', 'ing.com', 'example.com', 'test.com', 'testing.com',
    'mailinator.com', 'guerrillamail.com', 'tempmail.com', 'throwaway.com',
    'trashmail.com', 'sharklasers.com', 'spam4.me', 'grr.la',
    'dispostable.com', 'maildrop.cc', 'getairmail.com', 'yopmail.com',
}

# Gibberish patterns (random characters)
GIBBERISH_PATTERNS = [
    r'^[a-z]{1,4}@',  # Very short random letters before @
    r'^[asdfjkl;]+@',  # Home row keyboard mashing
    r'^[qwertyuiop]+@',  # Top row keyboard mashing
    r'^[zxcvbnm]+@',  # Bottom row keyboard mashing
    r'asdf',  # Common keyboard pattern
    r'qwer',  # Common keyboard pattern
    r'zxcv',  # Common keyboard pattern
    r'^[a-z]{2,3}\d+@',  # 2-3 letters followed by numbers (like ab123@)
]


def is_fake_email(email_value) -> bool:
    """Check if email appears to be fake, placeholder, or invalid.
    
    Detects:
    - Placeholder local parts (none, noemail, test, asdf, etc.)
    - Fake/disposable domains
    - Gibberish patterns
    - Emails starting with special characters
    - Refusal patterns (not@, noway@, etc.)
    
    Returns:
        True if email appears fake, False if it seems legitimate
    """
    if email_value is None or (isinstance(email_value, float) and pd.isna(email_value)):
        return True
    
    email_str = str(email_value).strip().lower()
    
    if not email_str or '@' not in email_str:
        return True
    
    # Check for emails starting with special characters
    if email_str[0] in '?!.@#$%^&*()_+-=[]{}|;:\'",<>/\\':
        return True
    
    parts = email_str.split('@')
    if len(parts) != 2:
        return True
    
    local_part, domain = parts
    
    # Check for empty parts
    if not local_part or not domain:
        return True
    
    # Check for fake local parts (exact match)
    local_clean = re.sub(r'[^a-z]', '', local_part)  # Remove numbers/special chars for comparison
    if local_clean in FAKE_EMAIL_LOCAL_PARTS:
        return True
    
    # Check for fake local part prefixes
    for prefix in FAKE_EMAIL_LOCAL_PREFIXES:
        if local_part.startswith(prefix):
            return True
    
    # Check for fake domains
    if domain in FAKE_EMAIL_DOMAINS:
        return True
    
    # Check for domain ending in .con instead of .com
    if domain.endswith('.con'):
        return True
    
    # Check for gibberish patterns
    for pattern in GIBBERISH_PATTERNS:
        if re.search(pattern, email_str):
            # Additional check: if it matches gibberish but is a real-looking email, skip
            # Only flag if the local part is very short or clearly random
            if len(local_part) <= 4 and re.match(r'^[a-z]+$', local_part):
                return True
    
    # Check for "refusal" patterns in local part
    refusal_patterns = [
        r'^not[._]?[a-z]*@',  # not@, not.ready@, notgiving@
        r'^no[._]?[a-z]*@',   # no@, no.way@
        r'^nope',
        r'^noway',
        r'^nothanks',
        r'^nothing',
        r'^fake',
        r'^test[0-9]*@',      # test@, test123@
        r'^utest',            # utest@
    ]
    for pattern in refusal_patterns:
        if re.match(pattern, email_str):
            return True
    
    # Check for johndoe/janedoe patterns
    if 'johndoe' in local_part or 'janedoe' in local_part:
        return True
    
    # Check for domain that looks like a refusal
    refusal_domains = ['happening', 'comfortable', 'nonya', 'nospam', 'thanks']
    domain_name = domain.split('.')[0] if '.' in domain else domain
    if domain_name in refusal_domains:
        return True
    
    return False


def filter_fake_emails(df: pd.DataFrame, email_col: str) -> CleanResult:
    """Remove rows with fake, placeholder, or suspicious emails.
    
    Detects and removes:
    - Placeholder emails (none@, noemail@, test@, asdf@, etc.)
    - Fake/disposable domains
    - Gibberish patterns
    - Emails starting with special characters
    - Refusal patterns (not@happening.com, noway@gmail.com, etc.)
    
    Args:
        df: DataFrame to filter
        email_col: Name of the email column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    fake_mask = df[email_col].apply(is_fake_email)
    
    cleaned_df = df[~fake_mask].copy()
    removed_df = df[fake_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="fake_email"
    )



# Step 2 Functions (some moved to Step 1 in app.py)

def filter_test_entries(df: pd.DataFrame, first_name_col: str = None, last_name_col: str = None) -> CleanResult:
    """Remove rows containing 'TEST' in first or last name fields.
    
    Args:
        df: DataFrame to filter
        first_name_col: Name of the first name column (optional)
        last_name_col: Name of the last name column (optional)
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    def contains_test(row) -> bool:
        cols_to_check = []
        if first_name_col and first_name_col in row.index:
            cols_to_check.append(first_name_col)
        if last_name_col and last_name_col in row.index:
            cols_to_check.append(last_name_col)
        
        for col in cols_to_check:
            val = row[col]
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                if 'test' in str(val).lower():
                    return True
        return False
    
    test_mask = df.apply(contains_test, axis=1)
    
    cleaned_df = df[~test_mask].copy()
    removed_df = df[test_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="contains_test"
    )



PLACEHOLDER_EMAILS = {'n/a', 'no', 'nada', 'na', 'noemail', 'none'}


def filter_placeholder_emails(df: pd.DataFrame, email_col: str) -> CleanResult:
    """Remove rows with placeholder emails.
    
    Placeholder emails: N/A, No, Nada, Na, NoEmail, None (case-insensitive)
    
    Args:
        df: DataFrame to filter
        email_col: Name of the email column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    def is_placeholder(email_val) -> bool:
        if email_val is None or (isinstance(email_val, float) and pd.isna(email_val)):
            return False
        normalized = str(email_val).strip().lower()
        return normalized in PLACEHOLDER_EMAILS
    
    placeholder_mask = df[email_col].apply(is_placeholder)
    
    cleaned_df = df[~placeholder_mask].copy()
    removed_df = df[placeholder_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="placeholder_email"
    )



PROHIBITED_TERMS = ['loan depot', 'fuck']


def filter_prohibited_content(df: pd.DataFrame) -> CleanResult:
    """Remove rows containing prohibited content.
    
    Prohibited: 'loan depot', 'fuck' (case-insensitive)
    
    Args:
        df: DataFrame to filter
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    def contains_prohibited(row) -> bool:
        for val in row:
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                val_lower = str(val).lower()
                for term in PROHIBITED_TERMS:
                    if term in val_lower:
                        return True
        return False
    
    prohibited_mask = df.apply(contains_prohibited, axis=1)
    
    cleaned_df = df[~prohibited_mask].copy()
    removed_df = df[prohibited_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="prohibited_content"
    )



def remove_duplicate_phones(df: pd.DataFrame, phone_col: str) -> CleanResult:
    """Remove duplicate phone numbers, keeping one random row per unique phone.
    
    Args:
        df: DataFrame to filter
        phone_col: Name of the phone column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    # Add normalized phone column for deduplication
    df = df.copy()
    df['_normalized_phone'] = df[phone_col].apply(normalize_phone)
    
    # Shuffle to randomize which row is kept
    df_shuffled = df.sample(frac=1, random_state=None).reset_index(drop=True)
    
    # Keep first occurrence of each phone (random due to shuffle)
    keep_mask = ~df_shuffled.duplicated(subset=['_normalized_phone'], keep='first')
    
    cleaned_df = df_shuffled[keep_mask].drop(columns=['_normalized_phone']).copy()
    removed_df = df_shuffled[~keep_mask].drop(columns=['_normalized_phone']).copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="duplicate_phone"
    )



UUID_PATTERN = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')


def is_valid_uuid(uuid_val) -> bool:
    """Check if value matches UUID format (8-4-4-4-12 hex pattern)."""
    if uuid_val is None or (isinstance(uuid_val, float) and pd.isna(uuid_val)):
        return False
    return bool(UUID_PATTERN.match(str(uuid_val).strip()))


def filter_invalid_uuid(df: pd.DataFrame, uuid_col: str) -> CleanResult:
    """Remove rows where UUID doesn't match 8-4-4-4-12 format.
    
    Args:
        df: DataFrame to filter
        uuid_col: Name of the UUID column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    valid_mask = df[uuid_col].apply(is_valid_uuid)
    
    cleaned_df = df[valid_mask].copy()
    removed_df = df[~valid_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="invalid_uuid"
    )



def dedupe_against_files(target_df: pd.DataFrame, reference_dfs: List[pd.DataFrame], phone_col: str) -> CleanResult:
    """Remove rows from target where phone exists in any reference DataFrame.
    
    This function removes duplicate phone numbers from the target DataFrame
    by checking against one or more reference DataFrames. A row is removed
    if its normalized phone number exists in any of the reference files.
    
    Args:
        target_df: DataFrame to deduplicate (rows will be removed from this)
        reference_dfs: List of DataFrames to check against (phones in these are kept)
        phone_col: Name of the phone column
        
    Returns:
        CleanResult with cleaned and removed DataFrames
    """
    if not reference_dfs:
        return CleanResult(
            cleaned_df=target_df.copy(),
            removed_df=pd.DataFrame(columns=target_df.columns),
            removed_count=0,
            reason="crossfile_dedupe"
        )
    
    # Build set of all normalized phones from reference files
    reference_phones = set()
    for ref_df in reference_dfs:
        if ref_df is not None and phone_col in ref_df.columns:
            phones = ref_df[phone_col].apply(normalize_phone)
            reference_phones.update(phones[phones != ''])
    
    # Normalize target phones and check for matches
    target_normalized = target_df[phone_col].apply(normalize_phone)
    
    # Keep rows where phone is NOT in reference files
    keep_mask = ~target_normalized.isin(reference_phones)
    
    cleaned_df = target_df[keep_mask].copy()
    removed_df = target_df[~keep_mask].copy()
    
    return CleanResult(
        cleaned_df=cleaned_df,
        removed_df=removed_df,
        removed_count=len(removed_df),
        reason="crossfile_dedupe"
    )
