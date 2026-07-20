"""Pipeline orchestration for the one-click ("Fast") workflows.

This module is a PURE extraction of the stage sequences that the existing
single-file and multi-file Streamlit workflows run inline in ``app.py``. It
contains no Streamlit code so it can be unit-tested directly and reused by the
fast render functions.

Each stage runner reuses the unchanged pure functions in ``cleaning.py`` and
``matching.py`` in the exact same order, with the exact same ``_removal_reason``
codes, that the classic workflows use — so fast-mode output matches the
step-by-step workflows byte-for-byte.

A stage is skipped when its input (a suppression/billing file, or a non-empty
bad-states set) is not provided. Skipped stages are reported so the UI can show
what ran and what was skipped.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

import pandas as pd

from models import ColumnMapping, StepResult
from cleaning import (
    remove_highlighted_rows, filter_invalid_last_names,
    filter_invalid_phones, filter_empty_phones, filter_invalid_emails,
    filter_test_entries, filter_placeholder_emails, filter_prohibited_content,
    remove_duplicate_phones, filter_invalid_uuid,
    filter_fake_emails, dedupe_against_files, filter_by_bad_states,
)
from matching import (
    load_tcpa_phones, load_tcpa_zipcodes, load_ld_dnc,
    filter_by_area_code, filter_by_name_match, filter_by_dnc_phones,
    filter_by_tcpa_phones, filter_by_tcpa_zips,
)


# Stage labels (also used as the human-facing names in the fast UI stepper).
STAGE_CLEAN = "Clean Bad Data"
STAGE_DNC = "TCPA DNC"
STAGE_ZIP = "Zip Code Removal"
STAGE_PHONES = "Phone Number Removal"
STAGE_MASTER = "Master Phone Suppression"
STAGE_CROSSFILE = "Cross-File Dedupe"
STAGE_BAD_STATES = "Bad States"
STAGE_BILLING = "Clean against Billing"

DEFAULT_BAD_STATES = {"AZ", "DE", "TX"}

# State column resolution order, matching app._resolve_state_column.
_STATE_COLUMN_CANDIDATES = ["State", "STATE", "StateCode", "state", "St"]


@dataclass
class PipelineConfig:
    """Optional inputs for the pipeline. Any ``None``/empty entry skips its stage."""
    dnc_df: Optional[pd.DataFrame] = None
    zips_df: Optional[pd.DataFrame] = None
    phones_df: Optional[pd.DataFrame] = None
    master_phone_set: Optional[Set[str]] = None
    billing_dfs: Optional[List[pd.DataFrame]] = None
    bad_states_set: Optional[Set[str]] = None


@dataclass
class StageRun:
    """Per-stage outcome for display."""
    label: str
    ran: bool
    before_count: int = 0
    after_count: int = 0
    removed_count: int = 0
    removal_summary: Dict[str, int] = field(default_factory=dict)
    skip_reason: str = ""


@dataclass
class SinglePipelineResult:
    cleaned_df: pd.DataFrame
    removed_df: pd.DataFrame  # combined, with a _removal_reason column
    original_count: int
    final_count: int
    stages: List[StageRun]
    combined_summary: Dict[str, int]  # reason_code -> count, across every stage


@dataclass
class FilePipelineResult:
    filename: str
    cleaned_df: pd.DataFrame
    removed_df: pd.DataFrame  # combined, with a _removal_reason column
    original_count: int
    final_count: int


@dataclass
class MultiPipelineResult:
    files: List[FilePipelineResult]
    stages: List[StageRun]
    combined_summary: Dict[str, int]


def resolve_state_column(df: pd.DataFrame, mapping: ColumnMapping) -> Optional[str]:
    """Return the State column name from df, using the mapping or common names."""
    if mapping and mapping.state and mapping.state in df.columns:
        return mapping.state
    for name in _STATE_COLUMN_CANDIDATES:
        if name in df.columns:
            return name
    return None


def _tag(removed_df: pd.DataFrame, reason: str) -> pd.DataFrame:
    """Return a copy of removed rows tagged with the given reason code."""
    tagged = removed_df.copy()
    tagged["_removal_reason"] = reason
    return tagged


def _build_step_result(
    df: pd.DataFrame,
    before_count: int,
    all_removed: List[pd.DataFrame],
    removal_summary: Dict[str, int],
) -> StepResult:
    removed_df = (
        pd.concat(all_removed, ignore_index=True) if all_removed else pd.DataFrame()
    )
    return StepResult(
        cleaned_df=df,
        all_removed_df=removed_df,
        before_count=before_count,
        after_count=len(df),
        removal_summary=removal_summary,
    )


# --------------------------------------------------------------------------- #
# Stage runners — each returns a StepResult for a single DataFrame.
# --------------------------------------------------------------------------- #

def run_clean_bad_data(
    df: pd.DataFrame,
    mapping: ColumnMapping,
    highlighted_cells: Optional[Set] = None,
) -> StepResult:
    """Run the 'Clean Bad Data' stage (highlights + the standard filters).

    Mirrors the order in app.render_step2_clean. ``highlighted_cells`` should be
    detected once by the caller via ``file_io.read_excel_with_highlights`` and is
    applied first (it is row-index based, so it must run before any row filtering
    and while the frame still has its original 0-based index).
    """
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}

    def apply(func, reason: str) -> None:
        nonlocal df
        result = func(df)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, reason))
            removal_summary[reason] = removal_summary.get(reason, 0) + result.removed_count

    if highlighted_cells:
        apply(lambda d: remove_highlighted_rows(d, highlighted_cells), "highlighted_cells")
    apply(lambda d: filter_invalid_last_names(d, mapping.last_name), "invalid_last_name")
    apply(lambda d: filter_empty_phones(d, mapping.phone), "empty_phone")
    apply(lambda d: filter_invalid_phones(d, mapping.phone), "invalid_phone")
    if mapping.email:
        apply(lambda d: filter_invalid_emails(d, mapping.email), "invalid_email")
    apply(lambda d: filter_test_entries(d, mapping.first_name, mapping.last_name), "contains_test")
    if mapping.email:
        apply(lambda d: filter_placeholder_emails(d, mapping.email), "placeholder_email")
        apply(lambda d: filter_fake_emails(d, mapping.email), "fake_email")
    apply(lambda d: filter_prohibited_content(d), "prohibited_content")
    if mapping.lead_id:
        apply(lambda d: filter_invalid_uuid(d, mapping.lead_id), "invalid_uuid")

    return _build_step_result(df, before_count, all_removed, removal_summary)


def run_dnc(df: pd.DataFrame, mapping: ColumnMapping, dnc_df: pd.DataFrame) -> StepResult:
    """Run the 'TCPA DNC' stage: DNC phones, blocked area codes, and name matches."""
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}
    dnc_phones, dnc_area_codes, dnc_names = load_ld_dnc(dnc_df)

    def apply(func, reason: str) -> None:
        nonlocal df
        result = func(df)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, reason))
            removal_summary[reason] = removal_summary.get(reason, 0) + result.removed_count

    if mapping.phone and dnc_phones:
        apply(lambda d: filter_by_dnc_phones(d, mapping.phone, dnc_phones), "dnc_phone_match")
    if mapping.phone and dnc_area_codes:
        apply(lambda d: filter_by_area_code(d, mapping.phone, dnc_area_codes), "dnc_area_code")
    if mapping.first_name and mapping.last_name and dnc_names:
        apply(
            lambda d: filter_by_name_match(d, mapping.first_name, mapping.last_name, dnc_names),
            "dnc_name_match",
        )

    return _build_step_result(df, before_count, all_removed, removal_summary)


def run_zip(df: pd.DataFrame, mapping: ColumnMapping, zips_df: pd.DataFrame) -> StepResult:
    """Run the 'Zip Code Removal' stage."""
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}
    tcpa_zips = load_tcpa_zipcodes(zips_df)

    if mapping.zip_code and tcpa_zips:
        result = filter_by_tcpa_zips(df, mapping.zip_code, tcpa_zips)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, "tcpa_zip_match"))
            removal_summary["tcpa_zip_match"] = result.removed_count

    return _build_step_result(df, before_count, all_removed, removal_summary)


def run_phones(df: pd.DataFrame, mapping: ColumnMapping, phones_df: pd.DataFrame) -> StepResult:
    """Run the 'Phone Number Removal' stage: TCPA phone match, then in-file dedupe."""
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}
    tcpa_phones = load_tcpa_phones(phones_df)

    def apply(func, reason: str) -> None:
        nonlocal df
        result = func(df)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, reason))
            removal_summary[reason] = removal_summary.get(reason, 0) + result.removed_count

    if mapping.phone and tcpa_phones:
        apply(lambda d: filter_by_tcpa_phones(d, mapping.phone, tcpa_phones), "tcpa_phone_match")
    if mapping.phone:
        apply(lambda d: remove_duplicate_phones(d, mapping.phone), "duplicate_phone")

    return _build_step_result(df, before_count, all_removed, removal_summary)


def run_master(df: pd.DataFrame, mapping: ColumnMapping, master_phone_set: Set[str]) -> StepResult:
    """Run the 'Master Phone Suppression' stage against a pre-extracted phone set."""
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}
    phone_col = mapping.phone or "Phone1"

    if phone_col and master_phone_set:
        result = filter_by_tcpa_phones(df, phone_col, master_phone_set)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, "master_phone_match"))
            removal_summary["master_phone_match"] = result.removed_count

    return _build_step_result(df, before_count, all_removed, removal_summary)


def run_bad_states(df: pd.DataFrame, mapping: ColumnMapping, bad_states: Set[str]) -> StepResult:
    """Run the 'Bad States' stage."""
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}
    state_col = resolve_state_column(df, mapping)

    if bad_states and state_col:
        result = filter_by_bad_states(df, state_col, bad_states)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, "bad_states"))
            removal_summary["bad_states"] = result.removed_count

    return _build_step_result(df, before_count, all_removed, removal_summary)


def run_billing(df: pd.DataFrame, mapping: ColumnMapping, billing_dfs: List[pd.DataFrame]) -> StepResult:
    """Run the 'Clean against Billing' stage against one or more billing files."""
    before_count = len(df)
    all_removed: List[pd.DataFrame] = []
    removal_summary: Dict[str, int] = {}
    phone_col = mapping.phone or "Phone1"

    refs = [d for d in (billing_dfs or []) if d is not None]
    if refs:
        result = dedupe_against_files(df, refs, phone_col)
        df = result.cleaned_df
        if result.removed_count > 0:
            all_removed.append(_tag(result.removed_df, "billing_dedupe"))
            removal_summary["billing_dedupe"] = result.removed_count

    return _build_step_result(df, before_count, all_removed, removal_summary)


# --------------------------------------------------------------------------- #
# Helpers for merging stage results.
# --------------------------------------------------------------------------- #

def _merge_summary(target: Dict[str, int], source: Dict[str, int]) -> None:
    for reason, count in source.items():
        target[reason] = target.get(reason, 0) + count


# --------------------------------------------------------------------------- #
# Orchestrators.
# --------------------------------------------------------------------------- #

def run_full_pipeline_single(
    df: pd.DataFrame,
    mapping: ColumnMapping,
    cfg: PipelineConfig,
    highlighted_cells: Optional[Set] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> SinglePipelineResult:
    """Run the full single-file pipeline, skipping stages with no input.

    Order: Clean Bad Data -> DNC -> Zip -> Phones -> Master -> Bad States ->
    Billing. There is no cross-file dedupe in single-file mode.
    """
    original_count = len(df)
    current = df
    removed_parts: List[pd.DataFrame] = []
    combined_summary: Dict[str, int] = {}
    stages: List[StageRun] = []

    def do_stage(label: str, enabled: bool, runner, skip_reason: str = "No file provided"):
        nonlocal current
        if not enabled:
            stages.append(StageRun(label=label, ran=False, skip_reason=skip_reason))
            return
        if progress:
            progress(label)
        before = len(current)
        step = runner(current)
        current = step.cleaned_df
        if len(step.all_removed_df) > 0:
            removed_parts.append(step.all_removed_df)
        _merge_summary(combined_summary, step.removal_summary)
        stages.append(
            StageRun(
                label=label,
                ran=True,
                before_count=before,
                after_count=len(current),
                removed_count=before - len(current),
                removal_summary=step.removal_summary,
            )
        )

    do_stage(STAGE_CLEAN, True, lambda d: run_clean_bad_data(d, mapping, highlighted_cells))
    do_stage(STAGE_DNC, cfg.dnc_df is not None, lambda d: run_dnc(d, mapping, cfg.dnc_df))
    do_stage(STAGE_ZIP, cfg.zips_df is not None, lambda d: run_zip(d, mapping, cfg.zips_df))
    do_stage(STAGE_PHONES, cfg.phones_df is not None, lambda d: run_phones(d, mapping, cfg.phones_df))
    do_stage(STAGE_MASTER, bool(cfg.master_phone_set), lambda d: run_master(d, mapping, cfg.master_phone_set))
    do_stage(
        STAGE_BAD_STATES,
        bool(cfg.bad_states_set),
        lambda d: run_bad_states(d, mapping, cfg.bad_states_set),
        skip_reason="No bad states selected",
    )
    do_stage(STAGE_BILLING, bool(cfg.billing_dfs), lambda d: run_billing(d, mapping, cfg.billing_dfs))

    combined_removed = (
        pd.concat(removed_parts, ignore_index=True) if removed_parts else pd.DataFrame()
    )
    return SinglePipelineResult(
        cleaned_df=current,
        removed_df=combined_removed,
        original_count=original_count,
        final_count=len(current),
        stages=stages,
        combined_summary=combined_summary,
    )


def run_full_pipeline_multi(
    file_dfs: List[pd.DataFrame],
    filenames: List[str],
    mapping: ColumnMapping,
    cfg: PipelineConfig,
    highlighted_cells_file1: Optional[Set] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> MultiPipelineResult:
    """Run the full multi-file pipeline over files ordered newest -> oldest.

    Order: per-file Clean -> DNC -> Zip -> Phones -> Master, then cross-file
    dedupe (File 1 keeps all rows; File N deduped against Files 1..N-1), then
    per-file Bad States, then Billing on File 1 only. Skips stages with no input.
    """
    n = len(file_dfs)
    files = [
        {
            "filename": filenames[i] if i < len(filenames) else f"File{i + 1}",
            "df": file_dfs[i],
            "removed": [],
            "original": len(file_dfs[i]),
        }
        for i in range(n)
    ]
    combined_summary: Dict[str, int] = {}
    stages: List[StageRun] = []

    def fold(fstate: dict, step: StepResult) -> None:
        fstate["df"] = step.cleaned_df
        if len(step.all_removed_df) > 0:
            fstate["removed"].append(step.all_removed_df)

    def per_file_stage(label: str, enabled: bool, runner_for_index, skip_reason: str = "No file provided"):
        if not enabled:
            stages.append(StageRun(label=label, ran=False, skip_reason=skip_reason))
            return
        if progress:
            progress(label)
        before_total = sum(len(f["df"]) for f in files)
        stage_summary: Dict[str, int] = {}
        for i, fstate in enumerate(files):
            step = runner_for_index(i, fstate["df"])
            fold(fstate, step)
            _merge_summary(stage_summary, step.removal_summary)
        _merge_summary(combined_summary, stage_summary)
        after_total = sum(len(f["df"]) for f in files)
        stages.append(
            StageRun(
                label=label,
                ran=True,
                before_count=before_total,
                after_count=after_total,
                removed_count=before_total - after_total,
                removal_summary=stage_summary,
            )
        )

    # Per-file stages.
    per_file_stage(
        STAGE_CLEAN, True,
        lambda i, d: run_clean_bad_data(d, mapping, highlighted_cells_file1 if i == 0 else None),
    )
    per_file_stage(STAGE_DNC, cfg.dnc_df is not None, lambda i, d: run_dnc(d, mapping, cfg.dnc_df))
    per_file_stage(STAGE_ZIP, cfg.zips_df is not None, lambda i, d: run_zip(d, mapping, cfg.zips_df))
    per_file_stage(STAGE_PHONES, cfg.phones_df is not None, lambda i, d: run_phones(d, mapping, cfg.phones_df))
    per_file_stage(STAGE_MASTER, bool(cfg.master_phone_set), lambda i, d: run_master(d, mapping, cfg.master_phone_set))

    # Cross-file dedupe: File 1 keeps all; File N removes phones present in Files 1..N-1.
    if n >= 2:
        if progress:
            progress(STAGE_CROSSFILE)
        before_total = sum(len(f["df"]) for f in files)
        stage_summary: Dict[str, int] = {}
        phone_col = mapping.phone or "Phone1"
        for i in range(1, n):
            reference_dfs = [files[j]["df"] for j in range(i)]
            result = dedupe_against_files(files[i]["df"], reference_dfs, phone_col)
            files[i]["df"] = result.cleaned_df
            if result.removed_count > 0:
                files[i]["removed"].append(_tag(result.removed_df, "crossfile_dedupe"))
                stage_summary["crossfile_dedupe"] = (
                    stage_summary.get("crossfile_dedupe", 0) + result.removed_count
                )
        _merge_summary(combined_summary, stage_summary)
        after_total = sum(len(f["df"]) for f in files)
        stages.append(
            StageRun(
                label=STAGE_CROSSFILE, ran=True, before_count=before_total,
                after_count=after_total, removed_count=before_total - after_total,
                removal_summary=stage_summary,
            )
        )
    else:
        stages.append(StageRun(label=STAGE_CROSSFILE, ran=False, skip_reason="Needs 2+ files"))

    per_file_stage(
        STAGE_BAD_STATES,
        bool(cfg.bad_states_set),
        lambda i, d: run_bad_states(d, mapping, cfg.bad_states_set),
        skip_reason="No bad states selected",
    )

    # Billing applies to File 1 only (matches the classic multi-file workflow).
    if cfg.billing_dfs:
        if progress:
            progress(STAGE_BILLING)
        before = len(files[0]["df"])
        step = run_billing(files[0]["df"], mapping, cfg.billing_dfs)
        fold(files[0], step)
        _merge_summary(combined_summary, step.removal_summary)
        stages.append(
            StageRun(
                label=STAGE_BILLING, ran=True, before_count=before,
                after_count=len(files[0]["df"]), removed_count=before - len(files[0]["df"]),
                removal_summary=step.removal_summary,
            )
        )
    else:
        stages.append(StageRun(label=STAGE_BILLING, ran=False, skip_reason="No billing files"))

    file_results = []
    for fstate in files:
        removed_df = (
            pd.concat(fstate["removed"], ignore_index=True)
            if fstate["removed"] else pd.DataFrame()
        )
        file_results.append(
            FilePipelineResult(
                filename=fstate["filename"],
                cleaned_df=fstate["df"],
                removed_df=removed_df,
                original_count=fstate["original"],
                final_count=len(fstate["df"]),
            )
        )

    return MultiPipelineResult(
        files=file_results,
        stages=stages,
        combined_summary=combined_summary,
    )
