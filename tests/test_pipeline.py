"""Unit tests for the fast-workflow pipeline engine (pipeline.py).

These verify that:
- Each stage runner reuses the underlying cleaning/matching logic in the right
  order with the right _removal_reason codes.
- The cleaned + removed partition is exact (no rows lost or duplicated).
- Stages are skipped when their input is missing, and reported as such.
- The single- and multi-file orchestrators chain stages and skip correctly.
- Cross-file dedupe keeps File 1 intact and removes newer-file phones from older
  files (newest -> oldest ordering).
"""

import sys
sys.path.insert(0, '.')

import pandas as pd
import pytest

from models import ColumnMapping
from cleaning import REQUIRED_COLUMNS, normalize_phone
import pipeline
from pipeline import (
    PipelineConfig, DEFAULT_BAD_STATES,
    run_clean_bad_data, run_dnc, run_zip, run_phones, run_master,
    run_bad_states, run_billing, resolve_state_column,
    run_full_pipeline_single, run_full_pipeline_multi,
    STAGE_CLEAN, STAGE_DNC, STAGE_ZIP, STAGE_PHONES, STAGE_MASTER,
    STAGE_CROSSFILE, STAGE_BAD_STATES, STAGE_BILLING,
)


FULL_MAPPING = ColumnMapping(
    phone='Phone1', first_name='FirstName', last_name='LastName',
    email='Email', zip_code='ZipCode', lead_id='Universal_LeadId', state='State',
)

VALID_UUID = "12345678-1234-1234-1234-123456789abc"


def make_row(**overrides):
    """Build one fully-valid data row; override individual fields as needed."""
    row = {
        'DateReceived': '2026-01-01',
        'FirstName': 'John',
        'LastName': 'Smith',
        'Email': 'john@example.org',
        'Phone1': '4155551234',
        'StreetAddress': '1 Main St',
        'City': 'Springfield',
        'State': 'CA',
        'ZipCode': '94016',
        'DesiredLoanAmount': 100000,
        'FirstMortgageBalance': 50000,
        'ExistingPropertyValue': 200000,
        'Universal_LeadId': VALID_UUID,
    }
    row.update(overrides)
    return row


def make_df(rows):
    return pd.DataFrame(rows, columns=REQUIRED_COLUMNS)


def assert_partition(before_df, step_result):
    """cleaned + removed row counts equal the input row count."""
    assert step_result.after_count + len(step_result.all_removed_df) == len(before_df)
    assert len(step_result.cleaned_df) == step_result.after_count


# --------------------------------------------------------------------------- #
# run_clean_bad_data
# --------------------------------------------------------------------------- #

def test_clean_removes_each_bad_row_with_correct_reason():
    df = make_df([
        make_row(FirstName='Ann', LastName='Good', Phone1='4155550001'),       # keep
        make_row(LastName='9bad'),                                              # invalid_last_name
        make_row(Phone1=''),                                                    # empty_phone
        make_row(Phone1='12345'),                                               # invalid_phone
        make_row(Email='not-an-email'),                                         # invalid_email
        make_row(FirstName='TESTER'),                                           # contains_test
        make_row(Email='asdf@asdf.com'),                                        # fake_email
        make_row(City='loan depot office'),                                     # prohibited_content
        make_row(Universal_LeadId='not-a-uuid'),                                # invalid_uuid
    ])
    before = df.copy()
    result = run_clean_bad_data(df, FULL_MAPPING)

    assert result.after_count == 1
    assert_partition(before, result)
    reasons = set(result.all_removed_df['_removal_reason'])
    assert reasons == {
        'invalid_last_name', 'empty_phone', 'invalid_phone', 'invalid_email',
        'contains_test', 'fake_email', 'prohibited_content', 'invalid_uuid',
    }
    assert result.removal_summary['invalid_last_name'] == 1


def test_clean_placeholder_email_is_caught_as_invalid_email_first():
    # Placeholder tokens (na, no, none) have no '@', so filter_invalid_emails
    # removes them before filter_placeholder_emails runs. This documents the
    # existing pipeline ordering (same as the classic app).
    df = make_df([make_row(Phone1='4155550001'), make_row(Email='na')])
    result = run_clean_bad_data(df, FULL_MAPPING)
    assert result.after_count == 1
    assert list(result.all_removed_df['_removal_reason']) == ['invalid_email']


def test_clean_applies_highlighted_cells_first():
    df = make_df([make_row(Phone1='4155550001'), make_row(Phone1='4155550002')])
    before = df.copy()
    # Highlight a cell in row index 0 -> that row must be removed as highlighted_cells.
    result = run_clean_bad_data(df, FULL_MAPPING, highlighted_cells={(0, 0)})
    assert result.after_count == 1
    assert 'highlighted_cells' in set(result.all_removed_df['_removal_reason'])
    assert_partition(before, result)


def test_clean_all_valid_removes_nothing():
    df = make_df([make_row(Phone1='4155550001'), make_row(Phone1='4155550002')])
    result = run_clean_bad_data(df.copy(), FULL_MAPPING)
    assert result.after_count == 2
    assert len(result.all_removed_df) == 0
    assert result.removal_summary == {}


# --------------------------------------------------------------------------- #
# run_dnc
# --------------------------------------------------------------------------- #

def test_dnc_removes_phone_area_and_name_matches():
    df = make_df([
        make_row(Phone1='4155550001'),                                  # keep
        make_row(Phone1='2025550100'),                                  # dnc phone match
        make_row(Phone1='3125550111'),                                  # area code 312 match
        make_row(FirstName='Jane', LastName='Doe', Phone1='4155550002'),  # name match
    ])
    before = df.copy()
    # DNC file: col1 = phones/area codes, col2 = concatenated names.
    dnc_df = pd.DataFrame({
        'value': ['2025550100', '312'],
        'name': ['janedoe', None],
    })
    result = run_dnc(df, FULL_MAPPING, dnc_df)
    assert result.after_count == 1
    assert set(result.all_removed_df['_removal_reason']) == {
        'dnc_phone_match', 'dnc_area_code', 'dnc_name_match',
    }
    assert_partition(before, result)


# --------------------------------------------------------------------------- #
# run_zip / run_phones
# --------------------------------------------------------------------------- #

def test_zip_removes_matching_zipcodes():
    df = make_df([
        make_row(ZipCode='94016'),
        make_row(ZipCode='10001'),
    ])
    zips_df = pd.DataFrame({'zip': ['10001']})
    result = run_zip(df.copy(), FULL_MAPPING, zips_df)
    assert result.after_count == 1
    assert list(result.all_removed_df['_removal_reason']) == ['tcpa_zip_match']


def test_phones_removes_tcpa_matches_and_dedupes():
    df = make_df([
        make_row(Phone1='4155550001'),   # keep
        make_row(Phone1='2025550100'),   # tcpa match -> removed
        make_row(Phone1='4155550002'),   # duplicate pair below
        make_row(Phone1='4155550002'),   # duplicate -> one removed
    ])
    before = df.copy()
    phones_df = pd.DataFrame({'phone': ['2025550100']})
    result = run_phones(df, FULL_MAPPING, phones_df)
    # 1 tcpa removed + 1 duplicate removed -> 2 remain.
    assert result.after_count == 2
    reasons = set(result.all_removed_df['_removal_reason'])
    assert reasons == {'tcpa_phone_match', 'duplicate_phone'}
    assert_partition(before, result)


# --------------------------------------------------------------------------- #
# run_master / run_bad_states / run_billing
# --------------------------------------------------------------------------- #

def test_master_suppresses_phone_set():
    df = make_df([make_row(Phone1='4155550001'), make_row(Phone1='4155550002')])
    result = run_master(df.copy(), FULL_MAPPING, {'4155550002'})
    assert result.after_count == 1
    assert list(result.all_removed_df['_removal_reason']) == ['master_phone_match']


def test_bad_states_default_set_removes_az_de_tx_keeps_missing():
    df = make_df([
        make_row(State='CA'),
        make_row(State='AZ'),
        make_row(State='tx'),   # case-insensitive
        make_row(State=''),     # missing state is kept
    ])
    result = run_bad_states(df.copy(), FULL_MAPPING, DEFAULT_BAD_STATES)
    assert result.after_count == 2
    assert result.removal_summary['bad_states'] == 2


def test_billing_removes_phone_matches_across_files():
    df = make_df([make_row(Phone1='4155550001'), make_row(Phone1='4155550002')])
    billing1 = make_df([make_row(Phone1='4155550002')])
    billing2 = make_df([make_row(Phone1='9998887777')])
    result = run_billing(df.copy(), FULL_MAPPING, [billing1, billing2])
    assert result.after_count == 1
    assert list(result.all_removed_df['_removal_reason']) == ['billing_dedupe']


def test_resolve_state_column_fallback():
    df = pd.DataFrame({'STATE': ['CA']})
    mapping = ColumnMapping()  # no state set
    assert resolve_state_column(df, mapping) == 'STATE'
    assert resolve_state_column(pd.DataFrame({'x': [1]}), mapping) is None


# --------------------------------------------------------------------------- #
# Single-file orchestrator
# --------------------------------------------------------------------------- #

def test_single_pipeline_skips_stages_without_input():
    df = make_df([make_row(Phone1='4155550001'), make_row(LastName='9bad')])
    # No suppression/billing files, no bad states -> only Clean runs.
    cfg = PipelineConfig(bad_states_set=set())
    result = run_full_pipeline_single(df, FULL_MAPPING, cfg)

    ran = {s.label: s.ran for s in result.stages}
    assert ran[STAGE_CLEAN] is True
    assert ran[STAGE_DNC] is False
    assert ran[STAGE_ZIP] is False
    assert ran[STAGE_PHONES] is False
    assert ran[STAGE_MASTER] is False
    assert ran[STAGE_BAD_STATES] is False
    assert ran[STAGE_BILLING] is False
    assert result.original_count == 2
    assert result.final_count == 1


def test_single_pipeline_runs_all_stages_and_partitions():
    df = make_df([
        make_row(Phone1='4155550001', State='CA'),   # survivor
        make_row(LastName='9bad'),                    # clean removes
        make_row(Phone1='2025550100', State='CA'),    # dnc phone
        make_row(ZipCode='10001', State='CA'),        # zip
        make_row(Phone1='7185550100', State='CA'),    # tcpa phone
        make_row(Phone1='8005550100', State='CA'),    # master
        make_row(State='AZ'),                          # bad state
        make_row(Phone1='6465550100', State='CA'),     # billing
    ])
    original = len(df)
    cfg = PipelineConfig(
        dnc_df=pd.DataFrame({'value': ['2025550100'], 'name': [None]}),
        zips_df=pd.DataFrame({'zip': ['10001']}),
        phones_df=pd.DataFrame({'phone': ['7185550100']}),
        master_phone_set={'8005550100'},
        billing_dfs=[make_df([make_row(Phone1='6465550100')])],
        bad_states_set=DEFAULT_BAD_STATES,
    )
    result = run_full_pipeline_single(df, FULL_MAPPING, cfg)

    assert all(s.ran for s in result.stages)
    assert result.final_count == 1
    # Combined removed rows + final == original (exact partition across the pipeline).
    assert len(result.removed_df) + result.final_count == original
    # Every stage contributed exactly one removal.
    for reason in ['dnc_phone_match', 'tcpa_zip_match', 'tcpa_phone_match',
                   'master_phone_match', 'bad_states', 'billing_dedupe']:
        assert result.combined_summary.get(reason, 0) == 1


def test_single_pipeline_progress_callback_receives_ran_stages():
    df = make_df([make_row()])
    seen = []
    cfg = PipelineConfig(bad_states_set=DEFAULT_BAD_STATES)
    run_full_pipeline_single(df, FULL_MAPPING, cfg, progress=seen.append)
    # Clean always runs; bad states runs because a set was provided.
    assert STAGE_CLEAN in seen
    assert STAGE_BAD_STATES in seen
    assert STAGE_DNC not in seen  # skipped -> no progress call


# --------------------------------------------------------------------------- #
# Multi-file orchestrator
# --------------------------------------------------------------------------- #

def test_multi_pipeline_crossfile_dedupe_newest_to_oldest():
    # File 1 (newest) keeps all rows. Files 2/3 lose phones present in newer files.
    f1 = make_df([make_row(Phone1='4155550001'), make_row(Phone1='4155550002')])
    f2 = make_df([make_row(Phone1='4155550002'), make_row(Phone1='4155550003')])  # 002 dup of f1
    f3 = make_df([make_row(Phone1='4155550003'), make_row(Phone1='4155550004')])  # 003 dup of f2
    cfg = PipelineConfig(bad_states_set=set())  # only clean + crossfile run
    result = run_full_pipeline_multi(
        [f1, f2, f3], ['newest.xlsx', 'mid.xlsx', 'oldest.xlsx'], FULL_MAPPING, cfg,
    )

    files = result.files
    assert files[0].final_count == 2  # File 1 untouched
    assert files[1].final_count == 1  # 002 removed
    assert files[2].final_count == 1  # 003 removed
    # Cross-file removals are tagged.
    assert 'crossfile_dedupe' in set(files[1].removed_df['_removal_reason'])
    crossfile_stage = next(s for s in result.stages if s.label == STAGE_CROSSFILE)
    assert crossfile_stage.ran is True
    assert crossfile_stage.removed_count == 2


def test_multi_pipeline_single_file_skips_crossfile_and_billing_only_file1():
    f1 = make_df([make_row(Phone1='4155550001'), make_row(Phone1='4155550002')])
    cfg = PipelineConfig(
        billing_dfs=[make_df([make_row(Phone1='4155550002')])],
        bad_states_set=set(),
    )
    result = run_full_pipeline_multi([f1], ['only.xlsx'], FULL_MAPPING, cfg)
    crossfile = next(s for s in result.stages if s.label == STAGE_CROSSFILE)
    billing = next(s for s in result.stages if s.label == STAGE_BILLING)
    assert crossfile.ran is False           # needs 2+ files
    assert billing.ran is True
    assert result.files[0].final_count == 1  # billing removed the 002 row from File 1


def test_multi_pipeline_billing_only_affects_file1():
    f1 = make_df([make_row(Phone1='4155550001')])
    f2 = make_df([make_row(Phone1='4155550001')])  # same phone as f1
    cfg = PipelineConfig(
        billing_dfs=[make_df([make_row(Phone1='4155550001')])],
        bad_states_set=set(),
    )
    result = run_full_pipeline_multi([f1, f2], ['a.xlsx', 'b.xlsx'], FULL_MAPPING, cfg)
    # File 1's only row matches billing -> removed. File 2 already lost its row to
    # cross-file dedupe (phone present in File 1), so billing must not touch it further.
    assert result.files[0].final_count == 0
    # File 2 lost its duplicate to crossfile dedupe, not billing.
    assert 'billing_dedupe' not in set(
        result.files[1].removed_df['_removal_reason']
    ) if len(result.files[1].removed_df) else True


def test_all_pipeline_reason_codes_have_descriptions():
    """Every _removal_reason the pipeline can emit must have a display label,
    so results screens and the removed-rows export never show a raw code."""
    from file_io import REASON_DESCRIPTIONS
    emitted_reasons = {
        'highlighted_cells', 'invalid_last_name', 'empty_phone', 'invalid_phone',
        'invalid_email', 'contains_test', 'placeholder_email', 'fake_email',
        'prohibited_content', 'invalid_uuid', 'dnc_phone_match', 'dnc_area_code',
        'dnc_name_match', 'tcpa_zip_match', 'tcpa_phone_match', 'duplicate_phone',
        'master_phone_match', 'crossfile_dedupe', 'bad_states', 'billing_dedupe',
    }
    missing = emitted_reasons - set(REASON_DESCRIPTIONS)
    assert not missing, f"Missing human-readable descriptions for: {sorted(missing)}"


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
