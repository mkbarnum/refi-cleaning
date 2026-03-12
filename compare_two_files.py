#!/usr/bin/env python3
"""Cross-compare two Excel lead files by Phone1, Universal_LeadId, or Address."""

import pandas as pd
import sys

def norm_str(s):
    if pd.isna(s):
        return ""
    return " ".join(str(s).upper().strip().split())

def addr_key(row, cols):
    return "|".join(norm_str(row.get(c, "")) for c in cols)

def main():
    path1 = "final_results/file5_final.xlsx"
    path2 = "final_results/2025-07-27 thru 08-02 CC TL $ - AZ DE TX removed (CLEANED).xlsx"

    if len(sys.argv) >= 3:
        path1, path2 = sys.argv[1], sys.argv[2]

    df1 = pd.read_excel(path1)
    df2 = pd.read_excel(path2)

    def norm_phone(s):
        try:
            n = int(float(s))
            s = str(n)
            if len(s) == 11 and s.startswith("1"):
                s = s[1:]
            return s if len(s) == 10 else (s[-10:] if len(s) >= 10 else None)
        except Exception:
            return None

    p1 = set(df1["Phone1"].apply(norm_phone).dropna())
    p2 = set(df2["Phone1"].apply(norm_phone).dropna())
    p1 = {x for x in p1 if x and len(x) == 10}
    p2 = {x for x in p2 if x and len(x) == 10}

    lid1 = set(df1["Universal_LeadId"].astype(str).str.strip().str.upper())
    lid2 = set(df2["Universal_LeadId"].astype(str).str.strip().str.upper())
    lid1 = {x for x in lid1 if x and x != "NAN"}
    lid2 = {x for x in lid2 if x and x != "NAN"}

    # Address key: StreetAddress|City|State|ZipCode (normalized)
    addr_cols = ["StreetAddress", "City", "State", "ZipCode"]
    if all(c in df1.columns for c in addr_cols) and all(c in df2.columns for c in addr_cols):
        a1 = set(df1.apply(lambda r: addr_key(r, addr_cols), axis=1))
        a2 = set(df2.apply(lambda r: addr_key(r, addr_cols), axis=1))
        a1 = {x for x in a1 if x.strip() and x != "|||"}
        a2 = {x for x in a2 if x.strip() and x != "|||"}
    else:
        a1 = a2 = set()

    shared_ph = p1 & p2
    shared_id = lid1 & lid2
    shared_addr = (a1 & a2) if a1 and a2 else set()
    only1_ph = p1 - p2
    only2_ph = p2 - p1
    only1_id = lid1 - lid2
    only2_id = lid2 - lid1
    only1_addr = (a1 - a2) if a1 and a2 else set()
    only2_addr = (a2 - a1) if a1 and a2 else set()

    print("=" * 60)
    print("CROSS-COMPARE: Two lead files")
    print("=" * 60)
    print()
    print("File A:", path1)
    print("  Rows: %d  |  Unique phones: %d  |  Unique lead IDs: %d  |  Unique addresses: %d"
          % (len(df1), len(p1), len(lid1), len(a1) if a1 else 0))
    print()
    print("File B:", path2)
    print("  Rows: %d  |  Unique phones: %d  |  Unique lead IDs: %d  |  Unique addresses: %d"
          % (len(df2), len(p2), len(lid2), len(a2) if a2 else 0))
    print()
    print("--- By Phone1 (10-digit) ---")
    print("  Shared (in both):     %d" % len(shared_ph))
    print("  Only in File A:       %d" % len(only1_ph))
    print("  Only in File B:       %d" % len(only2_ph))
    print("  Union (unique total): %d" % len(p1 | p2))
    print()
    print("--- By Universal_LeadId ---")
    print("  Shared (in both):     %d" % len(shared_id))
    print("  Only in File A:       %d" % len(only1_id))
    print("  Only in File B:       %d" % len(only2_id))
    print("  Union (unique total): %d" % len(lid1 | lid2))
    print()
    if a1 and a2:
        print("--- By Address (StreetAddress|City|State|ZipCode) ---")
        print("  Shared (in both):     %d" % len(shared_addr))
        print("  Only in File A:       %d" % len(only1_addr))
        print("  Only in File B:       %d" % len(only2_addr))
        print("  Union (unique total): %d" % len(a1 | a2))
        print()
    if len(shared_ph) == 0 and len(shared_id) == 0:
        print("Conclusion: No overlap by phone or lead ID.")
        if a1 and a2 and shared_addr:
            print("          %d addresses appear in both files." % len(shared_addr))
        else:
            print("          The two files appear to be different datasets.")
    else:
        print("Conclusion: %d shared by phone, %d by lead ID, %d by address."
              % (len(shared_ph), len(shared_id), len(shared_addr) if a1 and a2 else 0))
    print("=" * 60)

if __name__ == "__main__":
    main()
