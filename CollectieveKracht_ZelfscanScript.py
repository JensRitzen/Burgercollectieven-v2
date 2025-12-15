import re
import pandas as pd

# ====== CONFIG ======
RAW_CSV = "Zelfscan DUMMY.csv"
CLEAN_CSV = "Zelfscan_clean.csv"
AVG_CSV = "Zelfscan_avg.csv"
FINAL_CSV = "Zelfscan_final.csv"
SEP = ";"
# ====================

# Ruwe data inladen uit CSV bestand 
def data_extract(raw_path: str) -> pd.DataFrame:
    print("CSV inladen...")
    raw = pd.read_csv(raw_path, sep=SEP, engine="python", header=None)
    print(f"- Ruwe data geladen: {raw.shape[0]} rijen x {raw.shape[1]} kolommen\n")
    return raw

#
def data_cleanup(raw: pd.DataFrame) -> pd.DataFrame:
    print("Cleanup Qualtrics data...")

    # Header instellen
    header_row = raw.iloc[0].astype(str).tolist()
    data = raw.iloc[1:].copy()
    data.columns = header_row

    # Verwijder alle "label/vraagtekst"-rijen boven de eerste echte response rij
    resp_candidates = ["ResponseId", "ResponseID", "responseid"]
    resp_col = next((c for c in resp_candidates if c in data.columns), None)

    # Regex voor ResponseId
    resp_re = re.compile(r"^R_[A-Za-z0-9]+$")

    resp_series = data[resp_col].fillna("").astype(str).str.strip()
    data_row_mask = resp_series.str.match(resp_re)

    first_data_pos = data_row_mask.idxmax()
    clean_data = data.loc[first_data_pos:].copy()
    clean_data = clean_data.dropna(how="all")

    n_dropped = (data.index.get_loc(first_data_pos))  # aantal rijen erboven
    print(f"- Label/vraagtekst-rijen verwijderd boven eerste response: {n_dropped}")

    print(f"- Overgebleven rijen: {len(clean_data)}")
    print(f"- Kolommen: {len(clean_data.columns)}\n")

    clean_data.to_csv(CLEAN_CSV, sep=SEP, index=False)
    print(f"- Clean data geschreven naar: {CLEAN_CSV}\n")

    return clean_data

def data_avg_proof(df: pd.DataFrame) -> pd.DataFrame:
    print("AVG-proof...")

    # PII-kolommen verwijderen
    col_to_delete = [
        "IPAddress",
        "RecipientLastName",
        "RecipientFirstName",
        "RecipientEmail",
        "Q2 ",
        "Q3",
        "Q4",
        "Q5",  
        "Q5_1",
        "Q9",
    ]

    present = [c for c in col_to_delete if c in df.columns]
    print("\n- Kolommen die als PII worden verwijderd:")
    if present:
        for c in present:
            print(f"  • {c}")
        df = df.drop(columns=present)
    else:
        print("  • (Geen van de geconfigureerde PII-kolommen gevonden)")

    print(f"\n- Na kolommen verwijderen: {df.shape[0]} rijen x {df.shape[1]} kolommen")

    # Regex checks
    email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    ip_re = re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    )

    as_str = df.fillna("").astype(str)

    def count_matches(regex: re.Pattern) -> int:
        return int(as_str.apply(lambda col: col.str.contains(regex, na=False)).sum().sum())

    email_hits_before = count_matches(email_re)
    ip_hits_before = count_matches(ip_re)

    print("\n- PII-controles in overgebleven kolommen (voor):")
    print(f"  • Cellen die op e-mailadres lijken : {email_hits_before}")
    print(f"  • Cellen die op IP-adres lijken    : {ip_hits_before}")

    # PII waardes verwijderen in alle overgebleven kolommen
    def redact_pii_cellwise(series: pd.Series) -> pd.Series:
        s = series.fillna("").astype(str)
        # Als de cel een email of ip bevat
        mask = s.str.contains(email_re, na=False) | s.str.contains(ip_re, na=False)
        if mask.any():
            s.loc[mask] = ""
        return s

    # Pas toe
    df = df.apply(redact_pii_cellwise, axis=0)

    print(f"\n- Na AVG-proof: {df.shape[0]} rijen x {df.shape[1]} kolommen")

    # Wegschrijven
    df.to_csv(AVG_CSV, sep=SEP, index=False)
    print(f"- AVG-proof bestand geschreven naar: {AVG_CSV}\n")

    return df

def data_unpivot(df: pd.DataFrame) -> pd.DataFrame:
    print("Data unpivot...")

    # Zoek ResponseId-kolom
    resp_candidates = ["ResponseId", "ResponseID", "responseid", "Response ID"]
    resp_col = next((c for c in resp_candidates if c in df.columns), None)

    # Waardes naar String
    resp_series = (
        df[resp_col]
        .astype("string")
        .str.strip()
    )

    # Filter: alleen niet-leeg en niet-NA
    valid_mask = resp_series.notna() & (resp_series != "")

    # Alleen rijen met ResponseId
    work = df[valid_mask].copy()
    question_cols = [c for c in work.columns if c != resp_col]

    # Unpivot
    final_df = work.melt(
        id_vars=[resp_col],
        value_vars=question_cols,
        var_name="QuestionID",
        value_name="Answer"
    )

    final_df = final_df.rename(columns={resp_col: "ResponsID"})
    final_df = final_df[["ResponsID", "QuestionID", "Answer"]]

    # Wegschrijven
    final_df.to_csv(FINAL_CSV, sep=SEP, index=False)

    return final_df

def main():
    raw = data_extract(RAW_CSV)
    clean = data_cleanup(raw)
    avg = data_avg_proof(clean)
    _ = data_unpivot(avg)


if __name__ == "__main__":
    main()
