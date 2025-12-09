"""
Zelfscan-pipeline in 3 delen:

1. CSV's inladen
2. Opschonen (Qualtrics-vraagteksten / ImportId-rijen verwijderen)
3. AVG-proof maken (bepaalde kolommen droppen + simpele PII-checks)
"""

import re
import pandas as pd

# ====== CONFIG – IMPORT/EXPORT BESTANDEN ======
MAPPING_CSV = "Questions.csv"            # QID,Question
RAW_CSV = "Zelfscan DUMMY.csv"           # ruwe Qualtrics-export
CLEAN_CSV = "Zelfscan_clean.csv"         # output na opschonen
AVG_CSV = "Zelfscan_avg.csv"             # output na AVG-proof
SEP = ";"                                # scheidingsteken in CSV
# ===============================================


def data_extract(mapping_path: str, raw_path: str):
    print("CSV's inladen...")
    mapping = pd.read_csv(mapping_path, sep=";", encoding="utf-8")
    raw = pd.read_csv(raw_path, sep=SEP, engine="python", header=None)
    print(f"- Questions geladen: {mapping.shape[0]} regels, kolommen: {list(mapping.columns)}")
    print(f"- Ruwe data geladen: {raw.shape[0]} rijen x {raw.shape[1]} kolommen\n")
    return mapping, raw


def data_cleanup(mapping: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    print("Cleanup Qualtrics data...")

    # Rij 0 = QID
    header_row = raw.iloc[0].astype(str).tolist()
    data = raw.iloc[1:].copy()

    # Vraagteksten uit Questions
    question_texts = set(
        mapping["Question"]
        .dropna()
        .astype(str)
        .str.strip()
    )

    def is_meta_row(row) -> bool:
        row_str = row.astype(str).str.strip()
        # a) ImportId-rijen
        if row_str.str.contains("ImportId", na=False).any():
            return True
        # b) Rijen die vragen bevatten
        if row_str.isin(question_texts).any():
            return True
        return False

    meta_mask = data.apply(is_meta_row, axis=1)
    n_meta = int(meta_mask.sum())
    n_total = len(data)

    clean_data = data[~meta_mask].copy()
    clean_data.columns = header_row
    clean_data = clean_data.dropna(how="all")

    print(f"- Aantal header/meta-rijen gevonden en verwijderd: {n_meta}")
    print(f"- Overgebleven data-rijen: {len(clean_data)} (van {n_total})")
    print(f"- Aantal kolommen (QID's): {len(clean_data.columns)}\n")

    # Wegschrijven clean data
    clean_data.to_csv(CLEAN_CSV, sep=SEP, index=False)
    print(f"- Data clean weggeschreven naar:  {CLEAN_CSV}\n")

    return clean_data


def data_avg_proof(df: pd.DataFrame) -> pd.DataFrame:
    print("AVG-proof...")

    print(f"- Start: {df.shape[0]} rijen x {df.shape[1]} kolommen")

    # AVG kolommen
    cols_to_drop = [
        "IPAdress",  
        "RecipientLastName",
        "RecipientFirstName",
        "RecipientEmail",
        "Q2",    # Voornaam
        "Q3",    # Achternaam
        "Q4",    # Organisatie
        "Q5",    # Jouw e-mailadres
        "Q5_1",  # E-mailadres vertegenwoordiger
        "Q9",    # Website-URL
    ]

    present = [c for c in cols_to_drop if c in df.columns]
    print("\n- Kolommen die als PII worden verwijderd:")
    if present:
        for c in present:
            print(f"  • {c}")
        df = df.drop(columns=present)
    else:
        print("  • (Geen van de geconfigureerde PII-kolommen gevonden)")

    print(f"\n- Na AVG-proof: {df.shape[0]} rijen x {df.shape[1]} kolommen")

    # PII-checks op overgebleven data
    as_str = df.astype(str)

    email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    ip_re = re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    )

    def count_matches(regex: re.Pattern) -> int:
        return int(as_str.apply(lambda col: col.str.contains(regex, na=False)).sum().sum())

    email_hits = count_matches(email_re)
    ip_hits = count_matches(ip_re)

    print("\n- Algemene PII-controles in overgebleven kolommen:")
    print(f"  • Cellen die op e-mailadres lijken : {email_hits}")
    print(f"  • Cellen die op IP-adres lijken    : {ip_hits}")

    # Wegschrijven
    df.to_csv(AVG_CSV, sep=SEP, index=False)
    print(f"- AVG-proof bestand geschreven naar: {AVG_CSV}\n")

    return df


def main():
    # User story 3.1
    mapping, raw = data_extract(MAPPING_CSV, RAW_CSV)

    # User story 3.3
    clean = data_cleanup(mapping, raw)

    # User story 3.2
    _ = data_avg_proof(clean)


if __name__ == "__main__":
    main()
