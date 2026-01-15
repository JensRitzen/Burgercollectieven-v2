import re
import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


# CONFIG
# =======================
INPUT_CSV = "/app/data/Zelfscan_final.csv"
SEP = ";"

GROUP_BY_QID = "Q19"
ANALYZE_QIDS = ["Q24", "Q54"]

OUT_DIR = "/app/data/charts"
TYPST_EXE = "typst"
# =======================


def safe_filename(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    return s[:120] if len(s) > 120 else s


def load_long(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=SEP, dtype="string")
    df["ResponsID"] = df["ResponsID"].astype("string").str.strip()
    df["QuestionID"] = df["QuestionID"].astype("string").str.strip()
    df["Answer"] = df["Answer"].astype("string").str.strip()
    df = df.dropna(subset=["ResponsID", "QuestionID"])
    return df


def build_group_map(df: pd.DataFrame, group_qid: str) -> pd.DataFrame:
    g = df[df["QuestionID"] == group_qid][["ResponsID", "Answer"]].copy()
    g = g.dropna(subset=["Answer"])
    g = g[g["Answer"] != ""]
    g = g.rename(columns={"Answer": "GroupValue"})
    g = g.drop_duplicates(subset=["ResponsID"], keep="first")
    return g


def counts_for_group_and_qid(df: pd.DataFrame, group_map: pd.DataFrame, group_value: str, qid: str) -> pd.DataFrame:
    base = df[df["QuestionID"] == qid][["ResponsID", "Answer"]].copy()
    base = base.dropna(subset=["Answer"])
    base = base[base["Answer"] != ""]

    joined = base.merge(group_map, on="ResponsID", how="inner")
    joined = joined[joined["GroupValue"] == group_value]

    if joined.empty:
        return joined

    counts = (
        joined.groupby("Answer", dropna=False)
        .size()
        .reset_index(name="Count")
        .sort_values("Count", ascending=False)
    )
    return counts


def make_pie_temp_png(counts: pd.DataFrame, group_value: str, qid: str, tmp_dir: Path) -> str | None:
    if counts is None or counts.empty:
        return None
    total = int(counts["Count"].sum())
    if total <= 0:
        return None

    labels = counts["Answer"].astype(str).tolist()
    values = counts["Count"].tolist()

    tmp_dir.mkdir(parents=True, exist_ok=True)
    safe_group = safe_filename(group_value)
    out_path = tmp_dir / f"pie_{safe_group}_{qid}.png"

    # Bepaal index van grootste groep
    max_idx = int(counts["Count"].astype(int).values.argmax())

    # Explode: alleen grootste slice een beetje naar buiten
    explode = [0.0] * len(values)
    explode[max_idx] = 0.08  # pas aan naar smaak (bv. 0.05–0.15)

    plt.figure()
    plt.pie(
        values,
        labels=labels,
        autopct="%1.1f%%",
        explode=explode,
        startangle=90,  # optioneel: iets rustiger layout
    )
    plt.gca().set_aspect("equal")
    plt.title(f"{qid} – {group_value}")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()


    if out_path.stat().st_size == 0:
        try:
            out_path.unlink()
        except OSError:
            pass
        return None

    return str(out_path)


def write_typst_and_compile_pdf(group_value: str, image_paths: list[str], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_group = safe_filename(group_value)
    typ_path = out_dir / f"group_{safe_group}.typ"
    pdf_path = out_dir / f"group_{safe_group}.pdf"

    images_block = "\n".join(
    [
        f'#image("{Path(p).resolve().relative_to(out_dir.resolve()).as_posix()}", width: 100%)\n#v(14pt)'
        for p in image_paths
    ]
)
    
    typ_content = f"""\
#set page(margin: 18mm)
#set text(size: 11pt)

= Groep: {group_value}

{images_block}
"""

    typ_path.write_text(typ_content, encoding="utf-8")

    # Compile met typst
    result = subprocess.run(
        [TYPST_EXE, "compile", str(typ_path), str(pdf_path)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print("Typst compile error:")
        print(result.stderr)
        raise RuntimeError("Typst compile failed")

    return pdf_path


def main():
    df = load_long(INPUT_CSV)
    group_map = build_group_map(df, GROUP_BY_QID)

    if group_map.empty:
        print("Geen groepen gevonden. Stop.")
        return

    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Root voor tijdelijke images
    tmp_root = out_dir / "_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)

    # === PER GROEP ===
    for group_value in sorted(group_map["GroupValue"].astype(str).unique()):
        safe_group = safe_filename(group_value)

        tmp_dir = tmp_root / safe_group
        tmp_dir.mkdir(parents=True, exist_ok=True)

        temp_images: list[str] = []

        for qid in ANALYZE_QIDS:
            counts = counts_for_group_and_qid(df, group_map, group_value, qid)
            img_path = make_pie_temp_png(counts, group_value, qid, tmp_dir)

            if img_path:
                temp_images.append(img_path)

        if not temp_images:
            print(f"Groep '{group_value}': geen data; PDF overgeslagen.")
            continue

        try:
            pdf_path = write_typst_and_compile_pdf(group_value, temp_images, out_dir)
            print(f"PDF gemaakt: {pdf_path}")
        finally:
            # Opruimen tijdelijke PNG's
            for p in temp_images:
                try:
                    Path(p).unlink()
                except OSError:
                    pass

            # Opruimen lege tmp map
            try:
                tmp_dir.rmdir()
            except OSError:
                pass


if __name__ == "__main__":
    main()

