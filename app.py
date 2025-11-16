import sqlite3
import pandas as pd

# CSV inlezen
df = pd.read_csv("dummydata.csv")

# Verbinding maken met SQLite
conn = sqlite3.connect("data/qualtrics.db")

# Data naar de database schrijven (automatisch tabel aanmaken)
df.to_sql("responses", conn, if_exists="replace", index=False)

# Test: laat zien wat er nu in zit
print("Data succesvol geladen! Voorbeeld:")
print(pd.read_sql("SELECT * FROM responses LIMIT 5;", conn))

conn.close()

