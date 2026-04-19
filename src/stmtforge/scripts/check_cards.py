"""Temporary script to check card names in the database."""
import sqlite3

conn = sqlite3.connect("data/ccanalyser.db")

for bank in ["icici", "yes"]:
    print(f"\n=== {bank.upper()} ===")
    rows = conn.execute(
        "SELECT source_file, card_name, COUNT(*) FROM transactions "
        "WHERE bank = ? GROUP BY source_file, card_name ORDER BY source_file",
        (bank,),
    ).fetchall()
    for r in rows:
        print(f"  {r[0]:55s} | {str(r[1]):15s} | {r[2]}")

conn.close()
