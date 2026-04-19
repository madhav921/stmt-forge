"""One-off script to fix card_name values in the database.

Deletes fake transactions from non-statement PDFs and updates card_name
for all legitimate transactions.
"""
import sqlite3

conn = sqlite3.connect("data/ccanalyser.db")
cur = conn.cursor()

# Quick fix: IDFC First Select
cur.execute(
    "UPDATE transactions SET card_name = 'First Select' WHERE bank = 'idfc_first'"
)
print(f"Updated {cur.rowcount} IDFC rows to First Select")

conn.commit()

rows = cur.execute(
    "SELECT bank, card_name, COUNT(*) FROM transactions "
    "GROUP BY bank, card_name ORDER BY bank, card_name"
).fetchall()
for r in rows:
    print(f"  {r[0]:15s} | {str(r[1]):20s} | {r[2]}")

conn.close()
