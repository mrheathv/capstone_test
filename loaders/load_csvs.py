from pathlib import Path
import duckdb, pandas as pd

DATA = Path("data")
DB   = Path("db/sales.duckdb")
DB.parent.mkdir(parents=True, exist_ok=True)

tables = {
    "accounts": "accounts.csv",
    "products": "products.csv",
    "interactions": "interactions.csv",
    "sales_pipeline": "sales_pipeline.csv",
    "sales_teams": "sales_teams.csv",
}

con = duckdb.connect(DB.as_posix())


for t, f in tables.items():
    df = pd.read_csv(DATA / f)
    con.execute(f"DROP TABLE IF EXISTS {t}")
    con.register(f"df_{t}", df)
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM df_{t}")

print("Loaded tables:", [r[0] for r in con.execute("SHOW TABLES").fetchall()])


for maybe_key in ["id","account_id","owner_id","pipeline_id"]:
    for t in tables:
        cols = [c[0] for c in con.execute(f"DESCRIBE {t}").fetchall()]
        if maybe_key in cols:
            con.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_{maybe_key} ON {t}({maybe_key})")

con.close()
