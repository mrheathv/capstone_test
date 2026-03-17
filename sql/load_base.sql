PRAGMA disable_progress_bar;

CREATE OR REPLACE TABLE accounts       AS SELECT * FROM read_csv_auto('data/accounts.csv',        header=true);
CREATE OR REPLACE TABLE products       AS SELECT * FROM read_csv_auto('data/products.csv',        header=true);
CREATE OR REPLACE TABLE interactions   AS SELECT * FROM read_csv_auto('data/interactions.csv',    header=true);
CREATE OR REPLACE TABLE sales_pipeline AS SELECT * FROM read_csv_auto('data/sales_pipeline.csv',  header=true);
CREATE OR REPLACE TABLE sales_teams    AS SELECT * FROM read_csv_auto('data/sales_teams.csv',     header=true);

