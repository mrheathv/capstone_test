PRAGMA disable_progress_bar;

-- 1) Normalize interactions to consistent fields
CREATE OR REPLACE VIEW v_interactions_norm AS
SELECT
  i.*,
  TRY_CAST(i.timestamp AS TIMESTAMP)               AS ts_interaction,
  CAST(TRY_CAST(i.timestamp AS TIMESTAMP) AS DATE) AS d_interaction,
  LOWER(COALESCE(i.status, ''))                    AS status_lc,
  LOWER(COALESCE(i.activity_type, ''))             AS activity_type_lc,
  LENGTH(COALESCE(i.comment, ''))                  AS note_len
FROM interactions i;

-- 2) Last touch per account
CREATE OR REPLACE VIEW v_last_touch AS
SELECT
  account_id,
  MAX(ts_interaction) AS last_touch
FROM v_interactions_norm
GROUP BY account_id;


-- 3) Open work (broader definition)
--    Treat any interaction with an "open-like" status as outstanding.
CREATE OR REPLACE VIEW v_open_work AS
SELECT
  a.account_id,
  a.sales_agent,
  a.product,
  a.account                     AS account_name_from_pipeline,
  a.deal_stage,
  TRY_CAST(a.engage_date AS DATE) AS engage_date,
  TRY_CAST(a.close_date  AS DATE) AS close_date,
  b.activity_type,
  b.status_lc,
  b.ts_interaction,
  b.d_interaction,
  b.comment
FROM sales_pipeline a
LEFT JOIN v_interactions_norm b
  ON a.account_id = b.account_id
WHERE a.deal_stage = 'Engaging'
  AND a.account_id IS NOT NULL
  AND TRY_CAST(a.engage_date AS DATE) >= CURRENT_DATE - 30
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY a.account_id
  ORDER BY b.ts_interaction DESC NULLS LAST
) = 1;



-- Convenience: just items due today
CREATE OR REPLACE VIEW v_open_today AS
SELECT * FROM v_open_work WHERE due_date = CURRENT_DATE;

-- 4) Pipeline snapshot (aligned to your columns)
--    Use deal_stage and derive a simple deal_status from close_date.
CREATE OR REPLACE VIEW v_pipeline_snapshot AS
SELECT
  sp.account_id,
  sp.product_id,
  sp.product,
  sp.account              AS account_name_from_pipeline,
  sp.sales_agent,
  TRY_CAST(sp.engage_date AS DATE) AS engage_date,
  TRY_CAST(sp.close_date  AS DATE) AS close_date,
  sp.close_value                      AS amount,
  sp.deal_stage,
  CASE WHEN sp.close_date IS NULL THEN 'open' ELSE 'closed' END AS deal_status
FROM sales_pipeline sp;

-- 5) Accounts summary (uses accounts table for the display name)
CREATE OR REPLACE VIEW v_accounts_summary AS
SELECT
  a.account_id,
  a.account                 AS account_name,
  lt.last_touch,
  EXISTS (SELECT 1 FROM v_open_work ow WHERE ow.account_id = a.account_id) AS has_open_work
FROM accounts a
LEFT JOIN v_last_touch lt USING (account_id);
