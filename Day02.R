library(DBI)
library(duckdb)

con <- dbConnect(duckdb())

query_1 <- "
CREATE TABLE IF NOT EXISTS AOC_2024_DAY_2_LEVELS AS
SELECT
  *
FROM
  read_csv ('data/2024_DAY_2.csv');

CREATE TABLE IF NOT EXISTS AOC_2024_DAY_2_METRICS AS (
  WITH
    STEPS AS (
      SELECT
        *,
        LEVEL_1 - LEVEL_2 as STEP1,
        LEVEL_2 - LEVEL_3 as STEP2,
        LEVEL_3 - LEVEL_4 as STEP3,
        LEVEL_4 - LEVEL_5 as STEP4,
        LEVEL_5 - LEVEL_6 as STEP5,
        LEVEL_6 - LEVEL_7 as STEP6,
        LEVEL_7 - LEVEL_8 as STEP7
      FROM
        AOC_2024_DAY_2_LEVELS
    ),
    METRICS AS (
      SELECT
        *,
        LEAST (STEP1, STEP2, STEP3, STEP4, STEP5, STEP6, STEP7) as MIN_STEP,
        GREATEST (STEP1, STEP2, STEP3, STEP4, STEP5, STEP6, STEP7) as MAX_STEP,
        CASE
          WHEN STEP1 = 0
          OR STEP2 = 0
          OR STEP3 = 0
          OR STEP4 = 0
          OR STEP5 = 0
          OR STEP6 = 0
          OR STEP7 = 0 THEN 1
          ELSE 0
        END as FLAT_STEP,
      FROM
        STEPS
    )
  SELECT
    *
  FROM
    METRICS
);

SELECT
  COUNT(*) as SAFE_REPORTS
FROM
  AOC_2024_DAY_2_METRICS
WHERE
  FLAT_STEP <> 1
  AND ABS(MIN_STEP) <= 3
  AND ABS(MAX_STEP) <= 3
  AND SIGN (MIN_STEP) = SIGN (MAX_STEP);
"

DAY_2_RESULT_1 <- dbGetQuery(conn = con, statement = query_1)
