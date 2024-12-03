library(DBI)
library(duckdb)

con <- dbConnect(duckdb())

query_1 <- "
CREATE TABLE IF NOT EXISTS AOC_2024_DAY_2_LEVELS AS
SELECT
  *
FROM
  read_csv ('data/2024_DAY_2.csv');

WITH STEPS AS (SELECT 
  *,
  LEVEL_1-LEVEL_2 as STEP1,
  LEVEL_2-LEVEL_3 as STEP2,
  LEVEL_3-LEVEL_4 as STEP3,
  LEVEL_4-LEVEL_5 as STEP4,
  LEVEL_5-LEVEL_6 as STEP5,
  LEVEL_6-LEVEL_7 as STEP6,
  LEVEL_7-LEVEL_8 as STEP7
  
FROM AOC_2024_DAY_2_LEVELS)

SELECT 
  *,
  arg_min_null(STEP1, STEP2, STEP3, STEP4, STEP5, STEP6, STEP7) as MIN_STEP,
  arg_max_null(STEP1, STEP2, STEP3, STEP4, STEP5, STEP6, STEP7) as MAX_STEP,
FROM STEPS

"

aa <- dbGetQuery(conn = con, statement = query_1)
