library(DBI)
library(duckdb)

con <- dbConnect(duckdb())

query_1 <- "
CREATE TABLE IF NOT EXISTS AOC_2024_DAY_1_LIST_1 AS
SELECT
  LIST_1
FROM
  read_csv ('data/2024_DAY_1.csv');

CREATE TABLE IF NOT EXISTS AOC_2024_DAY_1_LIST_2 AS
SELECT
  LIST_2
FROM
  read_csv ('data/2024_DAY_1.csv');

WITH
  LIST_1_RANK AS (
    SELECT
      *,
      row_number() OVER (
        ORDER BY
          LIST_1
      ) as ROW_NUMBER
    FROM
      AOC_2024_DAY_1_LIST_1
  ),
  LIST_2_RANK AS (
    SELECT
      *,
      row_number() OVER (
        ORDER BY
          LIST_2
      ) as ROW_NUMBER
    FROM
      AOC_2024_DAY_1_LIST_2
  ),
  PAIRS AS (
    SELECT
      L1.LIST_1,
      L2.LIST_2,
      L1.ROW_NUMBER,
      ABS(L1.LIST_1 - L2.LIST_2) as DISTANCE
    FROM
      LIST_1_RANK L1
      JOIN LIST_2_RANK L2 ON L1.ROW_NUMBER = L2.ROW_NUMBER
  )
SELECT
  SUM(DISTANCE) as DISTANCE
from
  PAIRS;

"

DAY_1_RESULT_1 <- dbGetQuery(conn = con, statement = query_1)

cat(DAY_1_RESULT_1$DISTANCE)

query_2 <- "
WITH
  GROUPED_LIST_2 AS (
    SELECT
      LIST_2,
      COUNT(*) AS APPEARS_L2
    FROM
      AOC_2024_DAY_1_LIST_2
    GROUP BY
      LIST_2
  ),
  SIMILARITY_LIST AS (
    SELECT
      L1.*,
      IFNULL (L2.APPEARS_L2, 0) AS APPEARS_L2,
      L1.LIST_1 * IFNULL (L2.APPEARS_L2, 0) AS SIMILARITY_SCORE
    FROM
      AOC_2024_DAY_1_LIST_1 L1
      JOIN GROUPED_LIST_2 L2 ON L1.LIST_1 = L2.LIST_2
  )
SELECT
  SUM(SIMILARITY_SCORE) AS SIMILARITY
FROM
  SIMILARITY_LIST
"

DAY_1_RESULT_2 <- dbGetQuery(conn = con, statement = query_2)

cat(DAY_1_RESULT_2$SIMILARITY)

dbDisconnect(conn = con)

