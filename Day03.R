library(DBI)
library(duckdb)

con <- dbConnect(duckdb())

query_1 <- "
CREATE TABLE IF NOT EXISTS AOC_2024_DAY_3 AS
SELECT
  *
FROM
  read_csv ('data/2024_DAY_3.csv');

WITH
  UNNESTED AS (
    SELECT
      UNNEST(
        REGEXP_EXTRACT_ALL (TEXT, 'mul\\(\\d{1,3},\\d{1,3}\\)')
      ) as TEXT_SPLIT
    FROM
      AOC_2024_DAY_3
  ),
  NUMBERS_COMMA AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(TEXT_SPLIT, 'mul\\(', ''),
        '\\)',
        ''
      ) AS NUMBERS_COMMA
    FROM
      UNNESTED
  ),
  ARRAY_NUMBERS AS (
    SELECT
      REGEXP_SPLIT_TO_ARRAY(NUMBERS_COMMA, ',') AS ARRAY_NUMBERS
    FROM
      NUMBERS_COMMA
  ),
  NUMBERS_TO_MUL AS (
    SELECT
      CAST(ARRAY_NUMBERS[1] AS INTEGER) AS NUMBER_1,
      CAST(ARRAY_NUMBERS[2] AS INTEGER) AS NUMBER_2
    FROM
      ARRAY_NUMBERS
  )
SELECT
  SUM(NUMBER_1 * NUMBER_2) AS RESULT
FROM
  NUMBERS_TO_MUL

"

DAY_3_RESULT_1 <- dbGetQuery(conn = con, statement = query_1)

cat(DAY_3_RESULT_1$RESULT)


query_2 <- "

WITH
  UNNESTED AS (
    SELECT
      UNNEST(
        REGEXP_EXTRACT_ALL (TEXT, '((do\\(\\)|don.*?t\\(\\)).*?)?mul\\(\\d{1,3},\\d{1,3}\\)')
      ) as TEXT_SPLIT
    FROM
      AOC_2024_DAY_3
  )

SELECT * FROM UNNESTED

"

DAY_3_RESULT_2 <- dbGetQuery(conn = con, statement = query_2)
