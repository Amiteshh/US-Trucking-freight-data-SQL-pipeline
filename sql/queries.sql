/* 1) Top lanes by tons for 2022 */
SELECT
  dms_orig,
  dms_dest,
  SUM(tons) AS tons_2022
FROM faf_long
WHERE year = 2022
GROUP BY 1,2
ORDER BY tons_2022 DESC
LIMIT 25;

/* 2) Mode share by year using tons */
SELECT
  year,
  dms_mode,
  SUM(tons) AS total_tons,
  ROUND(100.0 * SUM(tons) / NULLIF(SUM(SUM(tons)) OVER (PARTITION BY year), 0), 2) AS mode_share_pct
FROM faf_long
GROUP BY year, dms_mode
ORDER BY year, mode_share_pct DESC;

/* 3) Value intensity: value per ton and value per ton-mile by mode and year */
SELECT
  year,
  dms_mode,
  AVG(NULLIF(value_usd,0) / NULLIF(tons,0)) AS value_per_ton,
  AVG(NULLIF(value_usd,0) / NULLIF(ton_miles,0)) AS value_per_ton_mile
FROM faf_long
WHERE value_usd IS NOT NULL AND tons IS NOT NULL AND ton_miles IS NOT NULL
GROUP BY year, dms_mode
ORDER BY year, dms_mode;

/* 4) Growth in tons 2018 to 2024 by mode (CAGR) */
WITH base AS (
  SELECT
    dms_mode,
    SUM(CASE WHEN year = 2018 THEN tons END) AS t_2018,
    SUM(CASE WHEN year = 2024 THEN tons END) AS t_2024
  FROM faf_long
  WHERE year IN (2018, 2024)
  GROUP BY dms_mode
)
SELECT
  dms_mode,
  t_2018,
  t_2024,
  CASE
    WHEN t_2018 IS NULL OR t_2018 = 0 OR t_2024 IS NULL THEN NULL
    ELSE POWER(t_2024 / t_2018, 1.0 / 6) - 1
  END AS cagr_2018_2024
FROM base
ORDER BY cagr_2018_2024 DESC NULLS LAST;

/* 5) Trade lens: domestic vs international share by year */
SELECT
  year,
  trade_type,
  SUM(tons) AS tons,
  SUM(value_usd) AS value_usd
FROM faf_long
GROUP BY year, trade_type
ORDER BY year, trade_type;

/* 6) Distance band performance for 2022: tons, ton-miles, and value ratio */
SELECT
  dist_band,
  SUM(tons) AS tons_2022,
  SUM(ton_miles) AS ton_miles_2022,
  AVG(NULLIF(value_usd,0) / NULLIF(ton_miles,0)) AS value_per_ton_mile_2022
FROM faf_long
WHERE year = 2022
GROUP BY dist_band
ORDER BY tons_2022 DESC;

/* 7) Top commodities by value in 2023 (SCTG level 2) */
SELECT
  sctg2,
  SUM(value_usd) AS value_2023
FROM faf_long
WHERE year = 2023
GROUP BY sctg2
ORDER BY value_2023 DESC
LIMIT 20;

/* 8) Nominal vs CPI adjusted value comparison for 2021 to 2024 */
SELECT
  l.year,
  l.dms_mode,
  SUM(l.value_usd) AS nominal_value_usd,
  SUM(l.current_value_usd) AS cpi_adjusted_value_usd
FROM faf_long_with_cpi l
WHERE l.year BETWEEN 2021 AND 2024
GROUP BY l.year, l.dms_mode
ORDER BY l.year, l.dms_mode;

/* 9) Forecast check: top growing lanes by projected tons to 2030 */
WITH two_pts AS (
  SELECT dms_orig, dms_dest,
         SUM(CASE WHEN year = 2024 THEN tons END) AS t_2024,
         SUM(CASE WHEN year = 2030 THEN tons END) AS t_2030
  FROM faf_long
  WHERE year IN (2024, 2030)
  GROUP BY dms_orig, dms_dest
)
SELECT
  dms_orig,
  dms_dest,
  t_2024,
  t_2030,
  CASE
    WHEN t_2024 IS NULL OR t_2024 = 0 OR t_2030 IS NULL THEN NULL
    ELSE (t_2030 - t_2024) / t_2024
  END AS growth_ratio_24_to_30
FROM two_pts
WHERE t_2024 IS NOT NULL AND t_2030 IS NOT NULL
ORDER BY growth_ratio_24_to_30 DESC
LIMIT 25;

/* 10) Data quality quick checks */
SELECT COUNT(*) AS rows_total FROM faf_raw;
SELECT COUNT(*) AS rows_null_mode FROM faf_raw WHERE dms_mode IS NULL OR dms_mode = '';
SELECT COUNT(*) AS rows_null_lane FROM faf_raw WHERE dms_orig IS NULL OR dms_dest IS NULL;
