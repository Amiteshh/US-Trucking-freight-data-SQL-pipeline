-- Raw table that mirrors your CSV exactly
DROP TABLE IF EXISTS faf_raw CASCADE;

CREATE TABLE faf_raw (
  fr_orig TEXT,
  dms_orig TEXT,
  dms_dest TEXT,
  fr_dest TEXT,
  fr_inmode TEXT,
  dms_mode TEXT,
  fr_outmode TEXT,
  sctg2 TEXT,
  trade_type TEXT,
  dist_band TEXT,
  tons_2017 NUMERIC,
  tons_2018 NUMERIC,
  tons_2019 NUMERIC,
  tons_2020 NUMERIC,
  tons_2021 NUMERIC,
  tons_2022 NUMERIC,
  tons_2023 NUMERIC,
  tons_2024 NUMERIC,
  tons_2030 NUMERIC,
  tons_2035 NUMERIC,
  tons_2040 NUMERIC,
  tons_2045 NUMERIC,
  tons_2050 NUMERIC,
  value_2017 NUMERIC,
  value_2018 NUMERIC,
  value_2019 NUMERIC,
  value_2020 NUMERIC,
  value_2021 NUMERIC,
  value_2022 NUMERIC,
  value_2023 NUMERIC,
  value_2024 NUMERIC,
  value_2030 NUMERIC,
  value_2035 NUMERIC,
  value_2040 NUMERIC,
  value_2045 NUMERIC,
  value_2050 NUMERIC,
  current_value_2018 NUMERIC,
  current_value_2019 NUMERIC,
  current_value_2020 NUMERIC,
  current_value_2021 NUMERIC,
  current_value_2022 NUMERIC,
  current_value_2023 NUMERIC,
  current_value_2024 NUMERIC,
  tmiles_2017 NUMERIC,
  tmiles_2018 NUMERIC,
  tmiles_2019 NUMERIC,
  tmiles_2020 NUMERIC,
  tmiles_2021 NUMERIC,
  tmiles_2022 NUMERIC,
  tmiles_2023 NUMERIC,
  tmiles_2024 NUMERIC,
  tmiles_2030 NUMERIC,
  tmiles_2035 NUMERIC,
  tmiles_2040 NUMERIC,
  tmiles_2045 NUMERIC,
  tmiles_2050 NUMERIC
);

-- Helpful indexes for lane and mode queries
CREATE INDEX IF NOT EXISTS idx_faf_lane ON faf_raw (dms_orig, dms_dest);
CREATE INDEX IF NOT EXISTS idx_faf_mode ON faf_raw (dms_mode);
CREATE INDEX IF NOT EXISTS idx_faf_trade ON faf_raw (trade_type);

-- Long format view that pivots all year fields into rows
-- Makes analytics much easier
CREATE OR REPLACE VIEW faf_long AS
WITH base AS (
  SELECT
    fr_orig, dms_orig, dms_dest, fr_dest,
    fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
    ARRAY[2017,2018,2019,2020,2021,2022,2023,2024,2030,2035,2040,2045,2050]::INT[] AS years,
    ARRAY[tons_2017,tons_2018,tons_2019,tons_2020,tons_2021,tons_2022,tons_2023,tons_2024,tons_2030,tons_2035,tons_2040,tons_2045,tons_2050]::NUMERIC[] AS tons_arr,
    ARRAY[value_2017,value_2018,value_2019,value_2020,value_2021,value_2022,value_2023,value_2024,value_2030,value_2035,value_2040,value_2045,value_2050]::NUMERIC[] AS value_arr,
    ARRAY[tmiles_2017,tmiles_2018,tmiles_2019,tmiles_2020,tmiles_2021,tmiles_2022,tmiles_2023,tmiles_2024,tmiles_2030,tmiles_2035,tmiles_2040,tmiles_2045,tmiles_2050]::NUMERIC[] AS tmiles_arr
  FROM faf_raw
)
SELECT
  fr_orig, dms_orig, dms_dest, fr_dest,
  fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
  years[i] AS year,
  tons_arr[i] AS tons,
  value_arr[i] AS value_usd,
  tmiles_arr[i] AS ton_miles
FROM base, generate_subscripts(base.years, 1) AS i;

-- Current value view for CPI adjusted fields (2018 to 2024 only)
CREATE OR REPLACE VIEW faf_current_value AS
SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2018 AS year, current_value_2018 AS current_value_usd FROM faf_raw
UNION ALL SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2019, current_value_2019 FROM faf_raw
UNION ALL SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2020, current_value_2020 FROM faf_raw
UNION ALL SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2021, current_value_2021 FROM faf_raw
UNION ALL SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2022, current_value_2022 FROM faf_raw
UNION ALL SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2023, current_value_2023 FROM faf_raw
UNION ALL SELECT fr_orig, dms_orig, dms_dest, fr_dest,
       fr_inmode, dms_mode, fr_outmode, sctg2, trade_type, dist_band,
       2024, current_value_2024 FROM faf_raw;

-- Convenience view to join nominal value with CPI adjusted value per year
CREATE OR REPLACE VIEW faf_long_with_cpi AS
SELECT l.*, cv.current_value_usd
FROM faf_long l
LEFT JOIN faf_current_value cv
  ON l.fr_orig = cv.fr_orig
 AND l.dms_orig = cv.dms_orig
 AND l.dms_dest = cv.dms_dest
 AND l.fr_dest = cv.fr_dest
 AND l.fr_inmode = cv.fr_inmode
 AND l.dms_mode = cv.dms_mode
 AND l.fr_outmode = cv.fr_outmode
 AND l.sctg2 = cv.sctg2
 AND l.trade_type = cv.trade_type
 AND l.dist_band = cv.dist_band
 AND l.year = cv.year;
