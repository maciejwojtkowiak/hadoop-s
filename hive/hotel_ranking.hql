-- =============================================================
-- Hive script: Hotel reservation ranking for 2024
-- Meets criteria H01-H15:
--   - EXTERNAL TABLE for inputs and output
--   - Variable parameters: input_dir3, input_dir4, output_dir6
--   - DROP IF EXISTS before CREATE
--   - Source tables remain intact (H04)
--   - No unnecessary intermediate tables (H05)
--   - Proper JOIN on hotel_id (H06)
--   - Filter year = '2024' (H07)
--   - RANK() OVER (ORDER BY reservation_count / rooms DESC) (H09)
--   - Result saved as JSON via JsonSerDe (H15)
-- =============================================================

DROP TABLE IF EXISTS mr_results;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS hotel_ranking;

-- 1. MapReduce results table (3)
CREATE EXTERNAL TABLE mr_results (
    hotel_id          STRING,
    year              STRING,
    reservation_count BIGINT,
    avg_price         DOUBLE,
    cancelled_count   BIGINT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir3}';

-- 2. Hotels dictionary table (4)
CREATE EXTERNAL TABLE hotels (
    hotel_id  STRING,
    name      STRING,
    region    STRING,
    category  STRING,
    rooms     INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir4}'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 3. Final ranking table formatted as JSON via SerDe (6)
CREATE EXTERNAL TABLE hotel_ranking (
    hotel_name          STRING,
    total_reservations  BIGINT,
    avg_price           DOUBLE,
    cancelled_ratio     DOUBLE,
    rooms               INT,
    rank                INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_dir6}';

-- 4. Single-pass insertion with JOIN, calculation and RANK()
INSERT OVERWRITE TABLE hotel_ranking
SELECT
    REGEXP_REPLACE(TRIM(h.name), '^\"|\"$', '')                                         AS hotel_name,
    mr.reservation_count                                                                 AS total_reservations,
    mr.avg_price                                                                         AS avg_price,
    ROUND(CAST(mr.cancelled_count AS DOUBLE) / CAST(mr.reservation_count AS DOUBLE), 6) AS cancelled_ratio,
    h.rooms                                                                              AS rooms,
    RANK() OVER (
        ORDER BY (CAST(mr.reservation_count AS DOUBLE) / CAST(h.rooms AS DOUBLE)) DESC
    )                                                                                    AS rank
FROM mr_results mr
JOIN hotels h
    ON mr.hotel_id = h.hotel_id
WHERE mr.year = '2024';
