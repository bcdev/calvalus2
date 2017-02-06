-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
# Note: This content has been designed for the HSQLDB 1.8.0.10 driver (org.hsqldb.jdbcDriver).
#       SQL may need to be adapted for other database drivers.
#
# Author: Norman (originally), Hans (adapted for wps use)


DROP TABLE LocalJob IF EXISTS;

CREATE TABLE LocalJob
(
    job_id              VARCHAR PRIMARY KEY,
    process_id          VARCHAR NOT NULL,
    production_name     VARCHAR NOT NULL,
    production_type     VARCHAR NOT NULL,
    geo_region          VARCHAR NOT NULL,
    source_product_name VARCHAR NOT NULL,
    output_format       VARCHAR NOT NULL,
    target_dir          VARCHAR NOT NULL,
    processing_state    VARCHAR NOT NULL,
    processing_progress FLOAT NOT NULL,
    processing_message  VARCHAR NOT NULL,
    result_urls         VARCHAR,
    stop_time           DATETIME
);
