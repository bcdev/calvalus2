# Note: This content has been designed for the HSQLDB 1.8.0.10 driver (org.hsqldb.jdbcDriver).
#       SQL may need to be adapted for other database drivers.
#
# Author: Norman


DROP TABLE Production IF EXISTS;

CREATE TABLE production
(
    production_id       VARCHAR PRIMARY KEY,
    production_name     VARCHAR NOT NULL,
    production_type     VARCHAR NOT NULL,
    production_user     VARCHAR NOT NULL,
    submit_time         DATE,
    start_time          DATE,
    stop_time           DATE,
    job_id_list         VARCHAR NOT NULL,
    processing_state    VARCHAR NOT NULL,
    processing_progress FLOAT NOT NULL,
    processing_message  VARCHAR NOT NULL,
    output_path         VARCHAR,
    staging_state       VARCHAR NOT NULL,
    staging_progress    FLOAT NOT NULL,
    staging_message     VARCHAR NOT NULL,
    staging_path        VARCHAR NOT NULL,
    auto_staging        BOOLEAN NOT NULL,
    request_xml         VARCHAR
);


