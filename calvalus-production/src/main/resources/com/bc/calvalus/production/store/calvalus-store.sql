DROP TABLE Production IF EXISTS;

CREATE TABLE production
(
    id                  VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL,
    production_type     VARCHAR NOT NULL,
    user                VARCHAR NOT NULL,
    submit_time         DATE NOT NULL,
    start_time          DATE,
    stop_time           DATE,
    job_id_list         VARCHAR NOT NULL,
    processing_state    VARCHAR NOT NULL,
    processing_progress FLOAT NOT NULL,
    processing_message  VARCHAR,
    staging_state       VARCHAR NOT NULL,
    staging_progress    FLOAT NOT NULL,
    staging_message     VARCHAR,
    staging_path        VARCHAR NOT NULL,
    auto_staging        BOOLEAN NOT NULL,
    request_xml         VARCHAR
);


