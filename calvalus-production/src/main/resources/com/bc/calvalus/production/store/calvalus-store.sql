DROP TABLE Production IF EXISTS;

CREATE TABLE production
(
    id                  INTEGER PRIMARY KEY,
    name                VARCHAR(256) NOT NULL,
    user                VARCHAR(32) NOT NULL,
    submit_time         TIME NOT NULL,
    start_time          TIME,
    stop_time           TIME,
    job_id_list         VARCHAR(512) NOT NULL,
    processing_state    VARCHAR(32) NOT NULL,
    processing_progress NUMBER NOT NULL,
    processing_message  VARCHAR(256),
    staging_state       VARCHAR(32) NOT NULL,
    staging_progress    NUMBER NOT NULL,
    staging_message     VARCHAR(256),
    staging_path        VARCHAR(512) NOT NULL,
    auto_staging        BOOLEAN NOT NULL,
    request_xml         VARCHAR(64000)
);


