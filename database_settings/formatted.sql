CREATE TABLE peru_exports_headings (

    HEADING VARCHAR PRIMARY KEY,
    DESCRIPTION VARCHAR (150),
    MAPPED_TO VARCHAR

);

CREATE TABLE peru_exports
(

    HEADING       VARCHAR,
    EXP_ID        VARCHAR,
    NET_WEIGHT    NUMERIC(14, 3),
    GROSS_WEIGHT  NUMERIC(14, 3),
    VALUE_USD     NUMERIC(11, 3),
    COUNTRY       VARCHAR(3),
    BOARDING_DATE VARCHAR,
    DESCRIPTION   TEXT,
    BATCH_WEEK    VARCHAR
);