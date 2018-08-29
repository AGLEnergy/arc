CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS meta CASCADE;

-- list of accepted types
CREATE TYPE meta_type AS ENUM ('boolean', 'date', 'decimal', 'double', 'integer', 'long', 'string', 'time', 'timestamp');

-- dont generally use camelcase in databases but this makes it easy to pass to the metadata format without renaming columns
CREATE TABLE meta (
	"dataset"			    TEXT 		    NOT NULL,
	"version" 		    INT 		    NOT NULL,
	"index" 			    INT 		    NOT NULL,
	"id" 				      UUID 		    DEFAULT uuid_generate_v4(),
	"name" 			      TEXT 		    UNIQUE NOT NULL,
  "description"     TEXT 		    NOT NULL,
	"trim" 			      BOOL 		    NOT NULL,
	"nullable" 		    BOOL 		    NOT NULL,
	"type" 			      meta_type 	NOT NULL,	
	"nullableValues" 	TEXT[]		  NULL,
	"trueValues" 		  TEXT[]		  NULL,
	"falseValues" 	  TEXT[]		  NULL,		
	"formatters"		  TEXT[]		  NULL,
	"timezoneId"		  TEXT 		    NULL,
	"time"			      JSONB 		  NULL,
	"precision"		    INTEGER 	  NULL,
	"scale"			      INTEGER 	  NULL,
	"metadata"		    JSONB 		  NULL,
	CONSTRAINT boolean_must_have_trueValues_falseValues CHECK (
    ("type" = 'boolean' AND ("trueValues" IS NOT NULL AND "falseValues" IS NOT NULL)) 
    OR type != 'boolean'
  ),
	CONSTRAINT date_must_have_formatters CHECK (
    ("type" = 'date' AND "formatters" IS NOT NULL) 
    OR type != 'date'
  ),
	CONSTRAINT decimal_must_have_precision_scale CHECK (
    ("type" = 'decimal' AND  ("precision" IS NOT NULL AND "scale" IS NOT NULL)) 
    OR "type" != 'decimal'
  ),
	CONSTRAINT time_must_have_formatters CHECK (
    (type = 'date' AND  "formatters" IS NOT NULL) 
    OR type != 'date'
  ),
	CONSTRAINT timestamp_must_have_formatters_timezoneId CHECK (
		("type" = 'timestamp' AND ("formatters" IS NOT NULL AND "timezoneId" IS NOT NULL) AND ("time" IS NULL OR ("time" IS NOT NULL AND "time" #> '{hour}' IS NOT NULL AND "time" #> '{minute}' IS NOT NULL AND "time" #> '{seconds}' IS NOT NULL AND "time" #> '{nano}' IS NOT NULL)))
		OR "type" != 'timestamp'
  ),	
	UNIQUE (dataset, version, index)
);

INSERT INTO meta VALUES
 ('yellow_tripdata', 0, 0, '7bd21a6e-ce4a-4641-958d-738e4345f44c', 'vendor_name', 'Provider that provided the record.', TRUE, TRUE, 'string', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 1, '86d35c23-4138-40c1-bc3c-ae4742b41856', 'trip_pickup_datetime', 'The date and time when the meter was engaged.', TRUE, TRUE, 'timestamp', '{"", "null"}', NULL, NULL, '{"yyyy-MM-dd HH:mm:ss"}', 'America/New_York', NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 2, 'bf4b5372-b9d3-4702-8d52-e560cded3d83', 'trip_dropoff_datetime', 'The date and time when the meter was disengaged.', TRUE, TRUE, 'timestamp', '{"", "null"}', NULL, NULL, '{"yyyy-MM-dd HH:mm:ss"}', 'America/New_York', NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 3, 'df29598c-d46a-4a26-aa25-84a216290f73', 'passenger_count', 'The number of passengers in the vehicle. This is a driver-entered value.', TRUE, TRUE, 'integer', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 4, 'd51dc85d-5ed0-4754-baaa-b96918d8b6a1', 'trip_distance', 'The elapsed trip distance in miles reported by the taximeter.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 18, 15, NULL)
,('yellow_tripdata', 0, 5, '4e64a6bb-8097-43b4-859a-f9b717e97e4f', 'start_lon', 'Longitude where the meter was engaged.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 18, 14, NULL)
,('yellow_tripdata', 0, 6, 'fbd7a2b8-3b69-48e8-b403-e7b5894fed32', 'start_lat', 'Latitude where the meter was engaged.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 18, 14, NULL)
,('yellow_tripdata', 0, 7, '26e8281d-539c-4c0b-8c54-aa3636659003', 'rate_code_id', 'The final rate code in effect at the end of the trip.', TRUE, TRUE, 'integer', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 8, '289ab6ec-0c2a-439b-ac8f-b251d80db2e3', 'store_and_fwd_flag', E'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka \'store and forward\', because the vehicle did not have a connection to the server.', TRUE, TRUE, 'boolean', '{"", "null"}', '{"Y","1"}', '{"N","0"}', NULL, NULL, NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 9, '835cec9a-cfbc-4956-97be-98c151aac6dc', 'end_lon', 'Longitude where the meter was disengaged.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 18, 14, NULL)
,('yellow_tripdata', 0, 10, 'f6ea8147-441f-4adc-bdad-1cbb9bb66822', 'end_lat', 'Latitude where the meter was disengaged.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 18, 14, NULL)
,('yellow_tripdata', 0, 11, 'd2ee7964-6f75-4013-90c7-b232d6f5e8eb', 'payment_type', 'A numeric code signifying how the passenger paid for the trip.', TRUE, TRUE, 'string', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
,('yellow_tripdata', 0, 12, '47919a22-9818-460b-a7ee-4d6e3d085be8', 'fare_amt', 'The time-and-distance fare calculated by the meter.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 10, 2, NULL)
,('yellow_tripdata', 0, 13, '054f8133-aeab-4f6b-99d5-68e55c5501e6', 'surcharge', 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 10, 2, NULL)
,('yellow_tripdata', 0, 14, '50eb6159-bd2c-44c9-b1ce-4f1f2bc65a5a', 'mta_tax', '$0.50 MTA tax that is automatically triggered based on the metered rate in use.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 10, 2, NULL)
,('yellow_tripdata', 0, 15, '986f0272-1529-440d-a54d-bf1a8b613d21', 'tip_amount', 'Tip amount – This field is automatically populated for credit card tips. Cash tips are not included.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 10, 2, NULL)
,('yellow_tripdata', 0, 16, '33624d96-88ec-4611-818d-1ac085410048', 'tolls_amount', 'Total amount of all tolls paid in trip.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 10, 2, NULL)
,('yellow_tripdata', 0, 17, '5036743f-b19d-4a2f-ab42-6272cfeb1997', 'total_amount', 'The total amount charged to passengers. Does not include cash tips.', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 10, 2, NULL)
,('known_dataset', 0, 0, '982cbf60-7ba7-4e50-a09b-d8624a5c49e6', 'booleanDatum', 'booleanDatum', TRUE, TRUE, 'boolean', '{"", "null"}', '{"true"}', '{"false"}', NULL, NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('booleanMeta', true, 'booleanArrayMeta', JSONB_BUILD_ARRAY(true, false), 'stringMeta', 'string', 'stringArrayMeta', JSONB_BUILD_ARRAY('string0', 'string1'), 'longMeta', 10, 'longArrayMeta',  JSONB_BUILD_ARRAY(10, 20), 'doubleMeta', 0.141, 'doubleArrayMeta', JSONB_BUILD_ARRAY(0.141, 0.52), 'private', false, 'securityLevel', 0))
,('known_dataset', 0, 1, '0e8109ba-1000-4b7d-8a4c-b01bae07027f', 'dateDatum', 'dateDatum', TRUE, TRUE, 'date', '{"", "null"}', NULL, NULL, '{"yyyy-MM-dd"}', NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', true, 'securityLevel', 3))
,('known_dataset', 0, 2, '9712c383-22d1-44a6-9ca2-0087af4857f1', 'decimalDatum', 'decimalDatum', TRUE, TRUE, 'decimal', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, 38, 18, JSONB_BUILD_OBJECT('private', true, 'securityLevel', 2))
,('known_dataset', 0, 3, '31541ea3-5b74-4753-857c-770bd601c35b', 'doubleDatum', 'doubleDatum', TRUE, TRUE, 'double', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', true, 'securityLevel', 2))
,('known_dataset', 0, 4, 'a66f3bbe-d1c6-44c7-b096-a4be59fdcd78', 'integerDatum', 'integerDatum', TRUE, TRUE, 'integer', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', true, 'securityLevel', 10))
,('known_dataset', 0, 5, '1c0eec1d-17cd-45da-8744-7a9ef5b8b086', 'longDatum', 'longDatum', TRUE, TRUE, 'long', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', false, 'securityLevel', 0))
,('known_dataset', 0, 6, '9712c383-22d1-44a6-9ca2-0087af4857f1', 'stringDatum', 'stringDatum', TRUE, TRUE, 'string', '{"", "null"}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', false, 'securityLevel', 0))
,('known_dataset', 0, 7, 'eb17a18e-4664-4016-8beb-cd2a492d4f20', 'timeDatum', 'timeDatum', TRUE, TRUE, 'time', '{"", "null"}', NULL, NULL, '{"HH:mm:ss"}', NULL, NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', true, 'securityLevel', 8))
,('known_dataset', 0, 8, '8e42c8f0-22a8-40db-9798-6dd533c1de36', 'timestampDatum', 'timestampDatum', TRUE, TRUE, 'timestamp', '{"", "null"}', NULL, NULL, E'{"yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX"}', 'UTC', NULL, NULL, NULL, JSONB_BUILD_OBJECT('private', true, 'securityLevel', 7))
;