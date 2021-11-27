
-- Topic hrv_data:
--    Session:"Core_315_185" --> KEY
--    empty-0:"0"
--    HRV:null
--    timestamp:"0"
--    type:"hrv"
--    EventTimestamp:1638003890143

DROP STREAM IF EXISTS hrv_test;

CREATE STREAM IF NOT EXISTS hrv_test 
  (session VARCHAR KEY,
   EventTimestamp BIGINT,
   timestamp VARCHAR,
   hrv INT, 
   type VARCHAR)
  WITH (kafka_topic='hrv_data',
        value_format='JSON');

DROP STREAM IF EXISTS hrv_test_forwarded;
CREATE STREAM IF NOT EXISTS hrv_test_forwarded
  WITH (kafka_topic='hrv_data_forwarded',
        value_format='JSON')
AS SELECT session,
   EventTimestamp AS event_timestamp ,
   timestamp ,
   hrv , 
   type 
FROM hrv_test
PARTITION BY session
EMIT CHANGES;

-- partition by session 


-- If reference running, hard stop, maybe require to delete the topic
show streams;
show tables;
show queries;
terminate <query ID>;




DROP STREAM IF EXISTS vehicle_tracking_sysB_s;
CREATE STREAM IF NOT EXISTS vehicle_tracking_sysB_s 
  (ROWKEY VARCHAR KEY,
   system VARCHAR,
   timestamp VARCHAR, 
   vehicleId VARCHAR, 
   driverId BIGINT, 
   routeId BIGINT,
   eventType VARCHAR,
   latLong VARCHAR,
   correlationId VARCHAR)
  WITH (kafka_topic='vehicle_tracking_sysB',
        value_format='DELIMITED');

CREATE STREAM IF NOT EXISTS vehicle_tracking_refined_s 
  WITH (kafka_topic='vehicle_tracking_refined',
        value_format='AVRO',
        VALUE_AVRO_SCHEMA_FULL_NAME='com.trivadis.avro.VehicleTrackingRefined')
AS SELECT truckId AS ROWKEY
		, 'Tracking_SysA' AS source
		, timestamp
		, AS_VALUE(truckId) AS vehicleId
		, driverId
		, routeId
		, eventType
		, latitude
		, longitude
		, correlationId
FROM vehicle_tracking_sysA_s
PARTITION BY truckId
EMIT CHANGES;


CREATE STREAM IF NOT EXISTS vehicle_tracking_refined_s 
  WITH (kafka_topic='vehicle_tracking_refined',
        value_format='AVRO',
        VALUE_AVRO_SCHEMA_FULL_NAME='com.trivadis.avro.VehicleTrackingRefined')
AS SELECT truckId AS ROWKEY
		, 'Tracking_SysA' AS source
		, timestamp
		, AS_VALUE(truckId) AS vehicleId
		, driverId
		, routeId
		, eventType
		, latitude
		, longitude
		, correlationId
FROM vehicle_tracking_sysA_s
PARTITION BY truckId
EMIT CHANGES;


INSERT INTO vehicle_tracking_refined_s 
SELECT ROWKEY
    , 'Tracking_SysB' AS source
	, timestamp
	, vehicleId
	, driverId
	, routeId
	, eventType
	, cast(split(latLong,':')[1] as DOUBLE) as latitude
	, CAST(split(latLong,':')[2] AS DOUBLE) as longitude
	, correlationId
FROM vehicle_tracking_sysB_s
EMIT CHANGES;




empty-0:"0"
HRV:null
timestamp:"0"
type:"hrv"
Session:"Core_315_185"
EventTimestamp:1638003890143





