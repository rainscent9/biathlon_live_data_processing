-- If reference running, hard stop, maybe require to delete the topic
show streams;
show tables;
show queries;
terminate <query ID>;

-- Topic hrv_data:
--    Session:"Core_315_185" --> KEY
--    empty-0:"0"
--    HRV:null
--    timestamp:"0"
--    type:"hrv"
--    EventTimestamp:1638003890143
DROP STREAM IF EXISTS hrv_data_s1;
CREATE STREAM IF NOT EXISTS hrv_data_s1
  (session VARCHAR KEY,
   EventTimestamp BIGINT,
   timestamp VARCHAR,
   hrv INT, 
   type VARCHAR)
  WITH (kafka_topic='hrv_data',
        value_format='JSON');

-- Topic hrv_data_forwarded:
DROP STREAM IF EXISTS hrv_data_s2;
CREATE STREAM IF NOT EXISTS hrv_data_s2
  WITH (kafka_topic='hrv_data_s2',
        value_format='JSON')
AS SELECT session,
   EventTimestamp AS event_timestamp,
   timestamp,
   hrv,
   type
FROM hrv_data_s1
-- PARTITION BY session -- Uncomment if Key is not already set in hrv_topic (from streamline)
EMIT CHANGES;
