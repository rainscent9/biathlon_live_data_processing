-- If reference running, hard stop, maybe require to delete the topic
-- show streams;
-- show tables;
-- show queries;
-- terminate <query ID>;

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

-- json topic: hrv_bpm_data
--   {“empty-0”:“17",
--   “HRV”:90.597275,
--   “BPM”:90.597275,
--   “timestamp”:“51",
--   “hrv_rolling_mean”:68.75738,
--   “type”:“hrv_bpm”,
--   “Session”:“Core_315_284”,
--   “EventTimestamp”:1638041444963}
create stream IF NOT EXISTS hrv_bpm_s
    (HRV double,
    BPM double,
    timestamp varchar,
    hrv_rolling_mean double,
    type varchar,
    Session varchar Key)
with (kafka_topic='hrv_bpm_data',
      value_format='JSON');

select * from hrv_bpm_s emit changes;

CREATE STREAM IF NOT EXISTS hrv_with_level_bpm_s
WITH (KAFKA_TOPIC= 'hrv_with_level_bpm_s',
      PARTITIONS=1,
      REPLICAS=1,
      value_format='JSON')
AS SELECT timestamp,
     hrv,
     bpm,
     hrv_rolling_mean,
     session,
     (CASE WHEN (hrv_bpm_s.hrv_rolling_mean IS NULL) then null
     when (hrv_bpm_s. hrv_rolling_mean < 10) THEN 'bad'
     when (hrv_bpm_s.hrv_rolling_mean < 30) THEN 'medium' ELSE 'good' END) hrv_rolling_level_s,
     (CASE WHEN (hrv_bpm_s.hrv IS NULL) then null
     when (hrv_bpm_s.hrv < 10) THEN 'bad'
     when (hrv_bpm_s.hrv < 30) THEN 'medium' ELSE 'good' END) hrv_level_s FROM hrv_bpm_s EMIT CHANGES;

-- Json topic gps_processed_data
--   {“empty-0”:“5440",
--    “time difference”:“0.5800000000000001",
--    “timestamp”:“0.0",
--    “speed”:0.0,
--    “gradients”:null,
--    “distance”:0.0,
--    “altitude_change”:“0.0”
--    ,“_time difference”:“0.0”,
--    “_distance”:“0.0”,
--    “type”:“gps_processed”,
--    “Session”:“Core_315_284”,
--    “speed_rolling_mean”:0.0,
--    “not_moving”:“1",
--    “EventTimestamp”:1638044506824}
create stream IF NOT EXISTS gps_processed_s
    (timestamp varchar,
    speed double,
    gradients double,
    distance double,
    altitude_change double,
    Session varchar Key ,
    speed_rolling_mean double,
    not_moving varchar)
with (kafka_topic= 'gps_processed_data',
    value_format='JSON');

select * from gps_processed_s emit changes;

create stream IF NOT EXISTS gps_for_vis_s
WITH (KAFKA_TOPIC= 'gps_for_vis_s',
    PARTITIONS=1,
    REPLICAS=1,
    value_format='JSON'
) AS SELECT timestamp,
            speed,
            gradients,
            distance,
            altitude_change,
            Session,
            speed_rolling_mean,
            not_moving
from gps_processed_s emit changes;





----------
-- Arrays
----------

-- Topic hrv_data_forwarded:
DROP TABLE IF EXISTS hrv_data_a1;
CREATE TABLE IF NOT EXISTS hrv_data_a1
  WITH (kafka_topic='hrv_data_a1',
        value_format='JSON')
AS SELECT session,
        collect_list(hrv) as hrv_array,
        collect_list(timestamp) as timestamp_array
FROM hrv_data_s1
GROUP BY session
EMIT CHANGES;

-- Topic hrv_data_forwarded:
DROP TABLE IF EXISTS BPM_data_a1;
CREATE TABLE IF NOT EXISTS BPM_data_a1
  WITH (kafka_topic='hrv_bpm_data',
        value_format='JSON')
AS SELECT session,
        collect_list(BPM) as BPM_array,
        collect_list(timestamp) as timestamp_array
FROM hrv_bpm_s
GROUP BY session
EMIT CHANGES;

-- Topic hrv_data_forwarded:
DROP TABLE IF EXISTS speed_data_a1;
CREATE TABLE IF NOT EXISTS speed_data_a1
  WITH (kafka_topic='speed_data_a1',
        value_format='JSON')
AS SELECT session,
        collect_list(speed) as speed_array,
        collect_list(timestamp) as timestamp_array
FROM gps_processed_s
GROUP BY session
EMIT CHANGES;
