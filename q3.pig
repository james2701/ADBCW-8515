RUN /vol/automed/data/usgs/load_tables.pig

state_data =
   FOREACH state
   GENERATE code, name;

populated_data = 
   FOREACH populated_place
   GENERATE county;


group_pop =
   GROUP populated_data
   BY county;

count_pop = 
   FOREACH group_pop
   GENERATE group AS county, COUNT(populated_data.county) AS no_ppl;

feature_data = 
   FOREACH feature
   GENERATE type, county;

stream_feature = 
   FILTER feature_data
   BY type == 'stream';

group_stream = 
   GROUP stream_feature
   BY county;

count_stream =
   FOREACH group_stream
   GENERATE group AS county, COUNT(feature_data.county) AS no_stream;

pop_stream = 
   JOIN count_pop BY county FULL,
        count_stream BY county;

STORE pop_stream INTO 'q3' USING PigStorage(',');
