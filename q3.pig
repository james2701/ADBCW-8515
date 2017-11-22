RUN /vol/automed/data/usgs/load_tables.pig

state_data =
   FOREACH state
   GENERATE code, name;

populated_county =
   FOREACH populated_place
   GENERATE county, state_code;

state_county_pop = 
   JOIN populated_county BY state_code,
        state_data BY code;

state_county_pop_name = 
   FOREACH state_county_pop
   GENERATE state_data::name AS state_name, county;

feature_county = 
   FOREACH feature
   GENERATE UPPER(state_name) AS state_name, county;

all_county_bag = 
   UNION state_county_pop_name, feature_county;

all_county = 
   DISTINCT all_county_bag;

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
   GENERATE group AS county, COUNT(stream_feature.county) AS no_stream;

count_pop_state = 
   JOIN all_county BY county LEFT,
            count_pop BY county;

count_pop_state_reduce = 
   FOREACH count_pop_state
   GENERATE state_name, all_county::county AS county, no_ppl;

count_pop_stream_state = 
  JOIN count_pop_state_reduce BY county LEFT,
            count_stream BY county;

count_pop_stream_state_reduce = 
   FOREACH count_pop_stream_state 
   GENERATE state_name, count_pop_state_reduce::county AS county, no_ppl, no_stream;


STORE count_pop_stream_state_reduce INTO 'q3' USING PigStorage(',');
