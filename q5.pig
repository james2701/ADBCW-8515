RUN /vol/automed/data/usgs/load_tables.pig

state_data =
   FOREACH state
   GENERATE code, name;

populated_county =
   FOREACH populated_place
   GENERATE name, county, state_code;

populated_data = 
   FOREACH populated_place
   GENERATE county;


group_pop =
   GROUP populated_data
   BY county;

count_pop = 
   FOREACH group_pop
   GENERATE group AS county, COUNT(populated_data.name) AS no_ppl;


count_pop_stream_state_reduce_ordered = 
   ORDER count_pop 
   BY county;

STORE count_pop_stream_state_reduce_ordered INTO 'q3' USING PigStorage(',');
