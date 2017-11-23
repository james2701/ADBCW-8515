RUN /vol/automed/data/usgs/load_tables.pig


populated_county =
   FOREACH populated_place
   GENERATE name, county;

group_pop =
   GROUP populated_county
   BY county;

count_pop = 
   FOREACH group_pop
   GENERATE group AS county, COUNT(populated_county.name) AS no_ppl;

count_pop_stream_state_reduce_ordered = 
   ORDER count_pop 
   BY county;

STORE count_pop_stream_state_reduce_ordered INTO 'q5' USING PigStorage(',');
