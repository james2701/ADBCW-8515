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


STORE count_pop INTO 'q3' USING PigStorage(',');
