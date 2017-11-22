RUN /vol/automed/data/usgs/load_tables.pig

state_data =
   FOREACH state
   GENERATE code, name;

populated_data = 
   FOREACH populated_place
   GENERATE state_code, elevation, population;

state_populated =
   JOIN state_data BY state_data::code LEFT,
        populated_data BY populated_data::state_code;


STORE state_populated INTO 'q2' USING PigStorage(',');
