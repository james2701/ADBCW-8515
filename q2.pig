RUN /vol/automed/data/usgs/load_tables.pig

state_data =
   FOREACH state
   GENERATE code, name;

populated_data = 
   FOREACH populated_place
   GENERATE state_code, elevation, population;

state_populated =
   JOIN state_data BY code LEFT,
        populated_data BY state_code;

state_populated_name =
   FOREACH state_populated
   GENERATE name, elevation, population;

state_group = 
   GROUP state_populated_name
   BY name;

state_result = 
   FOREACH state_group
   GENERATE group AS name, SUM(state_populated_name.population) AS pos,AVG(state_populated_name.elevation) AS ele;

state_result_ordered =
   ORDER state_result
   BY    name;

STORE state_result_ordered INTO 'q2' USING PigStorage(',');
