RUN /vol/automed/data/usgs/load_tables.pig

state_data =
   FOREACH state
   GENERATE code, name;

populated_data = 
   FOREACH populated_place
   GENERATE name, state_code, population;

populated_data_group = 
   GROUP populated_data
   BY state_code;

top_five = 
   FOREACH populated_data_group {
	ordered = ORDER populated_data_group BY population DESC;
	top = LIMIT ordered 5;
	GENERATE group AS state_code, FLATTEN(top);
};

STORE top_five INTO 'q4' USING PigStorage(',');
