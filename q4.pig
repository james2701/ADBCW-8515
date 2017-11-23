RUN /vol/automed/data/usgs/load_tables.pig

state_data = 
   FOREACH state
   GENERATE name, code;

state_data_ordered = 
   ORDER state_data
   BY name;

populated_data = 
   FOREACH populated_place
   GENERATE name, state_code, population;

populated_data_group = 
   GROUP populated_data
   BY state_code;

top_five = 
   FOREACH populated_data_group {
	ordered = ORDER populated_data BY population DESC, name ASC;
	top = LIMIT ordered 5;
	GENERATE FLATTEN(top);
};

result = 
   JOIN state_data_ordered BY code LEFT,
            top_five BY state_code;

result_opt = 
   FOREACH result
   GENERATE state_data_ordered::name AS state_name, top::name AS name, population AS population;

STORE result_opt INTO 'q4' USING PigStorage(',');
