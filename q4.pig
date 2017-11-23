RUN /vol/automed/data/usgs/load_tables.pig

state_data = 
   FOREACH state
   GENERATE name, code;

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
   JOIN state_data BY code,
            top_five BY state_code;

result_unfiltered = 
   FOREACH result
   GENERATE state_data::name AS state_name, top::name AS name, population AS population;

result_unordered =
   FILTER result_unfiltered
   BY population IS NOT NULL;

result_ordered = 
   ORDER result_unordered
   BY state_name, population DESC, name;

STORE result_ordered INTO 'q4' USING PigStorage(',');
