RUN /vol/automed/data/usgs/load_tables.pig


populated_data = 
   FOREACH state
   GENERATE name, code;


STORE populated_data INTO 'q5' USING PigStorage(',');
