RUN /vol/automed/data/usgs/load_tables.pig


populated_data = 
   FOREACH populated_place
   GENERATE name, county;

populated_data_filter = 
   FILTER populated_data
   BY county == 'Autauga';

STORE populated_data_filter INTO 'q5' USING PigStorage(',');
