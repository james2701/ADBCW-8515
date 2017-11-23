RUN /vol/automed/data/usgs/load_tables.pig

feature_data = 
   FOREACH feature
   GENERATE type, county, state_name;

feature_data_filtered = 
   FILTER feature_data
   BY county IS NOT NULL;

feature_seperate = 
   FOREACH feature_data_filtered
   GENERATE (type == 'ppl'), (type == 'stream'), county, state_name;

STORE feature_seperate INTO 'q3' USING PigStorage(',');
