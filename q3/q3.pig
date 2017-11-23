RUN /vol/automed/data/usgs/load_tables.pig

feature_data = 
   FOREACH feature
   GENERATE type, county, state_name;

feature_data_filtered = 
   FILTER feature_data
   BY county IS NOT NULL;

feature_seperate = 
   FOREACH feature_data_filtered
   GENERATE (type=='ppl'?1:0) AS ppl, (type=='stream'?1:0) AS st, county, state_name;

group_feature = 
   GROUP feature_data_filtered
   BY (county, state_name)

count_feature = 
   FOREACH group_feature
   GENERATE group AS (county, state_name), SUM(feature_seperate.ppl) AS no_ppl, SUM(feature_seperate.st) AS no_stream;

count_feature_ordered = 
   ORDER count_feature
   BY state_name, feature;

STORE count_feature_ordered INTO 'q3' USING PigStorage(',');
