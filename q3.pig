RUN /vol/automed/data/usgs/load_tables.pig

feature_data = 
   FOREACH feature
   GENERATE type, county, state_name;


group_feature = 
   GROUP feature_data
   BY county;

count_feature = 
   FOREACH group_feature {
	ppl =
	   FILTER feature_data_filtered BY type == 'ppl';
	str = 
	   FILTER feature_data_filtered BY type == 'stream';
	GENERATE group.state_name AS state_name, group AS county,
		COUNT(ppl) AS no_ppl,
		COUNT(str) AS no_stream;
   }

count_feature_filtered = 
   FILTER feature_data
   BY county IS NOT NULL;

count_feature_ordered = 
   ORDER count_feature_filtered
   BY state_name, county;

STORE count_feature_ordered INTO 'q3' USING PigStorage(',');
