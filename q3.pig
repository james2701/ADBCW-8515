RUN /vol/automed/data/usgs/load_tables.pig

feature_data = 
   FOREACH feature
   GENERATE type, county, state_name;


group_feature = 
   GROUP feature_data
   BY (county, state_name);

count_feature = 
   FOREACH group_feature {
	ppl =
	   FILTER feature_data BY type == 'ppl';
	str = 
	   FILTER feature_data BY type == 'stream';
	GENERATE group.state_name AS state_name, group.county AS county,
		COUNT(ppl) AS no_ppl,
		COUNT(str) AS no_stream;
   }

count_feature_filtered = 
   FILTER count_feature
   BY county =! 'null';

count_feature_ordered = 
   ORDER count_feature_filtered
   BY state_name, county;

STORE count_feature_ordered INTO 'q3' USING PigStorage(',');
