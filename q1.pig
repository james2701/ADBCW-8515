RUN /vol/automed/data/usgs/load_tables.pig

feature_data =
   FOREACH feature
   GENERATE UPPER(state_name) AS feature_state_name;

state_data =
   FOREACH state
   GENERATE name;

feature_state =
   JOIN feature_data BY feature_state_name LEFT,
        state_data BY name;

feature_state_norecord = 
   FILTER feature_state
   BY state_data::name IS NULL;

feature_state_data_bag = 
   FOREACH feature_state_norecord
   GENERATE feature_state_name;

feature_state_data = 
   DISTINCT feature_state_data_bag;

sorted_feature_state_data =
   ORDER feature_state_data
   BY    feature_state_name;

STORE sorted_feature_state_data INTO 'q1' USING PigStorage(',');
