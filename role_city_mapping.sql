Create or replace table role_city_mapping
as select
job_role,
city,
count(1) as vote_count
from kb_utrecht_training.silver.transformed_pulse_data
group by job_role,city
order by vote_count desc;