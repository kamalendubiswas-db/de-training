Create or replace table user_distribution
as select
job_role,
count(1) as headcount
from kb_utrecht_training.silver.transformed_pulse_data
group by job_role
order by headcount desc;
