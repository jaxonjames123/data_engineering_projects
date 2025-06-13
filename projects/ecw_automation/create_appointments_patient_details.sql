drop table appointments.patient_details;
create table appointments.patient_details
  as (select distinct patient_dob as dob, upper(patient_first_name || ' ' || patient_last_name) as patient_name
from appointments.ecw_appointments
where patient_dob is not null
  and added_tz in (select max(added_tz) from appointments.ecw_appointments));
grant all on schema appointments to group analytics;
grant all on all tables in schema appointments to group analytics;
