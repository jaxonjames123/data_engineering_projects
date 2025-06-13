create temp table temp_schedule
(
    mid                         varchar(255) not null,
    date_time                   timestamp    not null,
    somos_provider_id           integer      not null,
    somos_practice_id           integer      not null,
    create_date                 date,
    synchronization_id          varchar(255),
    synchronization             date,
    synchronization_status      varchar(255),
    appointment_npi             varchar(20),
    practice_tin                varchar(20),
    schedule_id                 integer,
    appointment_download_status varchar(255),
    added_tz                    timestamp default NOW(),
    modified_tz                 timestamp default null
);

copy temp_schedule
    from /home/etl/etl_home/downloads/sophia/schedule_all_data.csv with csv
    delimiter ',' header;

UPDATE aim.schedule_all t1
SET appointment_status = 'CANCELLED'
WHERE appointment_status != 'CANCELLED'
  and NOT EXISTS (SELECT 1
                  FROM temp_schedule t2
                  WHERE t2.mid = t1.mid
                    AND t2.somos_provider_id = t1.somos_provider_id
                    AND t2.practice_tin = t1.practice_tin
                    AND t2.date_time::DATE = t1.date_only
                    AND t2.appointment_download_status != 'failure');

insert into aim.schedule_all (mid, date_time, somos_provider_id, somos_practice_id, create_date, synchronization_id,
                              synchronization, synchronization_status, appointment_npi, practice_tin, schedule_id,
                              appointment_status, added_tz, modified_tz)
SELECT DISTINCT ON (mid, somos_provider_id, practice_tin, date_time::DATE) mid,
                                                                           date_time,
                                                                           somos_provider_id,
                                                                           somos_practice_id,
                                                                           create_date,
                                                                           synchronization_id,
                                                                           synchronization,
                                                                           synchronization_status,
                                                                           appointment_npi,
                                                                           practice_tin,
                                                                           schedule_id,
                                                                           'PENDING' as appointment_status,
                                                                           added_tz,
                                                                           modified_tz
FROM temp_schedule
ON CONFLICT (mid, somos_provider_id, practice_tin, date_only)
    DO UPDATE
    SET date_time   = EXCLUDED.date_time,
        date_only = date_time::DATE,
        modified_tz = now();


select count(*)
from aim.schedule_all
where date_time >= current_date;
-- add a status column in sophia, cancelled or pending
-- update sophia set status to cancelled where appointment_id does not exist in appointment table
-- if time, tin, provider, and patient does not exist in appointments, then set status to cancelled and add a new appointment