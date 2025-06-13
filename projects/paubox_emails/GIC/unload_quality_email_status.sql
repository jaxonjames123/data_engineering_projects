drop table if exists staging_emails_sent_status;
create temp table staging_emails_sent_status
(
    tin           varchar(max),
    practice_name varchar(max),
    email         varchar(255),
    email_status  varchar(255),
    delivery_time timestamp,
    opened_status varchar(255) default null,
    opened_time   timestamp,
    email_type    varchar(255),
    for_month     integer,
    tracking_id   varchar(255),
    file_name     varchar(max)
);

copy staging_emails_sent_status
    from 's3://acp-data/paubox/EMAIL_TYPE_emails.csv' iam_role 'arn:aws:iam::042108671686:role/myRedshiftRole' TRIMBLANKS MAXERROR 0 region 'us-east-1'
    delimiter ',' dateformat 'auto' REMOVEQUOTES;


MERGE INTO paubox.emails_sent_status
USING staging_emails_sent_status AS t2
ON paubox.emails_sent_status.tracking_id = t2.tracking_id
    AND paubox.emails_sent_status.email = t2.email
    and paubox.emails_sent_status.tin = t2.tin
    and paubox.emails_sent_status.practice_name = t2.practice_name
    and paubox.emails_sent_status.file_name = t2.file_name
WHEN MATCHED THEN
    UPDATE
    SET delivery_time = t2.delivery_time,
        email_status = t2.email_status,
        opened_status = t2.opened_status,
        opened_time   = t2.opened_time,
        modified_tz   = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (tin, practice_name, email, email_status, delivery_time,
            opened_status, opened_time, email_type, for_month,
            tracking_id, added_tz, modified_tz, file_name)
    VALUES (t2.tin, t2.practice_name, t2.email, t2.email_status, t2.delivery_time,
            t2.opened_status, t2.opened_time, t2.email_type, t2.for_month,
            t2.tracking_id, GETDATE(), NULL, t2.file_name);