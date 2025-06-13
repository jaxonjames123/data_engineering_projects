delete from paubox.quality_emails_group;

COPY paubox.quality_emails_group
    FROM 's3://sftp_test/transformed_quality_emails.csv'
    iam_role 'arn:aws:iam::042108671686:role/myRedshiftRole'
    region 'us-east-1'
    CSV
    IGNOREHEADER 0
    TIMEFORMAT 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL;