delete from appointments.lastpass_export;
copy appointments.lastpass_export
    from 's3://sftp_test/ecw_automation/lastpass/lastpass_vault_export_formatted.csv'
    iam_role 'arn:aws:iam::042108671686:role/myRedshiftRole'
    region 'us-east-1'
    delimiter ','
    blanksasnull
    IGNOREHEADER 1
    format as csv;
