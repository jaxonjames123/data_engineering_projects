copy paubox.emails_sent_status
from 's3://acp-data/Recertification/recertification_emails.csv' iam_role 'arn:aws:iam::042108671686:role/myRedshiftRole' TRIMBLANKS MAXERROR 0 region 'us-east-1'
delimiter ',' dateformat 'auto' REMOVEQUOTES;

