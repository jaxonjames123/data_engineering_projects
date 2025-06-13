SELECT distinct	
       t5.name mco,	
       t4.last_name AS PCP_Last_Name,	
       t4.first_name AS PCP_First_Name,	
       t4.npi,	
       t3.medicare_id,	
       t3.member_id,	
       t3.first_name pt_fname,	
       t3.last_name pt_lname,	
       t3.dob,	
       t6.name AS ICD10_code,	
       t6.description,	
       date(t2.chart_review_date) chart_review_date,	
       t7.first_name AS coder_first_name,	
       t7.last_name AS coder_last_name,	
       case 	
        when t1.code_type = '1' then 'Previous'	
        when t1.code_type = '2' then 'Suspect'	
       else 'Error'	
       end as code_type,	
       case 	
        when t1.code_type = '1' then 'Approved'	
        when t1.code_type = '2' then 'Pending'	
        when t1.code_type = '3' then 'Missing'	
        when t1.code_type = '4' then 'Rejected'	
       else 'Error'	
       end as status	
	
FROM assignedcode t1	
  INNER JOIN chart t2 ON t1.charts_id = t2.id	
  INNER JOIN patient t3 ON t2.patient_id = t3.id	
  INNER JOIN pcp t4 ON t3.pcp_id = t4.id	
  INNER JOIN mco t5 ON t3.mco_id = t5.id	
  INNER JOIN code t6 ON t1.hcc_code_id = t6.id	
 INNER JOIN auth_user t7 ON t2.created_by_id = t7.id	
WHERE 1=1	
AND   t2.chart_review_date >= ? AND t2.chart_review_date < ?
order by chart_review_date;