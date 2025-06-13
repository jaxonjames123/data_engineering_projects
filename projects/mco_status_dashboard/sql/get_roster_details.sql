SELECT *
FROM (SELECT 'Health First'                                                  AS MCO,
             'Corinthian'                                                    AS IPA,
             'Roster'                                                        AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'COR2')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'COR2')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'COR2')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'COR2')                           AS Effective_date,
             (SELECT COUNT(DISTINCT member_id)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'COR2'
                AND to_char(effective_period, 'YYYYMM') LIKE Latest_month
                AND received_month LIKE Latest_month)                        AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'COR2')                              Last_received
      UNION ALL
      SELECT 'Health First'                                                  AS MCO,
             'Excelsior'                                                     AS IPA,
             'Roster'                                                        AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'EXC1')                           AS Total_count,
             (SELECT MIN(datepart(YEAR, effective_period) || lpad(datepart(MONTH, effective_period), 2, '0'))
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'EXC1')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'EXC1')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'EXC1')                           AS Effective_date,
             (SELECT COUNT(DISTINCT member_id)
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'EXC1'
              AND to_char(effective_period, 'YYYYMM') LIKE Latest_month
              AND received_month LIKE Latest_month)                          AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_all_eligibility
              WHERE provider_parent_code = 'EXC1')                              Last_received
      UNION ALL
      SELECT 'Health First'                                                     AS MCO,
             'Somos'                                                            AS IPA,
             'Roster'                                                           AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_somos_all_eligibility
              WHERE provider_parent_code LIKE 'SOM%')                           AS Total_count,
             (SELECT MIN(datepart(YEAR, effective_period) || lpad(datepart(MONTH, effective_period), 2, '0'))
              FROM payor.healthfirst_somos_all_eligibility
              WHERE provider_parent_code LIKE 'SOM%')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_somos_all_eligibility
              WHERE provider_parent_code LIKE 'SOM%')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_somos_all_eligibility
              WHERE provider_parent_code LIKE 'SOM%')                           AS Effective_date,
             (SELECT COUNT(DISTINCT member_id)
              FROM payor.healthfirst_somos_all_eligibility
              WHERE provider_parent_code LIKE 'SOM%'
                AND to_char(effective_period, 'YYYYMM') LIKE Latest_month
                AND received_month LIKE Latest_month) AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_somos_all_eligibility
              WHERE provider_parent_code LIKE 'SOM%')                              Last_received
      UNION ALL
      SELECT 'Wellcare'                                          AS MCO,
             'Balance'                                        AS IPA,
             'Roster'                                            AS FeedType,
             (SELECT COUNT(*)
              FROM payor.wellcare_all_demographics
              WHERE master_ipa = 'VR1')                          AS Total_count,
             (SELECT MIN(receivedmonth)
              FROM payor.wellcare_all_demographics
              WHERE master_ipa = 'VR1')                          AS Earliest_month,
             (SELECT MAX(receivedmonth)
              FROM payor.wellcare_all_demographics
              WHERE master_ipa = 'VR1')                          AS Latest_month,
             (SELECT MAX(activity_date)
              FROM payor.wellcare_all_demographics
              WHERE master_ipa = 'VR1')                          AS Effective_date,
             (SELECT COUNT(DISTINCT subscriber_id)
              FROM payor.wellcare_all_demographics
              WHERE master_ipa = 'VR1'
                AND activity_date
                IN (SELECT MAX(activity_date)
                FROM payor.wellcare_all_demographics WHERE master_ipa = 'VR1'))          AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.wellcare_all_demographics
              WHERE master_ipa = 'VR1')                             Last_received
      UNION ALL
      SELECT 'Anthem'                                                                        AS MCO,
             'Somos'                                                                         AS IPA,
             'Roster'                                                                        AS FeedType,
             (SELECT COUNT(*) FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid,1,2) = 'AN')                                   AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid,1,2) = 'AN')                                   AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid,1,2) = 'AN')                                   AS Latest_month,
             (SELECT MAX(realeffectivedate)
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid,1,2) = 'AN')                                   AS Effective_date,
             (SELECT COUNT(DISTINCT memberid)
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(split_part (file_name,'_',3),1,6) LIKE Latest_month
              AND SUBSTRING(employergroupid,1,2) = 'AN')                                     AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid,1,2) = 'AN')                                   Last_received
      UNION ALL
      SELECT 'Anthem'                                                                                AS MCO,
              'Balance'                                                                               AS IPA,
              'Roster'                                                                                AS FeedType,
              (SELECT COUNT(*) FROM payor.anthem_balance_eligibility)                                 AS Total_count,
              (SELECT MIN(SUBSTRING(file_name, 26, 4) || SUBSTRING(file_name, 22, 2))
              FROM payor.anthem_balance_eligibility
              WHERE file_name != 'anthen_balance_membership_2020_2022.csv')                          AS Earliest_month,
              (SELECT MAX(SUBSTRING(file_name, 26, 4) || SUBSTRING(file_name, 22, 2))
              FROM payor.anthem_balance_eligibility
              WHERE file_name != 'anthen_balance_membership_2020_2022.csv')                          AS Latest_month,
              (SELECT to_date(MAX(effective_period), 'YYYYMM')
              FROM payor.anthem_balance_eligibility)                                                 AS Effective_date,
              (SELECT COUNT(DISTINCT member_id)
              FROM payor.anthem_balance_eligibility
              WHERE (SUBSTRING(file_name, 26, 4) || SUBSTRING(file_name, 22, 2)) LIKE
                     Latest_month)                                                                    AS Latest_month_member_count,
              (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.anthem_balance_eligibility)                                                    Last_received
      UNION ALL
      SELECT 'United'                                                                       AS MCO,
             'Somos'                                                                        AS IPA,
             'Roster'                                                                       AS FeedType,
             (SELECT COUNT(*) FROM payor.uhc_somos_all_membership)                          AS Total_count,
             (SELECT to_char(MIN(elig_start_date), 'YYYYMM')
              FROM payor.uhc_somos_all_membership)                                          AS Earliest_month,
             (SELECT to_char(MAX(elig_start_date), 'YYYYMM')
              FROM payor.uhc_somos_all_membership)                                          AS Latest_month,
             (SELECT MAX(elig_start_date)
              FROM payor.uhc_somos_all_membership)                                          AS Effective_date,
             (SELECT COUNT(DISTINCT subscriber_id)
              FROM payor.uhc_somos_all_membership
              WHERE added_tz IN
                    (SELECT MAX(added_tz) FROM payor.uhc_somos_all_membership))             AS Latest_month_member_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.uhc_somos_all_membership)                                             Last_received
      UNION ALL
      SELECT 'Fidelis'                                                              AS MCO,
             'Somos'                                                                AS IPA,
             'Roster'                                                               AS FeedType,
             (SELECT COUNT(*)
              FROM payor.evolent_fidelis_eligibility_data)                     AS Total_count,
             (SELECT CONVERT(varchar(20), MIN(monthid))
              FROM payor.evolent_fidelis_eligibility_data)                     AS Earliest_month,
             (SELECT CONVERT(varchar(20), MAX(monthid))
              FROM payor.evolent_fidelis_eligibility_data)                     AS Latest_month,
             (SELECT MAX(realeffectivedate)
              FROM payor.evolent_fidelis_eligibility_data)                     AS Effective_date,
             (SELECT COUNT(DISTINCT memberid)
              FROM payor.evolent_fidelis_eligibility_data
              WHERE monthid LIKE Latest_month)                                 AS Latest_month_member_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.evolent_fidelis_eligibility_data)                        Last_received
      UNION ALL
      SELECT 'Emblem'                                                                           AS MCO,
             'Somos'                                                                            AS IPA,
             'Roster'                                                                           AS FeedType,
             (SELECT COUNT(*)
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid, 1, 2) = 'EM')                                    AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid, 1, 2) = 'EM')                                    AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid, 1, 2) = 'EM')                                    AS Latest_month,
             (SELECT MAX(effectivedate)
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid, 1, 2) = 'EM')                                    AS Effective_date,
             (SELECT COUNT(DISTINCT memberid)
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid, 1, 2) = 'EM'
                AND SUBSTRING(split_part (file_name,'_',3),1,6) LIKE Latest_month)              AS Latest_month_member_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM innovator.evolent_eligibility_data_final
              WHERE SUBSTRING(employergroupid, 1, 2) = 'EM')                                    AS Last_received
      UNION ALL
      SELECT 'Elderplan'                                                            AS MCO,
             'Balance'                                                              AS IPA,
             'Roster'                                                               AS FeedType,
             (SELECT COUNT(DISTINCT (memberid)) FROM payor.elderplan_roster)        AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM payor.elderplan_roster)                                          AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM payor.elderplan_roster)                                          AS Latest_month,
             (SELECT MAX(pcpeffectivedate) FROM payor.elderplan_roster)             AS Effective_date,
             (SELECT COUNT(DISTINCT memberid) from payor.elderplan_roster
              WHERE SUBSTRING(split_part (file_name,'_',3),1,6) LIKE Latest_month)  AS Latest_month_member_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.elderplan_roster)                                          AS Last_received
      UNION ALL
      SELECT 'Humana'                                                                   AS MCO,
             'Somos'                                                                    AS IPA,
             'Roster'                                                                   AS FeedType,
             (SELECT COUNT(DISTINCT mbr_pid)
              FROM payor.humana_all_eligibility)                                          AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.humana_all_eligibility)                                          AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.humana_all_eligibility)                                          AS Latest_month,
             (SELECT MAX(rpt_pe) FROM payor.humana_all_eligibility)                       AS Effective_date,
             (SELECT COUNT(DISTINCT (mbr_pid))
              FROM payor.humana_all_eligibility
              WHERE received_month LIKE Latest_month)                                     AS Latest_month_member_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.humana_all_eligibility)                                          AS Last_received
      UNION ALL
      SELECT 'Molina'                                                                     AS MCO,
             'Somos'                                                                      AS IPA,
             'Roster'                                                                     AS FeedType,
             (SELECT COUNT(DISTINCT (member_id))
              FROM payor.molina_prod_membership)                                          AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',5) || split_part (file_name,'_',4),1,6))
              FROM payor.molina_prod_membership)                                          AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',5) || split_part (file_name,'_',4),1,6))
              FROM payor.molina_prod_membership)                                          AS Latest_month,
             (SELECT MAX(effective_date) FROM payor.molina_prod_membership)               AS Effective_date,
             (SELECT COUNT(DISTINCT (member_id))
              FROM payor.molina_prod_membership
              WHERE
              SUBSTRING(split_part (file_name,'_',5) || split_part (file_name,'_',4),1,6)
                 LIKE Latest_month)                                                       AS Latest_month_member_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.molina_prod_membership)                                          AS Last_received
      UNION ALL
      SELECT 'VillageCare'                                                              AS MCO,
             'Somos'                                                                    AS IPA,
             'Roster'                                                                   AS FeedType,
             (SELECT COUNT(*) FROM payor.villagecare_all_census)                        AS Total_count,
             (SELECT to_char(MIN(to_date(SUBSTRING(split_part (file_name,'_',2),1,3) || SUBSTRING(split_part (file_name,'_',3),1,4),'MonYYYY')), 'YYYYMM')
              FROM payor.villagecare_all_census)                                        AS Earliest_month,
             (SELECT to_char(MAX(to_date(SUBSTRING(split_part (file_name,'_',2),1,3) || SUBSTRING(split_part (file_name,'_',3),1,4),'MonYYYY')), 'YYYYMM')
              FROM payor.villagecare_all_census)                                        AS Latest_month,
             (SELECT MAX(day_of_member_effective_date)
              FROM payor.villagecare_all_census)                                        AS Effective_date,
             (SELECT COUNT(DISTINCT memberid)
              FROM payor.villagecare_all_census
              WHERE added_tz IN (SELECT MAX(added_tz) FROM payor.villagecare_all_census)) AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.villagecare_all_census)                                        AS Last_received
      UNION ALL
      SELECT 'MetroPlusHealth'                                                          AS MCO,
             'Somos'                                                                    AS IPA,
             'Roster'                                                                   AS FeedType,
             (SELECT COUNT(*) FROM payor.metroplus_all_eligibility)                     AS Total_count,
             (SELECT to_char(MIN(added_tz), 'YYYYMM')
              FROM payor.metroplus_all_eligibility)                                     AS Earliest_month,
             (SELECT to_char(MAX(added_tz), 'YYYYMM')
              FROM payor.metroplus_all_eligibility)                                     AS Latest_month,
             (SELECT to_date(MAX(SUBSTRING(split_part(file_name,'_',4),1,8)), 'YYYYMMDD')
              FROM payor.metroplus_all_eligibility)                                     AS Effective_date,
             (SELECT COUNT(DISTINCT member_id)
              FROM payor.metroplus_all_eligibility
              WHERE added_tz IN (SELECT MAX(added_tz) FROM payor.metroplus_all_eligibility)) AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.metroplus_all_eligibility)                                        AS Last_received
      UNION ALL
      SELECT 'VNS'                                                                    AS MCO,
             'Balance'                                                                AS IPA,
             'Roster'                                                                 AS FeedType,
             (SELECT COUNT(*) FROM payor.vns_all_eligibility)                         AS Total_count,
             (SELECT to_char(MIN(month), 'YYYYMM')
              FROM payor.vns_all_eligibility)                                         AS Earliest_month,
             (SELECT to_char(MAX(month), 'YYYYMM')
              FROM payor.vns_all_eligibility)                                         AS Latest_month,
             (SELECT MAX(enrollment_date)
              FROM payor.vns_all_eligibility)                                         AS Effective_date,
             (SELECT COUNT(DISTINCT subscriber_id)
              FROM payor.vns_all_eligibility
              WHERE month IN (SELECT MAX(month) FROM payor.vns_all_eligibility))      AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.vns_all_eligibility)                                         AS Last_received
      UNION ALL
      SELECT 'Emblem'                                                                   AS MCO,
             'Balance'                                                                  AS IPA,
             'Roster'                                                                   AS FeedType,
             (SELECT COUNT(*) FROM payor.emblem_balance_roster)                         AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.emblem_balance_roster)                                         AS Earliest_month,
             (SELECT Max(received_month)
              FROM payor.emblem_balance_roster)                                         AS Latest_month,
             (SELECT to_date(MAX(start_date),'YYYY-MM-DD')
              FROM payor.emblem_balance_roster)                                         AS Effective_date,
             (SELECT COUNT(DISTINCT mbr_id)
              FROM payor.emblem_balance_roster
              WHERE added_tz IN (SELECT MAX(added_tz) FROM payor.emblem_balance_roster)) AS Latest_month_member_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.emblem_balance_roster)                                         AS Last_received) a
ORDER BY 1