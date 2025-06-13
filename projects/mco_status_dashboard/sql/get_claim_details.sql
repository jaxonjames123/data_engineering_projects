SELECT *
FROM (SELECT 'Anthem'                                                                    AS MCO,
             'Balance'                                                                   AS IPA,
             'Claims'                                                                    AS FeedType,
             (SELECT COUNT(*) FROM payor.anthem_balance_claims)                          AS Total_count,
             (SELECT MIN(SUBSTRING(file_name,21,4) ||SUBSTRING(file_name,17,2))
              FROM payor.anthem_balance_claims)                                          AS Earliest_month,
             (SELECT MAX(SUBSTRING(file_name,21,4) ||SUBSTRING(file_name,17,2))
              FROM payor.anthem_balance_claims)                                          AS Latest_month,
             (SELECT MAX(CAST(activitydate AS DATE))
              FROM payor.anthem_balance_claims)                                          AS Activity_date,
             (SELECT COUNT(claimnumber)
              FROM payor.anthem_balance_claims
              WHERE SUBSTRING(file_name,21,4) ||SUBSTRING(file_name,17,2)
                    LIKE (Latest_month))                                                 AS Latest_month_claim_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.anthem_balance_claims)                                          AS last_received
      UNION ALL
      /* This is a big query because of the way anthem somos sends data. refer to the get_mco_details queries comments for anthem somos claims for info
         Extra code is basically to sort out the inaccurate dates they send in their filenames*/
      (WITH anthemsomosformatted AS (SELECT CASE
                WHEN file_name LIKE 'SOMO_MED%' THEN to_date(
                        SUBSTRING(split_part(file_name, '_', 3), 1, 8), 'YYYYMMDD')
                ELSE to_date(SUBSTRING(split_part(file_name, '_', 4), 1, 8), 'YYYYMMDD')
                END AS formonth,
            CASE
                WHEN EXTRACT(DAY FROM formonth) > 15
                    THEN DATEADD(month, 1, date_trunc('month', formonth))
                ELSE date_trunc('month', formonth)
                END AS realformonth,
            service_from_date,
            claim_number,
            added_tz
        FROM innovator.evolent_claim_data
        WHERE mco LIKE 'An%')
      SELECT 'Anthem'                                                              AS MCO,
           'Somos'                                                                 AS IPA,
           'Claims'                                                                AS FeedType,
           (SELECT COUNT(*) FROM anthemsomosformatted)                             AS Total_count,
           (SELECT to_char(MIN(realformonth), 'YYYYMM') FROM anthemsomosformatted) AS Earliest_month,
           (SELECT to_char(MAX(realformonth), 'YYYYMM') FROM anthemsomosformatted) AS Latest_month,
           (SELECT MAX(service_from_date) FROM anthemsomosformatted)               AS Activity_date,
           (SELECT COUNT(claim_number)
            FROM anthemsomosformatted
            WHERE realformonth IN
                  (SELECT MAX(realformonth) FROM anthemsomosformatted))            AS Latest_month_claim_count,
           (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
            FROM anthemsomosformatted)                                             AS last_received)
      UNION ALL
      SELECT 'Health First'                                             AS MCO,
             'Corinthian'                                               AS IPA,
             'Claims'                                                   AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'COR2')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'COR2')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'COR2')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'COR2')                           AS Activity_date,
             (SELECT COUNT(claim_id)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'COR2'
                AND received_month LIKE Latest_month)                   AS Latest_month_claim_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'COR2')                           AS last_received
      UNION ALL
      SELECT 'Health First'                                             AS MCO,
             'Excelsior'                                                AS IPA,
             'Claims'                                                   AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'EXC1')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'EXC1')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'EXC1')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code LIKE 'EXC1')                        AS Activity_date,
             (SELECT COUNT(claim_id)
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'EXC1'
                AND received_month LIKE Latest_month)                   AS Latest_month_claim_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_all_claims
              WHERE pcp_parent_code = 'EXC1')                           AS last_received
      UNION ALL
      SELECT 'Health First'                                                AS MCO,
             'Somos'                                                       AS IPA,
             'Claims'                                                      AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_somos_all_claims
              WHERE pcp_parent_code LIKE 'SOM%')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_somos_all_claims
              WHERE pcp_parent_code LIKE 'SOM%')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_somos_all_claims
              WHERE pcp_parent_code LIKE 'SOM%')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_somos_all_claims
              WHERE pcp_parent_code LIKE 'SOM%')                           AS Activity_date,
             (SELECT COUNT(claim_id)
              FROM payor.healthfirst_somos_all_claims
              WHERE pcp_parent_code LIKE 'SOM%'
                AND received_month LIKE Latest_month)                      AS Latest_month_claim_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_somos_all_claims
              WHERE pcp_parent_code LIKE 'SOM%')                           AS last_received
      UNION ALL
      SELECT 'United'                               AS MCO,
             'Somos'                                AS IPA,
             'Claims'                               AS FeedType,
             (SELECT COUNT(*)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc != 'PHARMACY')  AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc != 'PHARMACY'
                AND primary_svc_date != '99990909'
                AND received_month != '2017_2018') AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc != 'PHARMACY'
                AND primary_svc_date != '99990909') AS Latest_month,
             (SELECT MAX(primary_svc_date)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc != 'PHARMACY'
                AND primary_svc_date != '99990909') AS Activity_date,
             (SELECT COUNT(claim_number)
              FROM payor.uhc_somos_all_claims
              WHERE received_month LIKE Latest_month
                AND category_of_svc != 'PHARMACY')  AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc != 'PHARMACY')  AS last_received
      UNION ALL
      SELECT 'Wellcare'                                          AS MCO,
             'Balance'                                        AS IPA,
             'Claims'                                            AS FeedType,
             (SELECT COUNT(*)
              FROM payor.wellcare_all_claims
              WHERE master_ipa = 'VR1')                          AS Total_count,
             (SELECT MIN(receivedmonth)
              FROM payor.wellcare_all_claims
              WHERE master_ipa = 'VR1')                          AS Earliest_month,
             (SELECT MAX(receivedmonth)
              FROM payor.wellcare_all_claims
              WHERE master_ipa = 'VR1')                          AS Latest_month,
             (SELECT MAX(activity_date)
              FROM payor.wellcare_all_claims
              WHERE master_ipa = 'VR1')                          AS Activity_date,
             (SELECT COUNT(claim_number)
              FROM payor.wellcare_all_claims
              WHERE master_ipa = 'VR1'
                AND receivedmonth LIKE Latest_month)             AS Latest_month_claim_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.wellcare_all_claims
              WHERE master_ipa = 'VR1')                          AS last_received
      UNION ALL
      SELECT 'Fidelis'                                                               AS MCO,
             'Somos'                                                                 AS IPA,
             'Claims'                                                                AS FeedType,
             (SELECT COUNT(*)
              FROM payor.fideliscare_prod_ipclaimline_somos_all)                     AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.fideliscare_prod_ipclaimline_somos_all)                     AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.fideliscare_prod_ipclaimline_somos_all)                     AS Latest_month,
             (SELECT MAX(service_date)
              FROM payor.fideliscare_prod_ipclaimline_somos_all)                     AS Activity_date,
             (SELECT COUNT(claim_id)
              FROM payor.fideliscare_prod_ipclaimline_somos_all
              WHERE received_month LIKE Latest_month) AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.fideliscare_prod_ipclaimline_somos_all)                     AS last_received
      UNION ALL
      SELECT 'Emblem'                                                                     AS MCO,
             'Somos'                                                                      AS IPA,
             'Claims'                                                                     AS FeedType,
             (SELECT COUNT(*) FROM innovator.evolent_claim_data WHERE mco LIKE 'Em%')     AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM innovator.evolent_claim_data
              WHERE mco LIKE 'Em%')                                                       AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',3),1,6))
              FROM innovator.evolent_claim_data
              WHERE mco LIKE 'Em%')                                                       AS Latest_month,
             (SELECT MAX(service_from_date) FROM innovator.evolent_claim_data
             WHERE mco LIKE 'Em%')                                                        AS Activity_date,
             (SELECT COUNT(claim_number)
              FROM innovator.evolent_claim_data
              WHERE mco LIKE 'Em%'
              AND SUBSTRING(split_part (file_name,'_',3),1,6) LIKE Latest_month)          AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM innovator.evolent_claim_data
              WHERE mco LIKE 'Em%')                                                       AS last_received
      UNION ALL
      SELECT 'Humana'                                                                AS MCO,
             'Somos'                                                                 AS IPA,
             'Claims'                                                                AS FeedType,
             (SELECT COUNT(*) FROM payor.humana_all_claims)                          AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.humana_all_claims)                                          AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.humana_all_claims)                                          AS Latest_month,
             (SELECT MAX(srv_frm_dt) FROM payor.humana_all_claims)                   AS Activity_date,
             (SELECT COUNT(claim_nbr)
              FROM payor.humana_all_claims
              WHERE received_month LIKE latest_month)                               AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.humana_all_claims)                                          AS last_received
      UNION ALL
        (WITH molinacomosallclaims AS (
            SELECT received_month, service_date AS effectiveperiod, claim_id, added_tz FROM payor.molina_prod_ipclaimline
            UNION ALL
            SELECT received_month, NULL as effectiveperiod, claim_id, added_tz FROM payor.molina_prod_ipclaimheader
            UNION ALL
            SELECT received_month, first_date_of_service AS effectiveperiod, claim_id, added_tz FROM payor.molina_prod_opclaimheader
            UNION ALL
            SELECT received_month, service_date AS effectiveperiod, claim_id, added_tz FROM payor.molina_prod_opclaimline
            UNION ALL
            SELECT received_month, first_date_of_service AS effectiveperiod, claim_id, added_tz FROM payor.molina_prod_profheader
            UNION ALL
            SELECT received_month, service_from_date AS effectiveperiod, claim_id, added_tz FROM payor.molina_prod_profline
        )
        SELECT 'Molina'                                                AS MCO,
           'Somos'                                                 AS IPA,
           'Claims'                                                AS FeedType,
           (SELECT COUNT(*) FROM molinacomosallclaims)             AS Total_count,
           (SELECT MIN(received_month)
            -- != '' is there because there is bad data with empty received_month
            FROM molinacomosallclaims WHERE received_month != '')  AS Earliest_month,
           (SELECT MAX(received_month)
            FROM molinacomosallclaims)                             AS Latest_month,
           (SELECT MAX(effectiveperiod) FROM molinacomosallclaims) AS Activity_date,
           (SELECT COUNT(claim_id)
            FROM molinacomosallclaims
            WHERE received_month IN (SELECT MAX(received_month)
                                     FROM molinacomosallclaims ))  AS Latest_month_claim_count,
           (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
            FROM molinacomosallclaims)                             AS last_received)
      UNION ALL
              SELECT 'MetroPlusHealth'                                                        AS MCO,
              'Somos'                                                                  AS IPA,
              'Claims'                                                                 AS FeedType,
              (SELECT COUNT(*) FROM payor.metroplus_all_claims)                        AS Total_count,
              (SELECT MIN(SUBSTRING(split_part(file_name, '_', 4), 1, 6))
              FROM payor.metroplus_all_claims)                                        AS Earliest_month,
              (SELECT MAX(SUBSTRING(split_part(file_name, '_', 4), 1, 6))
              FROM payor.metroplus_all_claims)                                        AS Latest_month,
              (SELECT MAX(to_date(effective_period, 'YYYYMM'))
              FROM payor.metroplus_all_claims)                                        AS Activity_date,
              (SELECT COUNT(claim_id)
              FROM payor.metroplus_all_claims
              WHERE SUBSTRING(split_part(file_name, '_', 4), 1, 6) LIKE Latest_month) AS Latest_month_claim_count,
              (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.metroplus_all_claims)                                        AS Last_received
      UNION ALL
        (WITH emblembalancecombined AS (
                        SELECT substring(split_part(file_name, '_', 8), 1, 6) AS received_month,
                                activity_date as effectiveperiod,
                                claim_id,
                                added_tz
                        FROM payor.emblem_balance_prof_claims
                        UNION ALL
                        SELECT substring(split_part(file_name, '_', 8), 1, 6) AS received_month,
                                activity_date as effectiveperiod,
                                claim_id,
                                added_tz
                        FROM payor.emblem_balance_facility_claims
                )
                SELECT 'Emblem'                                                AS MCO,
                'Balance'                                                 AS IPA,
                'Claims'                                                AS FeedType,
                (SELECT COUNT(*) FROM emblembalancecombined)             AS Total_count,
                (SELECT MIN(received_month)
                FROM emblembalancecombined)  AS Earliest_month,
                (SELECT MAX(received_month)
                FROM emblembalancecombined)                             AS Latest_month,
                (SELECT MAX(effectiveperiod) FROM emblembalancecombined) AS Activity_date,
                (SELECT COUNT(claim_id)
                FROM emblembalancecombined
                WHERE received_month IN (SELECT MAX(received_month)
                                        FROM emblembalancecombined ))  AS Latest_month_claim_count,
                (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
                FROM emblembalancecombined)                             AS last_received)) a
ORDER BY 1