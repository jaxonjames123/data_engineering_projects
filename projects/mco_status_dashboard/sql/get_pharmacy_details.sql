SELECT *
FROM (SELECT 'Anthem'                                                                       AS MCO,
             'Balance'                                                                      AS IPA,
             'Pharmacy Claims'                                                              AS FeedType,
             (SELECT COUNT(*) FROM payor.anthem_balance_pharmacy)                          AS Total_count,
             (SELECT MIN(SUBSTRING(file_name,23,4) || SUBSTRING(file_name,19,2))
              FROM payor.anthem_balance_pharmacy)                                          AS Earliest_month,
             (SELECT MAX(SUBSTRING(file_name,23,4) || SUBSTRING(file_name,19,2))
              FROM payor.anthem_balance_pharmacy)                                          AS Latest_month,
             (SELECT to_date(MAX(datefilled),'YYYY-MM-DD') FROM payor.anthem_balance_pharmacy)  AS Effective_date,
             (SELECT COUNT(claimnumber)
              FROM payor.anthem_balance_pharmacy
              WHERE SUBSTRING(file_name,23,4) || SUBSTRING(file_name,19,2)
              LIKE Latest_month)                                                           AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.anthem_balance_pharmacy)                                          AS last_received
      UNION ALL
      SELECT 'Anthem'                                                                  AS MCO,
             'Somos'                                                                   AS IPA,
             'Pharmacy Claims'                                                         AS FeedType,
             (SELECT COUNT(*) FROM innovator.evolent_rx_feed
                WHERE filename LIKE 'SOMO_ATHM%')                                      AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (filename,'_',4),1,6))
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_ATHM%')         AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (filename,'_',4),1,6))
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_ATHM%')         AS Latest_month,
             (SELECT MAX(CAST(fill_date AS DATE))
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_ATHM%')         AS Effective_date,
             (SELECT COUNT(claim_number)
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_ATHM%'
              AND SUBSTRING(split_part (filename,'_',4),1,6)
                 LIKE Latest_month)                                                    AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_ATHM%')         AS last_received
      UNION ALL
      SELECT 'Health First'                                                  AS MCO,
             'Corinthian'                                                    AS IPA,
             'Pharmacy Claims'                                               AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'COR2')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'COR2')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'COR2')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'COR2')                           AS Effective_date,
             (SELECT COUNT(claim_id)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'COR2'
                AND received_month LIKE Latest_month)                        AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'COR2')                           AS last_received
      UNION ALL
      SELECT 'Health First'                                                  AS MCO,
             'Excelsior'                                                     AS IPA,
             'Pharmacy Claims'                                               AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'EXC1')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'EXC1')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'EXC1')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'EXC1')                           AS Effective_date,
             (SELECT COUNT(claim_id)
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'EXC1'
                AND received_month LIKE Latest_month)                        AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_all_rx_claims
              WHERE provider_parent_code = 'EXC1')                           AS last_received
      UNION ALL
      SELECT 'Health First'                                                     AS MCO,
             'Somos'                                                            AS IPA,
             'Pharmacy Claims'                                                  AS FeedType,
             (SELECT COUNT(*)
              FROM payor.healthfirst_somos_all_rx_claims
              WHERE provider_parent_code LIKE 'SOM%')                           AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.healthfirst_somos_all_rx_claims
              WHERE provider_parent_code LIKE 'SOM%')                           AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.healthfirst_somos_all_rx_claims
              WHERE provider_parent_code LIKE 'SOM%')                           AS Latest_month,
             (SELECT MAX(effective_period)
              FROM payor.healthfirst_somos_all_rx_claims
              WHERE provider_parent_code LIKE 'SOM%')                           AS Effective_date,
             (SELECT COUNT(claim_id)
              FROM payor.healthfirst_somos_all_rx_claims
              WHERE provider_parent_code LIKE 'SOM%'
                AND received_month LIKE Latest_month)                           AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.healthfirst_somos_all_rx_claims
              WHERE provider_parent_code LIKE 'SOM%')                           AS last_received
      UNION ALL
      SELECT 'United'                                                                           AS MCO,
             'SOMOS'                                                                            AS IPA,
             'Pharmacy Claims'                                                                  AS FeedType,
             (SELECT COUNT(*)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc = 'PHARMACY')                                               AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc = 'PHARMACY' AND received_month != '2017_2018')             AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc = 'PHARMACY')                                               AS Latest_month,
             (SELECT MAX(date_received)
              FROM payor.uhc_somos_all_claims WHERE date_received != '9999=09-09')              AS Activity_date,
             (SELECT COUNT(claim_number)
              FROM payor.uhc_somos_all_claims
              WHERE received_month LIKE Latest_month
                AND category_of_svc = 'PHARMACY')                                               AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.uhc_somos_all_claims
              WHERE category_of_svc = 'PHARMACY')                                               AS last_received
      UNION ALL
      SELECT 'Wellcare'                                          AS MCO,
             'Balance'                                           AS IPA,
             'Pharmacy Claims'                                   AS FeedType,
             (SELECT COUNT(*)
              FROM payor.wellcare_all_rx
              WHERE master_ipa = 'VR1')                          AS Total_count,
             (SELECT MIN(receivedmonth)
              FROM payor.wellcare_all_rx
              WHERE master_ipa = 'VR1')                          AS Earliest_month,
             (SELECT MAX(receivedmonth)
              FROM payor.wellcare_all_rx
              WHERE master_ipa = 'VR1')                          AS Latest_month,
             (SELECT MAX(date_filled)
              FROM payor.wellcare_all_rx
              WHERE master_ipa = 'VR1')                          AS Effective_date,
             (SELECT COUNT(*)
              FROM payor.wellcare_all_rx
              WHERE master_ipa = 'VR1'
                AND receivedmonth LIKE Latest_month)             AS Latest_month_claim_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.wellcare_all_rx
              WHERE master_ipa = 'VR1')                          AS last_received
      UNION ALL
      SELECT 'Fidelis'                                                            AS MCO,
             'SOMOS'                                                              AS IPA,
             'Pharmacy Claims'                                                    AS FeedType,
             (SELECT COUNT(*)
              FROM payor.fideliscare_prod_pharmacy_somos_all)                     AS Total_count,
             (SELECT MIN(received_month)
              FROM payor.fideliscare_prod_pharmacy_somos_all)                     AS Earliest_month,
             (SELECT MAX(received_month)
              FROM payor.fideliscare_prod_pharmacy_somos_all)                     AS Latest_month,
             (SELECT MAX(CAST(filled_date AS DATE))
              FROM payor.fideliscare_prod_pharmacy_somos_all)                     AS Effective_date,
             (SELECT COUNT(claim_id)
              FROM payor.fideliscare_prod_pharmacy_somos_all
              WHERE received_month LIKE Latest_month)                             AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.fideliscare_prod_pharmacy_somos_all)                     AS last_received
      UNION ALL
      SELECT 'Evolent'                                                                 AS MCO,
             'SOMOS'                                                                   AS IPA,
             'Pharmacy Claims'                                                         AS FeedType,
             (SELECT COUNT(*) FROM innovator.evolent_rx_feed)                          AS Total_count,
             (SELECT MIN(to_char(CAST(added_tz AS DATE), 'YYYYMM'))
              FROM innovator.evolent_rx_feed)                                          AS Earliest_month,
             (SELECT MAX(to_char(CAST(added_tz AS DATE), 'YYYYMM'))
              FROM innovator.evolent_rx_feed)                                          AS Latest_month,
             (SELECT MAX(fill_date) FROM innovator.evolent_rx_feed)                    AS Effective_date,
             (SELECT COUNT(DISTINCT claim_number)
              FROM innovator.evolent_rx_feed
              WHERE added_tz IN (SELECT MAX(added_tz) FROM innovator.evolent_rx_feed)) AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM innovator.evolent_rx_feed)                                          AS last_received
      UNION ALL
      SELECT 'Humana'                                                            AS MCO,
             'SOMOS'                                                             AS IPA,
             'Pharmacy Claims'                                                   AS FeedType,
             (SELECT COUNT(*) FROM payor.humana_all_rx)                          AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',2),3,6))
              FROM payor.humana_all_rx)                                          AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',2),3,6))
              FROM payor.humana_all_rx)                                          AS Latest_month,
             (SELECT MAX(service_dt) FROM payor.humana_all_rx)                   AS Effective_date,
             (SELECT COUNT(cas_clmnbr)
              FROM payor.humana_all_rx
              WHERE SUBSTRING(split_part (file_name,'_',2),3,6)
                    LIKE Latest_month)                                           AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM payor.humana_all_rx)                                          AS last_received
      UNION ALL
      SELECT 'Emblem'                                                                 AS MCO,
             'SOMOS'                                                                  AS IPA,
             'Pharmacy Claims'                                                        AS FeedType,
             (SELECT COUNT(*) FROM innovator.evolent_rx_feed
             WHERE filename LIKE 'SOMO_EMBL%')                                        AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (filename,'_',4),1,6))
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_EMBL%')        AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (filename,'_',4),1,6))
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_EMBL%')        AS Latest_month,
             (SELECT MAX(fill_date)
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_EMBL%')        AS Effective_date,
             (SELECT COUNT(DISTINCT claim_number)
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_EMBL%'
              AND added_tz IN (SELECT MAX(added_tz) FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_EMBL%')) AS Latest_month_claim_count,
             (SELECT to_char(MAX(added_tz), 'YYYY-MM-DD')
              FROM innovator.evolent_rx_feed WHERE filename LIKE 'SOMO_EMBL%')   AS last_received
      UNION ALL
      SELECT 'MetroPlusHealth'                                                        AS MCO,
             'SOMOS'                                                                  AS IPA,
             'Pharmacy Claims'                                                        AS FeedType,
             (SELECT COUNT(*) FROM payor.metroplus_all_rx)                            AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',4),1,6))
              FROM payor.metroplus_all_rx)                                            AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',4),1,6))
              FROM payor.metroplus_all_rx)                                            AS Latest_month,
             (SELECT MAX( -- case is needed because metroplus changed format randomly at the beginning of 2024
                    CASE
                        WHEN added_tz <= '2024-01-01' THEN to_date(date_written, 'YYYY-MM-DD')
                        ELSE to_date(date_written, 'Mon DD YYYY')
                    END) FROM payor.metroplus_all_rx)                                 AS Effective_date,
             (SELECT COUNT(prescription_number)
              FROM payor.metroplus_all_rx
              WHERE SUBSTRING(split_part (file_name,'_',4),1,6)
                 LIKE Latest_month)                                                   AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.metroplus_all_rx)                                            AS last_received
    UNION ALL
      SELECT 'Emblem'                                                                             AS MCO,
             'Balance'                                                                            AS IPA,
             'Pharmacy Claims'                                                                    AS FeedType,
             (SELECT COUNT(*) FROM payor.emblem_balance_pharmacy_claims)                          AS Total_count,
             (SELECT to_char(MIN(to_date(SUBSTRING(split_part (file_name,'_',8),1,6), 'YYYYMM')),'YYYYMM')
              FROM payor.emblem_balance_pharmacy_claims)                                          AS Earliest_month,
             (SELECT to_char(MAX(to_date(SUBSTRING(split_part (file_name,'_',8),1,6), 'YYYYMM')),'YYYYMM')
              FROM payor.emblem_balance_pharmacy_claims)                                          AS Latest_month,
             (SELECT MAX(rx_dt) FROM payor.emblem_balance_pharmacy_claims)                        AS Effective_date,
             (SELECT COUNT(DISTINCT clm_nbr)
              FROM payor.emblem_balance_pharmacy_claims
              WHERE added_tz IN (SELECT MAX(added_tz) FROM payor.emblem_balance_pharmacy_claims)) AS Latest_month_rx_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM payor.emblem_balance_pharmacy_claims)                                          AS last_received) a
ORDER BY 1