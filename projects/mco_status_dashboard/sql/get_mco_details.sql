SELECT ipa,
       feed_type,
       for_month,
       cnt
FROM (SELECT ipa,
             feed_type,
             to_char(for_month,'YYYYMMDD') AS for_month,
             cnt AS cnt,
             rnk,
             DENSE_RANK() OVER (ORDER BY for_month DESC) AS rnk_month
      FROM ((WITH max_activity_date AS (
              SELECT filename, MAX(received_month) AS max_date
              FROM payor.healthfirst_all_eligibility
              WHERE added_tz >= dateadd(month,-6,getdate())
              AND filename IS NOT NULL
              GROUP BY filename
            )
            SELECT CASE
               WHEN hae.filename LIKE '%CORINTHIAN%' THEN 'HEALTHFIRST_CORINTHIAN'
               WHEN hae.filename LIKE '%Excelsior%' THEN 'HEALTHFIRST_EXCELSIOR'
                  END AS ipa,
                  'Eligibility' AS feed_type,
                  to_date('01' || split_part (hae.filename,'_',3),'DDMONYYYY') AS for_month,
                  COUNT(DISTINCT CASE WHEN to_char(effective_period, 'YYYYMM') = mad.max_date THEN member_id ELSE NULL END) / 100 AS CNT,
                  RANK() OVER (PARTITION BY split_part (hae.filename,'_',2) ORDER BY to_date ('01' || split_part (hae.filename,'_',3),'DDMONYYYY') DESC) AS rnk
            FROM payor.healthfirst_all_eligibility hae
            JOIN max_activity_date AS mad ON hae.filename = mad.filename
            WHERE hae.filename IS NOT NULL
            GROUP BY hae.filename)
            UNION ALL
            (WITH max_activity_date AS (
              SELECT filename, MAX(received_month) AS max_date
              FROM payor.healthfirst_somos_all_eligibility
              WHERE added_tz >= dateadd(month,-6,getdate())
              AND filename IS NOT NULL
              GROUP BY filename
            )
            SELECT 'HEALTHFIRST_SOMOS' AS IPA,
                   'Eligibility' AS feed_type,
                   to_date(MAX(received_month), 'YYYYMM') AS for_month,
                   COUNT(DISTINCT CASE WHEN to_char(effective_period, 'YYYYMM') = mad.max_date THEN member_id ELSE NULL END) / 100 AS cnt,
                   RANK() OVER (PARTITION BY split_part (hsae.filename,'_',2) ORDER BY to_date ('01' || split_part (hsae.filename,'_',3),'DDMONYYYY') DESC) AS rnk
            FROM payor.healthfirst_somos_all_eligibility hsae
            JOIN max_activity_date AS mad ON hsae.filename = mad.filename
            WHERE hsae.filename IS NOT NULL
            AND provider_parent_code LIKE 'SOM%'
            GROUP BY hsae.filename)
            UNION ALL
            SELECT CASE
                     WHEN filename LIKE '%CORINTHIAN%' THEN 'HEALTHFIRST_CORINTHIAN'
                     WHEN filename LIKE '%Excelsior%' THEN 'HEALTHFIRST_EXCELSIOR'
                   END AS ipa,
                   'Claim' AS feed_type,
                   to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS CNT,
                   RANK() OVER (PARTITION BY split_part (filename,'_',2) ORDER BY to_date ('01' || split_part (filename,'_',3),'DDMONYYYY') DESC) AS rnk
            FROM payor.healthfirst_all_claims
            WHERE filename IS NOT NULL
            AND added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'HEALTHFIRST_SOMOS' AS IPA,
                   'Claim' AS feed_type,
                   to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS CNT,
                   RANK() OVER (PARTITION BY split_part (filename,'_',2) ORDER BY to_date ('01' || split_part (filename,'_',3),'DDMONYYYY') DESC) AS rnk
            FROM payor.healthfirst_somos_all_claims
            WHERE filename IS NOT NULL
            AND added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'HEALTHFIRST_SOMOS' AS IPA,
                   'RXClaim' AS feed_type,
                   to_date('01' || split_part (filename,'_',3),'DDMONYYYY') AS for_month,
                   COUNT(*) / 1000 AS CNT,
                   RANK() OVER (PARTITION BY split_part (filename,'_',2) ORDER BY to_date ('01' || split_part (filename,'_',3),'DDMONYYYY') DESC) AS rnk
            FROM payor.healthfirst_somos_all_rx_claims
            WHERE filename IS NOT NULL
            AND added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT CASE
                     WHEN filename LIKE '%CORINTHIAN%' THEN 'HEALTHFIRST_CORINTHIAN'
                     WHEN filename LIKE '%Excelsior%' THEN 'HEALTHFIRST_EXCELSIOR'
                   END AS ipa,
                   'RXClaim' AS feed_type,
                   to_date('01' || split_part (filename,'_',3),'DDMONYYYY') AS for_month,
                   COUNT(*) / 1000 AS CNT,
                   RANK() OVER (PARTITION BY split_part (filename,'_',2) ORDER BY to_date ('01' || split_part (filename,'_',3),'DDMONYYYY') DESC) AS rnk
            FROM payor.healthfirst_all_rx_claims
            WHERE filename IS NOT NULL
            AND added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'UNITED_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(split_part (filename,'_',2) || '01','YYYYMMDD') AS for_month,
                   COUNT(*) / 100 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.uhc_somos_all_membership
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'UNITED_SOMOS' AS ipa,
                   'Claim' AS feed_type,
                   to_date(MAX(received_month),'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.uhc_somos_all_claims
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   claim_type != 'PHARM'
            GROUP BY filename
            UNION ALL
            SELECT 'UNITED_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(split_part (filename,'_',2) || '01','YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.uhc_somos_all_claims
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   claim_type = 'PHARM'
            GROUP BY filename
            UNION ALL
            (WITH max_activity_date AS (
              SELECT file_name, MAX(monthid) AS max_date
              FROM payor.evolent_fidelis_eligibility_data
              WHERE added_tz >= dateadd(month,-6,getdate())
              GROUP BY file_name
            )
            SELECT 'FIDELIS_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(MAX(monthid), 'YYYYMM') AS for_month,
                   COUNT(DISTINCT CASE WHEN efed.monthid = mad.max_date THEN memberid ELSE NULL END) / 100 AS cnt,
                   RANK() OVER (ORDER BY efed.file_name DESC) AS rnk
            FROM payor.evolent_fidelis_eligibility_data efed
            JOIN max_activity_date AS mad ON efed.file_name = mad.file_name
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY efed.file_name)
            UNION ALL
            SELECT 'FIDELIS_SOMOS' AS ipa,
                   'Claim' AS feed_type,
                   to_date(MAX(received_month), 'YYYYMM') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.fideliscare_prod_ipclaimline_somos_all
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'FIDELIS_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(MAX(received_month), 'YYYYMM') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.fideliscare_prod_pharmacy_somos_all
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'ANTHEM_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',3),1,6),'YYYYMM01') AS for_month,
                   COUNT(DISTINCT memberid) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM innovator.evolent_eligibility_data_final
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   SUBSTRING(employergroupid,1,2) = 'AN'
            GROUP BY file_name
            UNION ALL
            SELECT 'EMBLEM_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',3),1,6),'YYYYMM01') AS for_month,
                   COUNT(DISTINCT memberid) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM innovator.evolent_eligibility_data_final
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   SUBSTRING(employergroupid,1,2) = 'EM'
            GROUP BY file_name
            UNION ALL
            -- with clause is needed because there are two files for each month, so the rows with the same for_months must be grouped together
            -- first case statement is needed because the two files have different formats
            /* real_for_month is required because of the way anthem somos sends data. sometimes data comes at the end of a month but that should be classified as data for the beginning of next month
            this basically 'rounds' the date, if it is sent in the second half of the month, it will be classified as being for the 1st of the next month
            if data is sent in the first half, it is classified as belonging to the current month.
            this case also has to be done here so that the select clause can properly group by the rounded for_months
            ex. 12-26-2023 becomes 01-01-2024 while 12-04-2023 becomes 12-01-2023
*/
              (WITH anthemsomoscombined AS (
                     SELECT
                     CASE
                            WHEN file_name LIKE 'SOMO_MED%' THEN to_date(SUBSTRING(split_part(file_name, '_', 3), 1, 8), 'YYYYMMDD')
                            ELSE to_date(SUBSTRING(split_part (file_name,'_',4),1,8),'YYYYMMDD')
                     END          AS for_month,
                     COUNT(*) / 1000 AS cnt,
                     CASE
                            WHEN EXTRACT(DAY FROM for_month) > 15 THEN DATEADD(month, 1, date_trunc('month', for_month))
                            ELSE date_trunc('month', for_month)
                     END           AS real_for_month
                     FROM innovator.evolent_claim_data
                     WHERE added_tz >= dateadd(month,-6,getdate())
                     AND mco LIKE 'An%'
                     GROUP BY file_name
              )
              SELECT 'ANTHEM_SOMOS' AS ipa,
                     'Claim' AS feed_type,
                     real_for_month AS for_month,
                     SUM(cnt) AS cnt,
                     RANK() OVER (ORDER BY real_for_month DESC) AS rnk
              FROM anthemsomoscombined
              GROUP BY real_for_month)
            UNION ALL
              SELECT 'EMBLEM_SOMOS' AS ipa,
                   'Claim' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',3),1,6),'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM innovator.evolent_claim_data
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND mco LIKE 'Em%'
            GROUP BY file_name
            UNION ALL
            SELECT 'ANTHEM_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (filename,'_',4),1,6),'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM innovator.evolent_rx_feed t1
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   filename LIKE 'SOMO_ATHM%'
            GROUP BY filename
            UNION ALL
            SELECT 'EMBLEM_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (filename,'_',4),1,6),'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM innovator.evolent_rx_feed t1
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   filename LIKE 'SOMO_EMBL%'
            GROUP BY filename
            UNION ALL
            SELECT 'ANTHEM_BALANCE' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(SUBSTRING(file_name,26,4) ||SUBSTRING(file_name,22,2) || '01','YYYYMMDD') AS for_month,
                   COUNT(DISTINCT member_id) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.anthem_balance_eligibility
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND   file_name != 'anthen_balance_membership_2020_2022.csv'
            GROUP BY file_name
            UNION ALL
            SELECT 'ANTHEM_BALANCE' AS ipa,
                   'Claim' AS feed_type,
                   to_date(SUBSTRING(file_name,21,4) ||SUBSTRING(file_name,17,2) || '01','YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.anthem_balance_claims
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'ANTHEM_BALANCE' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(file_name,23,4) ||SUBSTRING(file_name,19,2) || '01','YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.anthem_balance_pharmacy
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'MOLINA_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',4),1,6),'YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.molina_prod_pharmacy
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            -- molina seems to be inaccurate because of how inconsistent their data is
            -- this is a big query because molina has 6 different tables for claims
            (WITH molinasomosall AS (
                SELECT to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                       COUNT(*) AS cnt
                FROM payor.molina_prod_ipclaimheader
                WHERE added_tz >= dateadd(month,-6,getdate())
                GROUP BY file_name
                UNION ALL
                SELECT to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                       COUNT(*) AS cnt
                FROM payor.molina_prod_ipclaimline
                WHERE added_tz >= dateadd(month,-6,getdate())
                GROUP BY file_name
                UNION ALL
                SELECT to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                       COUNT(*) AS cnt
                FROM payor.molina_prod_opclaimheader
                WHERE added_tz >= dateadd(month,-6,getdate())
                GROUP BY file_name
                UNION ALL
                SELECT to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                       COUNT(*) AS cnt
                FROM payor.molina_prod_opclaimline
                WHERE added_tz >= dateadd(month,-6,getdate())
                GROUP BY file_name
                UNION ALL
                SELECT to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                       COUNT(*) AS cnt
                FROM payor.molina_prod_profheader
                WHERE added_tz >= dateadd(month,-6,getdate())
                GROUP BY file_name
                UNION ALL
                SELECT to_date(MAX(received_month), 'YYYYMM01') AS for_month,
                       COUNT(*) AS cnt
                FROM payor.molina_prod_profline
                WHERE added_tz >= dateadd(month,-6,getdate())
                GROUP BY file_name
            )
            SELECT 'MOLINA_SOMOS' AS ipa,
                   'Claim' AS feed_type,
                    for_month,
                    SUM(cnt) / 1000 AS cnt,
                    RANK() OVER (ORDER BY for_month DESC) AS rnk
            FROM molinasomosall
            GROUP BY for_month)
            UNION ALL
            SELECT 'MOLINA_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',5) || split_part (file_name,'_',4),1,6),'YYYYMMDD') AS for_month,
                   COUNT(distinct member_id) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.molina_prod_membership
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'EMBLEM_BALANCE' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(MAX(received_month), 'YYYYMM') AS for_month,
                   COUNT(*) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.emblem_balance_roster
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            (WITH emblembalancecombined
                     AS (SELECT to_date(SUBSTRING(split_part(file_name, '_', 8), 1, 6), 'YYYYMMDD') AS for_month,
                                COUNT(*) / 1000                                                     AS cnt
                         FROM payor.emblem_balance_prof_claims
                         WHERE added_tz >= dateadd(month, -6, getdate())
                         GROUP BY file_name
                         UNION ALL
                         SELECT to_date(SUBSTRING(split_part(file_name, '_', 8), 1, 6), 'YYYYMMDD') AS for_month,
                                COUNT(*) / 1000                                                     AS cnt
                         FROM payor.emblem_balance_facility_claims
                         WHERE added_tz >= dateadd(month, -6, getdate())
                         GROUP BY file_name)
            SELECT 'EMBLEM_BALANCE'                      AS ipa,
                   'Claim'                               AS feed_type,
                   for_month,
                   SUM(cnt)                              AS cnt,
                   RANK() OVER (ORDER BY for_month DESC) AS rnk
            FROM emblembalancecombined
            GROUP BY for_month)
            UNION ALL
            SELECT 'EMBLEM_BALANCE' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',8),1,6),'YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.emblem_balance_pharmacy_claims
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'HUMANA_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',2),3),'YYYYMMDD') AS for_month,
                   COUNT(DISTINCT mbr_pid) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.humana_all_eligibility t1
            GROUP BY file_name
            UNION ALL
            SELECT 'HUMANA_SOMOS' AS ipa,
                   'Claim' AS feed_type,
                   to_date(MAX(received_month),'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.humana_all_claims t1
            GROUP BY file_name
            UNION ALL
            SELECT 'HUMANA_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',2),3),'YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.humana_all_rx t1
            GROUP BY file_name
            UNION ALL
            SELECT 'VNS_BALANCE' AS ipa,
                   'Eligibility' AS feed_type,
                    MAX(month) AS for_month,
                   COUNT(DISTINCT subscriber_id) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.vns_all_eligibility
            GROUP BY file_name
            UNION ALL
            (WITH max_activity_date AS (
              SELECT filename, MAX(activity_date) AS max_date
              FROM payor.wellcare_all_demographics
              WHERE added_tz >= dateadd(month,-6,getdate())
              AND master_ipa = 'VR1'
              GROUP BY filename
            )
            SELECT 'WELLCARE_BALANCE' AS ipa,
                  'Eligibility' AS feed_type,
                  to_date(MAX(receivedmonth), 'YYYYMM') AS for_month,
                  COUNT(DISTINCT CASE WHEN wad.activity_date = mad.max_date THEN subscriber_id ELSE NULL END) / 100 AS cnt,
                  RANK() OVER (ORDER BY wad.filename DESC) AS rnk
            FROM payor.wellcare_all_demographics wad
            JOIN max_activity_date AS mad ON wad.filename = mad.filename
            WHERE added_tz >= dateadd(month,-6,getdate())
            AND master_ipa = 'VR1'
            GROUP BY wad.filename)
            UNION ALL
            SELECT 'WELLCARE_BALANCE' AS ipa,
                   'Claim' AS feed_type,
                   to_date(MAX(receivedmonth), 'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.wellcare_all_claims
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'WELLCARE_BALANCE' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (filename,'-',4),1,6),'YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY filename DESC) AS rnk
            FROM payor.wellcare_all_rx
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY filename
            UNION ALL
            SELECT 'ELDERPLAN_BALANCE' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',3),1,6),'YYYYMM01') AS for_month,
                   COUNT(DISTINCT memberid) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.elderplan_roster
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
                    SELECT 'METROPLUS_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   to_date(to_char(MAX(added_tz), 'YYYYMM'), 'YYYYMM') AS for_month,
                   COUNT(DISTINCT member_id) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.metroplus_all_eligibility
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'METROPLUS_SOMOS' AS ipa,
                   'Claim' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',4),1,6),'YYYYMM01') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.metroplus_all_claims
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'METROPLUS_SOMOS' AS ipa,
                   'RXClaim' AS feed_type,
                   to_date(SUBSTRING(split_part (file_name,'_',4),1,6),'YYYYMMDD') AS for_month,
                   COUNT(*) / 1000 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.metroplus_all_rx
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT 'VILLAGECARE_SOMOS' AS ipa,
                   'Eligibility' AS feed_type,
                   CASE
                        WHEN SPLIT_PART(REPLACE(file_name, '_', ' '), ' ', 3) ~ '^[0-9]{4}$'
                        THEN TO_DATE(
                             LEFT(TRIM(SPLIT_PART(REPLACE(file_name, '_', ' '), ' ', 2)), 3) ||
                             TRIM(SPLIT_PART(REPLACE(file_name, '_', ' '), ' ', 3)),
                             'MonYYYY'
                             )
                        ELSE NULL
                   END AS for_month,
                   COUNT(*) / 100 AS cnt,
                   RANK() OVER (ORDER BY file_name DESC) AS rnk
            FROM payor.villagecare_all_census
            WHERE added_tz >= dateadd(month,-6,getdate())
            GROUP BY file_name
            UNION ALL
            SELECT ipa,
                   feed_type,
                   for_month,
                   cnt,
                   RANK() OVER (PARTITION BY ipa ORDER BY for_month DESC) AS rnk
            FROM (SELECT DISTINCT CASE
                           WHEN mco || '_' || ipa = 'Anthem_Balance' THEN 'ANTHEM_BALANCE'
                           WHEN mco || '_' || ipa = 'Anthem_SOMOS' THEN 'ANTHEM_SOMOS'
                           WHEN mco || '_' || ipa = 'Elderplan_Balance' THEN 'ELDERPLAN_BALANCE'
                           WHEN mco || '_' || ipa = 'Emblem_Balance' THEN 'EMBLEM_BALANCE'
                           WHEN mco || '_' || ipa = 'Emblem_Corinthian' THEN 'EMBLEM_CORINTHIAN'
                           WHEN mco || '_' || ipa = 'Emblem_SOMOS' THEN 'EMBLEM_SOMOS'
                           WHEN mco || '_' || ipa = 'Fidelis_SOMOS' THEN 'FIDELIS_SOMOS'
                           WHEN mco || '_' || ipa IN ('Healthfirst_COR2','Healthfirst_Corinthian') THEN 'HEALTHFIRST_CORINTHIAN'
                           WHEN mco || '_' || ipa IN ('Healthfirst_EXC1','Healthfirst_Excelsior') THEN 'HEALTHFIRST_EXCELSIOR'
                           WHEN mco || '_' || ipa = 'Healthfirst_SOMOS' THEN 'HEALTHFIRST_SOMOS'
                           WHEN mco || '_' || ipa = 'Humana_SOMOS' THEN 'HUMANA_SOMOS'
                           WHEN mco || '_' || ipa = 'United_SOMOS' THEN 'UNITED_SOMOS'
                           WHEN mco || '_' || ipa = 'Wellcare_Balance' THEN 'WELLCARE_BALANCE'
                           WHEN mco || '_' || ipa = 'Molina_SOMOS' THEN 'MOLINA_SOMOS'
                           WHEN mco || '_' || ipa = 'VNS_Balance' THEN 'VNS_BALANCE'
                           WHEN mco || '_' || ipa = 'MetroPlus_SOMOS' THEN 'METROPLUS_SOMOS'
                           WHEN mco || '_' || ipa = 'VillageCare_SOMOS' THEN 'VILLAGECARE_SOMOS'
                         END AS ipa,
                         'GIC' AS feed_type,
                         to_date(SUBSTRING(split_part (file_name,'_',5),1,6) || '01','YYYYMMDD') AS for_month,
                         COUNT(*) / 100 AS cnt
                  FROM caregaps.gic_all
                  GROUP BY CASE
                                               WHEN mco || '_' || ipa = 'Anthem_Balance' THEN 'ANTHEM_BALANCE'
                                               WHEN mco || '_' || ipa = 'Anthem_SOMOS' THEN 'ANTHEM_SOMOS'
                                               WHEN mco || '_' || ipa = 'Elderplan_Balance' THEN 'ELDERPLAN_BALANCE'
                                               WHEN mco || '_' || ipa = 'Emblem_Balance' THEN 'EMBLEM_BALANCE'
                                               WHEN mco || '_' || ipa = 'Emblem_Corinthian' THEN 'EMBLEM_CORINTHIAN'
                                               WHEN mco || '_' || ipa = 'Emblem_SOMOS' THEN 'EMBLEM_SOMOS'
                                               WHEN mco || '_' || ipa = 'Fidelis_SOMOS' THEN 'FIDELIS_SOMOS'
                                               WHEN mco || '_' || ipa IN ('Healthfirst_COR2','Healthfirst_Corinthian') THEN 'HEALTHFIRST_CORINTHIAN'
                                               WHEN mco || '_' || ipa IN ('Healthfirst_EXC1','Healthfirst_Excelsior') THEN 'HEALTHFIRST_EXCELSIOR'
                                               WHEN mco || '_' || ipa = 'Healthfirst_SOMOS' THEN 'HEALTHFIRST_SOMOS'
                                               WHEN mco || '_' || ipa = 'Humana_SOMOS' THEN 'HUMANA_SOMOS'
                                               WHEN mco || '_' || ipa = 'United_SOMOS' THEN 'UNITED_SOMOS'
                                               WHEN mco || '_' || ipa = 'Wellcare_Balance' THEN 'WELLCARE_BALANCE'
                                               WHEN mco || '_' || ipa = 'Molina_SOMOS' THEN 'MOLINA_SOMOS'
                                               WHEN mco || '_' || ipa = 'VNS_Balance' THEN 'VNS_BALANCE'
                                               WHEN mco || '_' || ipa = 'MetroPlus_SOMOS' THEN 'METROPLUS_SOMOS'
                                               WHEN mco || '_' || ipa = 'VillageCare_SOMOS' THEN 'VILLAGECARE_SOMOS'
                                             END
                  ,
                           for_month)
            WHERE for_month >= '20220101'
            AND   ipa IS NOT NULL) A
      WHERE RNK <= 5)
WHERE rnk_month <= 5