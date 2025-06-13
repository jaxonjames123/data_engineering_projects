SELECT       '{mco}'                                        AS MCO,
             '{ipa}'                                        AS IPA,
             'GIC'                                          AS FeedType,
             (SELECT COUNT(*)
              FROM caregaps.gic_all
              WHERE mco = '{mco}'
                AND ipa = '{ipa}')                          AS Total_count,
             (SELECT MIN(SUBSTRING(split_part (file_name,'_',5),1,6))
              FROM caregaps.gic_all
              WHERE mco = '{mco}'
                AND ipa = '{ipa}')                          AS Earliest_month,
             (SELECT MAX(SUBSTRING(split_part (file_name,'_',5),1,6))
              FROM caregaps.gic_all
              WHERE mco = '{mco}'
                AND ipa = '{ipa}')                          AS Latest_month,
             (SELECT MAX(claim_thru_date)
              FROM caregaps.gic_all
              WHERE mco = '{mco}'
                AND ipa = '{ipa}')                          AS Effective_date,
             (SELECT COUNT(numerator)
              FROM caregaps.gic_all
              WHERE mco = '{mco}'
                AND ipa = '{ipa}'
                AND SUBSTRING(split_part (file_name,'_',5),1,6)
                 LIKE Latest_month)                         AS latest_gic_count,
             (SELECT CAST(trunc(MAX(added_tz)) AS VARCHAR(20))
              FROM caregaps.gic_all
              WHERE mco = '{mco}'
                AND ipa = '{ipa}')                          AS last_received