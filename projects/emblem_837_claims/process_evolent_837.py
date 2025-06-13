import pandas as pd
import pyodbc
from s3fs.core import S3FileSystem
import sys
import os

conn=pyodbc.connect(dsn="somos_redshift_1")
os.environ['AWS_CONFIG_FILE'] = 'aws_config.ini'
s3=S3FileSystem(anon=False)

filename=sys.argv[1]

var = 'p' if 'P' in filename else 'i'
source_type= "Anthem" if filename.startswith("SOMOS1NY") else "Emblem"
#file_ext=filename.split("-")[4] if source_type == "Anthem" else filename.split("_")[5]
#file_type="FULL" if file_ext == "FULL" or file_ext == "FW01" else "INCR"

def process_file(output_f):
    df=pd.read_csv("/home/etl/etl_home/input/sevolent/"+output_f,sep="~")
    df.to_csv("/home/etl/etl_home/temp/" + output_f,index=True,sep=",",header=False)
    s3.put("/home/etl/etl_home/temp/" + output_f,"sftp_test/837/"+output_f)


def process_db(output_f,source,v):

    q_del="""delete innovator.claims_{0}_837_parsed_proc where file_name = '{1}';
            delete innovator.claims_{0}_837_parsed_header where file_name = '{1}';
            delete innovator.claims_{0}_837_parsed_drugs where file_name = '{1}';
            delete innovator.claims_{0}_837_parsed_line_adjustments where file_name = '{1}';
            delete innovator.claims_{0}_837_parsed_line_adjudication where file_name = '{1}';
            """
    #conn.execute(q_del.format(v,output_f))
    #conn.commit()

    q1="""
        drop table if exists temp_837;
        create temporary table temp_837 (
        rid int,
        raw_data varchar(max)
        );

        copy temp_837
        from 's3://sftp_test/837/{0}'
        iam_role 'arn:aws:iam::042108671686:role/myRedshiftRole'
        csv;
    """
    print('q1',output_f)
    conn.execute(q1.format(output_f))

    q2="""
        drop table if exists anthem_837_staging;
        create temporary table anthem_837_staging as
            select rid,
            split_part(raw_data,'*',1) as fields_0_content,
            split_part(raw_data,'*',2) as fields_1_content,
            split_part(raw_data,'*',3) as fields_2_content,
            split_part(raw_data,'*',4) as fields_3_content,
            split_part(raw_data,'*',5) as fields_4_content,
            split_part(raw_data,'*',6) as fields_5_content,
            split_part(raw_data,'*',7) as fields_6_content,
            split_part(raw_data,'*',8) as fields_7_content,
            split_part(raw_data,'*',9) as fields_8_content,
            split_part(raw_data,'*',10) as fields_9_content,
            split_part(raw_data,'*',11) as fields_10_content,
            split_part(raw_data,'*',12) as fields_11_content,
            split_part(raw_data,'*',13) as fields_12_content,
            split_part(raw_data,'*',14) as fields_13_content,
            split_part(raw_data,'*',15) as fields_14_content,
            split_part(raw_data,'*',16) as fields_15_content,
            split_part(raw_data,'*',17) as fields_16_content
        from temp_837
        where rid is not null;
        drop table if exists temp_clm_rows;
        CREATE TEMPORARY TABLE TEMP_CLM_ROWS
        AS
        SELECT FIELDS_1_CONTENT::VARCHAR(50) AS CLAIM_NUMBER, RID AS CLM_ROW_ID, ISNULL(LEAD( RID) OVER( ORDER BY RID),RID+50)                                                                                                 AS CLM_NEXT_ROW_ID
            FROM anthem_837_staging
            WHERE FIELDS_0_CONTENT = 'CLM';

    """
    print('q2',output_f)
    conn.execute(q2)

    q3="""
        set statement_timeout to 0;
        drop table if exists t11;
        create temporary table t11
        as

        -- RENDERING PROVIDER INFO
        WITH CTE_RENDER AS (
        SELECT T1.RID AS RENDERING_RID,T1.FIELDS_3_CONTENT AS RENDERING_PROVIDER_NAME, T1.FIELDS_9_CONTENT AS RENDERING_PROVID                                                                                                ER_NPI,
                T2.FIELDS_3_CONTENT AS RENDERING_PROVIDER_TAXONOMY
        FROM ANTHEM_837_STAGING T1
        INNER JOIN ANTHEM_837_STAGING T2
        ON T2.RID = T1.RID +1
        AND T2.FIELDS_1_CONTENT = 'PE'
        AND T2.FIELDS_2_CONTENT = 'PXC'
        WHERE T1.FIELDS_0_CONTENT = 'NM1'
        AND T1.FIELDS_1_CONTENT = '82'  ) ,

        CTE_2310AB AS (
                    SELECT CLAIM_NUMBER,T2.*
                            FROM TEMP_CLM_ROWS T1
                            LEFT JOIN CTE_RENDER T2
                            ON RENDERING_RID BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID  ),


        -- SERVICE LOCATION INFO
        CTE_SERVICE AS ( SELECT T1.RID AS SERVICE_LOCATION_RID,T1.FIELDS_3_CONTENT AS SERVICE_LOCATION_NAME,T1.FIELDS_9_CONTENT                                                                                                 AS SERVICE_LOCATION_NPI,
                        T2.FIELDS_1_CONTENT AS SERVICE_ADDRESS_1,T2.FIELDS_2_CONTENT AS SERVICE_ADDRESS_2,
                        T3.FIELDS_1_CONTENT AS SERVICE_CITY, T3.FIELDS_2_CONTENT AS SERVICE_STATE, T3.FIELDS_3_CONTENT AS SERV                                                                                                ICE_ZIP
                        FROM ANTHEM_837_STAGING T1
                        INNER JOIN ANTHEM_837_STAGING T2
                        ON T2.RID = T1.RID +1
                        AND T2.FIELDS_0_CONTENT = 'N3'
                        INNER JOIN ANTHEM_837_STAGING T3
                        ON T3.RID = T1.RID + 2
                        AND T3.FIELDS_0_CONTENT = 'N4'
                        WHERE T1.FIELDS_0_CONTENT = 'NM1'
                        AND T1.FIELDS_1_CONTENT = '77'   ),

        CTE_2310C AS (SELECT CLAIM_NUMBER,SER.*
                        FROM TEMP_CLM_ROWS T1
                        LEFT JOIN CTE_SERVICE SER
                        ON SERVICE_LOCATION_RID BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID  ),


        --LOOP 2000 A
        CTE_BILL_1 AS ( SELECT RID AS BILL_RID ,
                        CASE WHEN FIELDS_2_CONTENT = '1' THEN 'Individual'
                            WHEN FIELDS_2_CONTENT = '2' THEN 'Organization' END AS BILLING_PROV_TYPE,
                        CASE WHEN FIELDS_2_CONTENT = '1' THEN FIELDS_4_CONTENT || ' ' || FIELDS_5_CONTENT || ' ' || FIELDS_3_CONT                                                                                                ENT
                            WHEN FIELDS_2_CONTENT = '2' THEN FIELDS_3_CONTENT END BILLER_NAME,
                        CASE WHEN FIELDS_2_CONTENT IN ('1','2') THEN  FIELDS_9_CONTENT  END AS BILLING_NPI,
                        ISNULL(LEAD( RID) OVER( ORDER BY RID),RID+50000) AS BILL_NEXT_RID
                        FROM ANTHEM_837_STAGING
                        WHERE FIELDS_0_CONTENT = 'NM1' AND FIELDS_1_CONTENT = '85'
                        ),

        CTE_BILL_2 AS ( SELECT CB1.*,CB2.FIELDS_1_CONTENT AS BILL_ADDRESS,CB3.FIELDS_1_CONTENT AS BILL_CITY,CB3.FIELDS_2_CONTEN                                                                                                T AS BILL_STATE,
                        CB3.FIELDS_3_CONTENT AS BILL_ZIP,
                        CB4.FIELDS_3_CONTENT AS BILLING_PROVIDER_TAXONOMY,CB5.FIELDS_2_CONTENT  AS BILLING_TIN
                        FROM CTE_BILL_1 CB1
                        INNER JOIN ANTHEM_837_STAGING CB2
                        ON BILL_RID +1 = CB2.RID
                        INNER JOIN ANTHEM_837_STAGING CB3
                        ON BILL_RID +2 = CB3.RID
                        INNER JOIN ANTHEM_837_STAGING CB4
                        ON BILL_RID - 1 = CB4.RID
                        AND CB4.FIELDS_0_CONTENT = 'PRV'
                        AND CB4.FIELDS_1_CONTENT = 'BI'
                        INNER JOIN ANTHEM_837_STAGING CB5
                        ON CB5.RID BETWEEN BILL_RID AND BILL_NEXT_RID
                        AND CB5.FIELDS_0_CONTENT = 'REF'
                        AND CB5.FIELDS_1_CONTENT = 'EI' ) ,

        CTE_2000A AS (
                        SELECT CLAIM_NUMBER, BILL.*
                        FROM TEMP_CLM_ROWS CLM
                        LEFT JOIN CTE_BILL_2 BILL
                        ON CLM_ROW_ID BETWEEN BILL_RID AND BILL_NEXT_RID
                    ),

        CTE_CLM AS ( SELECT RID AS CLM_RID,FIELDS_1_CONTENT AS CLAIM_NUMBER,
                    CAST(FIELDS_2_CONTENT AS FLOAT)  AS CLAIM_AMOUNT,
                    CASE WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '11' THEN 'Office'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '02' THEN 'Telehealth'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '03' THEN 'School'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '04' THEN 'Homeless Shelter'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '09' THEN 'Prison/Correctional Facility'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '12' THEN 'Home'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '13' THEN 'Assisted Living Facility'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '14' THEN 'Group Home'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '15' THEN 'Mobile Unit'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '16' THEN 'Temporary Lodging'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '21' THEN 'Hospital'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '22' THEN 'Outpatient Hospital'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '26' THEN 'Military Treatment Facility'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '311' THEN 'Skilled Nursing Facility'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '32' THEN 'Nursing Facility'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '34' THEN 'Hospice'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '49' THEN 'Independent Clinic'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '53' THEN 'Community Mental Health Center'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '62' THEN 'Outpatient Rehab'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',1) = '99' THEN 'Other Setting' END as place_of_service,
                    'B' AS PROFESSIONAL_CLAIM,
                    CASE WHEN SPLIT_PART(FIELDS_5_CONTENT,':',3) = '1' THEN 'Original Claim'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',3) = '6' THEN 'Corrected Claim'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',3) = '7' THEN 'Replacement of Prior Claim'
                            WHEN SPLIT_PART(FIELDS_5_CONTENT,':',3) = '8' THEN 'Void/Cancel Prior Claim'  END as claim_frequency_                                                                                                type,
                    FIELDS_6_CONTENT AS PROVIDER_SIGNATURE_INDICATOR,
                    CASE WHEN FIELDS_7_CONTENT = 'A' then 'Assigned'
                            WHEN FIELDS_7_CONTENT = 'C' THEN 'Not Assigned' END AS PROVIDER_ACCEPT_ASSIGNMENT,
                    CASE WHEN FIELDS_8_CONTENT = 'Y' THEN 'Valid'
                            WHEN FIELDS_8_CONTENT = 'N' THEN 'Not Valid' END AS  PROVIDER_BENEFITS_ASSIGNMENT,
                    CASE WHEN FIELDS_9_CONTENT = 'Y' THEN 'Valid'
                            WHEN FIELDS_9_CONTENT = 'N' THEN 'Not Valid' END AS  RELEASE_INFO_I,
                    FIELDS_10_CONTENT AS CLIENT_SIGNATURE_SOURCE,
                    CASE WHEN FIELDS_11_CONTENT = 'EM' THEN 'Employment'
                            WHEN FIELDS_11_CONTENT = 'AA' THEN 'Auto Accident'
                            WHEN FIELDS_11_CONTENT = 'OA' THEN ' Other Accident' END AS RELATED_CAUSE,
                    ISNULL(LEAD( RID) OVER( ORDER BY RID),RID+50) AS CLM_NEXT_RID
                    FROM ANTHEM_837_STAGING
                    WHERE FIELDS_0_CONTENT = 'CLM' ),

        CTE_HI AS ( SELECT T1.*,SPLIT_PART(FIELDS_1_CONTENT,':',2) AS PRIMARY_DIAGNOSIS_CODE,
                    SPLIT_PART(FIELDS_2_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE1,
                    SPLIT_PART(FIELDS_3_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE2,
                    SPLIT_PART(FIELDS_4_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE3,
                    SPLIT_PART(FIELDS_5_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE4,
                    SPLIT_PART(FIELDS_6_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE5,
                    SPLIT_PART(FIELDS_7_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE6,
                    SPLIT_PART(FIELDS_8_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE7,
                    SPLIT_PART(FIELDS_9_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE8,
                    SPLIT_PART(FIELDS_10_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE9,
                    SPLIT_PART(FIELDS_11_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE10,
                    SPLIT_PART(FIELDS_12_CONTENT,':',2) AS SECONDARY_DIAGNOSIS_CODE11
                    FROM CTE_CLM T1
                    LEFT JOIN ANTHEM_837_STAGING CTE_HI
                    ON RID BETWEEN T1.CLM_RID AND T1.CLM_NEXT_RID
                    AND FIELDS_0_CONTENT = 'HI' ),

        CTE_DTP AS ( SELECT CTE_HI.* ,CAST(FIELDS_3_CONTENT AS DATE) AS DOS
                    FROM CTE_HI
                    LEFT JOIN  ANTHEM_837_STAGING
                    ON RID BETWEEN CLM_RID AND CLM_NEXT_RID
                    AND  FIELDS_0_CONTENT = 'DTP'
                    AND FIELDS_1_CONTENT = '050' ),

        CTE_PAID_AMT AS ( SELECT CLAIM_NUMBER,SUM(PAID_AMOUNT) AS PAID_AMOUNT FROM (
                            SELECT CLAIM_NUMBER, CAST(fields_2_content AS FLOAT) AS PAID_AMOUNT
                                    FROM TEMP_CLM_ROWS
                            LEFT JOIN ANTHEM_837_STAGING
                            ON RID BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID
                            AND FIELDS_0_CONTENT = 'AMT'
                            AND FIELDS_1_CONTENT = 'D' ) A
                            GROUP BY CLAIM_NUMBER ),

        CTE_PATIENT_AMT AS ( SELECT CLAIM_NUMBER,SUM(PAID_RESPONSIBLE_AMOUNT) AS PATIENT_RESPONSIBLE_AMOUNT FROM (
                    SELECT CLAIM_NUMBER, CAST(fields_2_content AS FLOAT) AS PAID_RESPONSIBLE_AMOUNT
                    FROM TEMP_CLM_ROWS
                    LEFT JOIN anthem_837_staging
                    ON RID BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID
                    AND FIELDS_0_CONTENT = 'AMT'
                    AND FIELDS_1_CONTENT = 'F5') A
                    GROUP BY CLAIM_NUMBER )

        SELECT CTE_DTP.CLAIM_NUMBER,CLAIM_AMOUNT,DOS,PLACE_OF_SERVICE,PROFESSIONAL_CLAIM,CLAIM_FREQUENCY_TYPE,PROVIDER_SIGNATUR                                                                                                E_INDICATOR,
                PROVIDER_ACCEPT_ASSIGNMENT,PROVIDER_BENEFITS_ASSIGNMENT,RELEASE_INFO_I,CLIENT_SIGNATURE_SOURCE,RELATED_CAUSE,
                BILLING_PROV_TYPE,BILLER_NAME,BILLING_NPI,BILL_ADDRESS,BILL_CITY,BILL_STATE,BILL_ZIP,BILLING_PROVIDER_TAXONOMY,B                                                                                                ILLING_TIN,
                SERVICE_LOCATION_NAME,SERVICE_LOCATION_NPI,SERVICE_ADDRESS_1,SERVICE_ADDRESS_2,SERVICE_CITY,SERVICE_STATE,SERVIC                                                                                                E_ZIP,
                PRIMARY_DIAGNOSIS_CODE,SECONDARY_DIAGNOSIS_CODE1,SECONDARY_DIAGNOSIS_CODE2,SECONDARY_DIAGNOSIS_CODE3,SECONDARY_D                                                                                                IAGNOSIS_CODE4,
                SECONDARY_DIAGNOSIS_CODE5,SECONDARY_DIAGNOSIS_CODE6,SECONDARY_DIAGNOSIS_CODE7,SECONDARY_DIAGNOSIS_CODE8,SECONDAR                                                                                                Y_DIAGNOSIS_CODE9,
                SECONDARY_DIAGNOSIS_CODE10,SECONDARY_DIAGNOSIS_CODE11,PATIENT_RESPONSIBLE_AMOUNT FROM CTE_DTP
        INNER JOIN  CTE_PAID_AMT
        ON CTE_DTP.CLAIM_NUMBER = CTE_PAID_AMT.CLAIM_NUMBER
        INNER JOIN CTE_PATIENT_AMT
        ON CTE_PATIENT_AMT.CLAIM_NUMBER = CTE_DTP.CLAIM_NUMBER
        INNER JOIN CTE_2000A
        ON CTE_DTP.CLAIM_NUMBER = CTE_2000A.CLAIM_NUMBER
        INNER JOIN CTE_2310C
        ON CTE_DTP.CLAIM_NUMBER = CTE_2310C.CLAIM_NUMBER
        INNER JOIN CTE_2310AB
        ON CTE_DTP.CLAIM_NUMBER = CTE_2310AB.CLAIM_NUMBER;

        drop table if exists t12;
        create temporary table t12
        as
        with CTE_SBR_P AS ( SELECT RID AS SBR_P_RID ,
                    CASE WHEN FIELDS_1_CONTENT = 'P' THEN 'Primary'
                    WHEN FIELDS_1_CONTENT = 'S' THEN 'Secondary'
                    WHEN FIELDS_1_CONTENT = 'T' THEN 'Tertiary' END AS PAYER_RESPONSIBILITY,
                    CASE WHEN FIELDS_2_CONTENT = '18' THEN 'Self'
                    WHEN FIELDS_2_CONTENT = '01' THEN 'Spouse'
                    WHEN FIELDS_2_CONTENT = '19' THEN 'Child'
                    WHEN FIELDS_2_CONTENT = 'G8' THEN 'Other' END AS RELATIONSHIP_TO_INSURED,
                    ISNULL(LEAD( RID) OVER( ORDER BY RID),RID+50) AS SBR_P_NEXT_RID
                    FROM ANTHEM_837_STAGING
                    WHERE FIELDS_0_CONTENT = 'SBR'
                    AND FIELDS_1_CONTENT = 'P'
                ),

        CTE_T1_P AS ( SELECT RID AS PATIENT_RID,CASE WHEN FIELDS_2_CONTENT = '1' THEN 'Individual'
                                WHEN FIELDS_2_CONTENT = '2' THEN 'Entity (Workers Comp)' END AS INSURED_PARTY,
        FIELDS_4_CONTENT AS PRIMARY_FIRST_NAME, FIELDS_5_CONTENT AS PRIMARY_MIDDLE_INITIAL, FIELDS_3_CONTENT AS PRIMARY_LAST_NA                                                                                                ME,
        FIELDS_9_CONTENT AS PRIMARY_CIN, SBR.*
        FROM ANTHEM_837_STAGING
        INNER JOIN CTE_SBR_P SBR
        ON RID = SBR_P_RID +1
        WHERE FIELDS_0_CONTENT = 'NM1'
        AND FIELDS_1_CONTENT = 'IL'  ),

        CTE_T2_P AS ( SELECT  PRIMARY_FIRST_NAME,PRIMARY_MIDDLE_INITIAL,PRIMARY_LAST_NAME,PRIMARY_CIN,PAYER_RESPONSIBILITY,RELA                                                                                                TIONSHIP_TO_INSURED,
                        FIELDS_1_CONTENT AS PRIMARY_ADDRESS_LINE_1,FIELDS_2_CONTENT AS PRIMARY_ADDRESS_LINE_2,INSURED_PARTY,SBR_                                                                                                P_RID,SBR_P_NEXT_RID,RID AS ADD_RID
                FROM ANTHEM_837_STAGING T2
                INNER JOIN CTE_T1_P T1
                ON RID = PATIENT_RID + 1
                WHERE FIELDS_0_CONTENT = 'N3' ),


        CTE_T3_P AS ( SELECT CTE_T2_P.*, FIELDS_1_CONTENT AS  CITY, FIELDS_2_CONTENT AS STATE, FIELDS_3_CONTENT AS ZIP
                                FROM anthem_837_staging
                                INNER JOIN CTE_T2_P
                                ON RID = ADD_RID +1
                                WHERE FIELDS_0_CONTENT = 'N4' ),
        CTE_T4_P AS ( SELECT CTE_T3_P.*, CAST(FIELDS_2_CONTENT AS DATE) AS DOB, FIELDS_3_CONTENT AS GENDER
                        FROM anthem_837_staging
                        INNER JOIN CTE_T3_P
                        ON RID = ADD_RID + 2
                        WHERE FIELDS_0_CONTENT = 'DMG' )

                SELECT CLAIM_NUMBER::VARCHAR(50) , SUB.*
                FROM CTE_T4_P SUB
                INNER JOIN TEMP_CLM_ROWS CLM
                ON CLM_ROW_ID BETWEEN SBR_P_RID AND SBR_P_NEXT_RID ;

        delete innovator.claims_p_837_parsed_header where file_name = '{0}';
        insert into innovator.claims_p_837_parsed_header
        WITH CTE_1 AS ( select *, RANK() OVER(PARTITION BY CLAIM_NUMBER ORDER BY PRIMARY_DIAGNOSIS_CODE DESC) AS RNK FROM T11 )
        SELECT T11.*,T12.PRIMARY_FIRST_NAME,PRIMARY_MIDDLE_INITIAL,PRIMARY_LAST_NAME,PRIMARY_CIN,PAYER_RESPONSIBILITY,RELATIONS                                                                                                HIP_TO_INSURED,
                PRIMARY_ADDRESS_LINE_1,PRIMARY_ADDRESS_LINE_2,INSURED_PARTY,CITY,STATE,ZIP,DOB,GENDER,'{0}' as file_name,
                getdate() as added_tz,NULL as modified_tz,'{1}' as source
        FROM T11
        INNER JOIN CTE_1
        ON T11.CLAIM_NUMBER = CTE_1.CLAIM_NUMBER
        AND T11.PRIMARY_DIAGNOSIS_CODE = CTE_1.PRIMARY_DIAGNOSIS_CODE
        AND CTE_1.RNK =1
        INNER JOIN T12
        ON T11.CLAIM_NUMBER = T12.CLAIM_NUMBER ;
    """
    conn.execute(q3.format(output_f,source))
    conn.commit()
    print('table ready')

    q4="""
            set statement_timeout to 0;
            delete innovator.claims_p_837_parsed_proc where file_name = '{0}';
            insert into innovator.claims_p_837_parsed_proc
            select CLAIM_NUMBER,SPLIT_PART(t2.FIELDS_1_CONTENT,':',2) AS CPT_CODE,
            SPLIT_PART(t2.FIELDS_1_CONTENT,':',3) || ':' || SPLIT_PART(t2.FIELDS_1_CONTENT,':',4) ||
            SPLIT_PART(t2.FIELDS_1_CONTENT,':',5) || ':' || SPLIT_PART(t2.FIELDS_1_CONTENT,':',6) AS MODIFIER,
            SPLIT_PART(t2.FIELDS_1_CONTENT,':',7) AS DESCRIPTION,
            cast(substring(t3.fields_3_content,1,8) as date) as procedure_date,
            CAST(t2.FIELDS_2_CONTENT AS FLOAT) AS AMOUNT_diag_level,
            t2.FIELDS_3_CONTENT AS BASIS_OF_MEASUREMENT,
            CAST(t2.FIELDS_4_CONTENT AS INT) AS QUANTITY,
            t2.FIELDS_5_CONTENT AS FACILITY_CODE,
            CAST(t1.FIELDS_1_CONTENT AS INT) AS LX_NUMBER,
            '{0}' AS  FILE_NAME,
            GETDATE(),Null AS MODIFIED_TZ
            FROM anthem_837_staging T1
            INNER JOIN anthem_837_staging T2
            ON t1.rid + 1  = t2.rid
            AND t2.fields_0_content = 'SV1'
            LEFT JOIN anthem_837_staging t3
            ON t3.rid = t2.rid+1
            AND t3.fields_0_content = 'DTP'
            AND t3.fields_1_content = '472'
            INNER JOIN TEMP_CLM_ROWS t4
            ON t1.rid BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID
            WHERE t1.fields_0_content = 'LX';

            delete innovator.claims_p_837_parsed_line_adjudication where file_name = '{0}';
            insert into innovator.claims_p_837_parsed_line_adjudication
            -- Line Adjudication

            with CTE_PROC_CLM AS (SELECT FIELDS_1_CONTENT AS CLAIM_NUMBER, RID AS CLM_ROW_ID, ISNULL(LEAD( RID) OVER( ORDER BY                                                                                                 RID),RID+350) AS CLM_NEXT_ROW_ID
                                    FROM anthem_837_staging
                                    WHERE FIELDS_0_CONTENT = 'CLM'),
            cte_lx as ( select FIELDS_1_CONTENT AS LINE_NUMBER, RID AS LX_RID, ISNULL(LEAD( RID) OVER( ORDER BY RID),RID+350) AS                                                                                                 LX_NEXT_RID
                        FROM anthem_837_staging  WHERE FIELDS_0_CONTENT = 'LX' ),

            cte_svd as ( select FIELDS_3_CONTENT,SPLIT_PART(FIELDS_3_CONTENT,':',2) AS CPT_CODE  ,
                        CASE WHEN POSITION(':' IN SUBSTRING(FIELDS_3_CONTENT,4) ) > 0
                                THEN SUBSTRING(FIELDS_3_CONTENT,POSITION(':' IN SUBSTRING(FIELDS_3_CONTENT,4) )+4) END AS MODIFIE                                                                                                R,
                        CAST(FIELDS_2_CONTENT AS FLOAT) AS AMOUNT,CAST(FIELDS_5_CONTENT AS INT) AS QTY, RID AS SVD_RID,
                        line_number
                        from anthem_837_staging
                        inner join cte_lx
                        on rid between lx_rid and lx_next_rid
                        where fields_0_content = 'SVD' )

            select claim_number,cpt_code,modifier,AMOUNT,QTY,'{0}',getdate(),null,cast(line_number as int)
                    from cte_svd
                    inner join cte_proc_clm
                    on svd_rid between clm_row_id and clm_next_row_id;
            delete innovator.claims_p_837_parsed_line_adjustments where file_name = '{0}';
            insert into innovator.claims_p_837_parsed_line_adjustments

            -- line adjustment
            with CTE_PROC_CLM AS (SELECT FIELDS_1_CONTENT AS CLAIM_NUMBER, RID AS CLM_ROW_ID, ISNULL(LEAD( RID) OVER( ORDER BY                                                                                                 RID),RID+350) AS CLM_NEXT_ROW_ID
                                    FROM anthem_837_staging
                                    WHERE FIELDS_0_CONTENT = 'CLM'),
            cte_lx as ( select FIELDS_1_CONTENT AS LINE_NUMBER, RID AS LX_RID, ISNULL(LEAD( RID) OVER( ORDER BY RID),RID+350) A                                                                                                S LX_NEXT_RID
                    FROM anthem_837_staging  WHERE FIELDS_0_CONTENT = 'LX' ),

            cte_line_adj as ( select FIELDS_1_CONTENT AS CLAIM_ADJ_GROUP_CODE,FIELDS_2_CONTENT AS CLAIM_ADJ_REASON_CODE,CAST(FI                                                                                                ELDS_3_CONTENT AS FLOAT) AS AMOUNT,
                                    RID AS ADJ_RID,line_number
                                    from anthem_837_staging
                                    inner join cte_lx
                                    on rid between lx_rid and lx_next_rid
                                    where fields_0_content = 'CAS' )

            select CLAIM_NUMBER,CLAIM_ADJ_GROUP_CODE,CLAIM_ADJ_REASON_CODE,AMOUNT,'{0}' as file_name,getdate(),null,cast(line_n                                                                                                umber as int)
                    FROM CTE_PROC_CLM
                    INNER JOIN CTE_LINE_ADJ
                    ON ADJ_RID BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID;
            delete innovator.claims_p_837_parsed_drugs where file_name = '{0}';
            insert into innovator.claims_p_837_parsed_drugs
            WITH cte_drugs as (select fields_3_content as DRUG_CODE,rid as drug_rid
                        from anthem_837_staging
                        where fields_0_content = 'LIN' and fields_2_content = 'N4'),

            cte_quantity as (select cast(fields_4_content as float) as drug_Quantity,fields_5_content as drug_Qty_Unit,rid-1 as                                                                                                 q_rid
                            from anthem_837_staging
                            where fields_0_content = 'CTP')

            SELECT claim_number,drug_code,drug_quantity,drug_qty_unit,'{0}',getdate(),null
                    FROM CTE_drugs
                    INNER JOIN TEMP_CLM_ROWS
                    ON drug_RID BETWEEN CLM_ROW_ID AND CLM_NEXT_ROW_ID
                    inner join cte_quantity
                    on drug_rid = q_rid;
    """
    conn.execute(q4.format(output_f))
    print('header table ingested')
    conn.commit()


def main():
    #s3.get("acp-data/Evolent_copy/837/" + filename,"/home/etl/etl_home/temp/"+ filename)
    #output_file=".".join(filename.split(".")[:1])
    #output_file=filename.split(".")[:1][0]
    output_file=filename
    process_file(output_file)
    file_in_db="""select distinct file_name from innovator.claims_p_837_parsed_header"""
    df=pd.read_sql(file_in_db,conn)
    files=df['file_name'].tolist()
    if filename not in files:
        process_db(output_file,source_type,var)


if __name__ == '__main__':
    main()
