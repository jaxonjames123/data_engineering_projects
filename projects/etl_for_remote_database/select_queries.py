QUERY_REGISTRY = {
    "HC": {
        "summary_measure_result_snapshot": """
            USE ABLEHEALTH;
            SELECT DISTINCT BindingID, BindingNM, LastLoadDTS, CalendarGrainCD, 
                CalendarDTKeyTXT, CalendarDT, CalendarYearNBR, CalendarMonthNBR, 
                CalendarQuarterNBR, CustomerID, ImportID, GroupID, GroupIDKeyTXT, 
                GroupNM, ProgramTXT, ProgramKeyTXT, MeasureSlugID, MeasureID, RateID, 
                InverseFLG, MeasurementPeriodStartDTS, MeasurementPeriodStartDT, 
                MeasurementPeriodEndDTS, MeasurementPeriodEndDT, CalculationDTS, 
                CalculationDT, CONVERT(datetime, CalculationDTO) as CalculationDTO, 
                MeasureCertificationStatusDSC, EDWPatientID, ExternalID, FamilyNameTXT, 
                GivenNameTXT, EpisodeID, EpisodeStartDTS, EpisodeStartKeyTXT, SourceTXT, 
                ReportingStatusTXT, InitialPopulationBIT, DenominatorBIT, DenominatorExclusionBIT, 
                NumeratorExclusionBIT, NumeratorPerformanceMetBIT, DenominatorExceptionBIT,
                NumeratorPerformanceNotMetBIT, InitialPopulationNBR, DenominatorNBR, 
                DenominatorExclusionNBR, NumeratorPerformanceMetNBR, DenominatorExceptionNBR, 
                NumeratorPerformanceNotMetNBR, ValueVAL, StatusDSC, BirthDTS, SexCD, EthnicityCD, 
                RaceCD, PhoneNumberTXT, ProviderID, ProviderIDKeyTXT, EDWProviderID, PCPID, 
                PCPFullNameTXT, ProviderFullNameTXT, LanguageCD, SpecialAccommodationsTXT 
            FROM Summary.MeasureResultSnapshot
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "reference_measure": """
            USE ABLEHEALTH;
            SELECT DISTINCT * FROM Reference.Measure
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "reference_value_set": """
            USE ABLEHEALTH;
            SELECT DISTINCT * FROM Reference.ValueSet 
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "result_measure_result": """
            USE ABLEHEALTH;
            select DISTINCT BindingID, BindingNM, LastLoadDTS, CustomerID, ImportID, GroupID, 
                GroupIDKeyTXT, GroupNM, ProgramTXT, ProgramKeyTXT, MeasureSlugID, 
                MeasureID, RateID, InverseFLG, MeasurementPeriodStartDTS, MeasurementPeriodStartDT, 
                MeasurementPeriodEndDTS, MeasurementPeriodEndDT, CalculationDTS, CalculationDT, 
                CONVERT(datetime, CalculationDTO) as CalculationDTO, MeasureCertificationStatusDSC, 
                ExternalID, FamilyNameTXT, GivenNameTXT, EpisodeID, EpisodeStartDTS, EpisodeStartKeyTXT, 
                SourceTXT, ReportingStatusTXT, InitialPopulationBIT, DenominatorBIT, 
                DenominatorExclusionBIT, NumeratorPerformanceMetBIT, DenominatorExceptionBIT, 
                NumeratorExclusionBIT, NumeratorPerformanceNotMetBIT, ValueVAL, StatusDSC, BirthDTS, 
                SexCD, EthnicityCD, RaceCD, PhoneNumberTXT, ProviderID, ProviderIDKeyTXT, PCPID, 
                PCPFullNameTXT, ProviderFullNameTXT, LanguageCD, SpecialAccommodationsTXT, 
                AttributedGroupID, AttributedGroupNM
            FROM Result.MeasureResult WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "result_measure_result_most_recent": """
            USE ABLEHEALTH;
            SELECT DISTINCT BindingID, BindingNM, LastLoadDTS, CustomerID, ImportID, GroupID, 
                GroupIDKeyTXT, GroupNM, ProgramTXT, ProgramKeyTXT, MeasureSlugID, MeasureID, RateID, 
                MeasurementPeriodStartDTS, MeasurementPeriodEndDTS, CalculationDTS, MeasureCertificationStatusDSC, 
                ExternalID, FamilyNameTXT, GivenNameTXT, EpisodeID, EpisodeStartDTS, EpisodeStartKeyTXT, 
                SourceTXT, ReportingStatusTXT, InitialPopulationBIT, DenominatorBIT, DenominatorExclusionBIT, 
                NumeratorExclusionBIT, NumeratorPerformanceMetBIT, DenominatorExceptionBIT, 
                NumeratorPerformanceNotMetBIT, ValueVAL, StatusDSC, BirthDTS, SexCD, EthnicityCD, RaceCD, 
                PhoneNumberTXT, ProviderID, ProviderIDKeyTXT, PCPFullNameTXT, InverseFLG, ProviderFullNameTXT, 
                LanguageCD, SpecialAccommodationsTXT
            FROM Result.MeasureResultMostRecent
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "result_measure_measure_result_external_id_alias": """
            USE ABLEHEALTH;
            SELECT DISTINCT * FROM Result.MeasureResultMostRecent
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "resultload_external_id_alias_text": """
            USE ABLEHEALTH;
            SELECT DISTINCT *
            FROM ResultLoad.ExternalIDAliasText
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "reference_foundation_data_source": """
            USE ABLEHEALTH;
            SELECT DISTINCT *
            FROM Reference.FoundationDataSource
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "reference_most_recent_payor_id": """
            USE ABLEHEALTH;
            SELECT DISTINCT *
            FROM Reference.MostRecentPayorID
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "ed_detail": """
            USE SAM;
            SELECT DISTINCT *
            FROM Claim.EDDetail
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "ed_header": """
            USE SAM;
            SELECT DISTINCT *
            FROM Claim.EDHeader
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "inpatient_header": """
            USE SAM;
            SELECT DISTINCT *
            FROM Claim.InpatientHeader
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "member_event_history": """
            USE SAM;
            SELECT DISTINCT *
            FROM Claim.MemberEventHistory
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "provider": """
            USE SAM;
            SELECT DISTINCT  BindingID, BindingNM, LastLoadDTS, NPI, NPINM, EntityTypeCD, EntityTypeDSC, 
                replace(OrganizationNM, '''', '') as OrganizationNM, FirstNM, MiddleNM, LastNM, 
                GroupNM, PracticeAddress01TXT, PracticeAddressCityNM, PracticeAddressStateCD, 
                PracticeAddressZipCD, CurrentProviderServiceAreaNM, PrimaryTaxonomyCD, PrimarySpecialtyNM, 
                ProviderTypeCD, ProviderTypeRankNBR, ServiceClassificationNM, 
                concat('"', ServiceTypeNM, '"') as ServiceTypeNM
            FROM Claim.provider
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "provider_report_group": """
            USE SAM;
            SELECT DISTINCT *
            FROM Claim.ProviderReportGroup
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "value_optimizer_member_month_grain": """
            USE SAM;
            SELECT DISTINCT *
            FROM Claim.ValueOptimizerMemberMonthGrain
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "value_set_code": """
            USE SHARED;
            SELECT DISTINCT *
            FROM Shared.Terminology.ValueSetCode
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "value_set_description": """
            USE SHARED;
            SELECT DISTINCT *
            FROM Shared.Terminology.ValueSetDescription
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "Claim_diagnosis": """
            USE SOLUTION;
            SELECT DISTINCT *
            FROM Solution.Claim.ClaimDiagnosis
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "Claim_line_item": """
            USE SOLUTION;
            SELECT DISTINCT *
            FROM Solution.Claim.ClaimLineItem
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "encounter": """
            USE SOLUTION;
            SELECT DISTINCT *
            FROM Solution.Claim.encounter
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "member": """
            USE SOLUTION;
            SELECT DISTINCT *
            FROM Solution.Claim.Member
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "enrolled_export_list": """
            USE POPULATIONHEALTH;
            SELECT DISTINCT *
            FROM CareManagement.EnrolledExportList
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "dimpatient": """
            USE POPULATIONHEALTH;
            SELECT DISTINCT BindingID, BindingNM, LastLoadDTS, PatientID, EDWPatientID, MRN, 
                MedicaidID, PatientFullNM, LastName, FirstName, MiddleName, BirthDTS, 
                AgeNBR, AgeGroupCD, AgeGroupDSC, AgeGroupDSC2, GenderCD, GenderDSC, RaceCD, 
                RaceDSC, EthnicityCD, EthnicityDSC, LanguageCD, LanguageDSC, StateCD, 
                StateNM, PostalCD, CommunicationPreferenceNM, ClaimsDataAvailableFLG, 
                CurrentPayerCD, CurrentPayerHierarchyCD, CurrentMemberEnrollmentDT, CareFlowPayer, 
                PolicyInformation, IsPrimaryInsurance, CustomPayer, PrimaryCarePhysicianName, 
                PCPEDWProviderID, CurrentPCPID, ProviderFullNM, PCPSpecialtyCD, PCPSpecialtyNM, 
                PCPNPI, PCPName, convert(datetime, AsOfDT) as AsOfDt, CustomAttribute01NM, 
                CustomAttribute01PrimaryAttribute, CustomAttribute01AttributeList, CustomAttribute02NM, 
                CustomAttribute02PrimaryAttribute, CustomAttribute02AttributeList, CustomAttribute03NM, 
                CustomAttribute03PrimaryAttribute, CustomAttribute03AttributeList, CustomAttribute04NM, 
                CustomAttribute04PrimaryAttribute, CustomAttribute04AttributeList, AgeGroup3, 
                AgeSortOrder3, Payer, RowNBR
            FROM CareManagement.dimPatient
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "care_management_workflow_patient_identifier": """
            USE CareManagementWorkflow;
            SELECT DISTINCT PatientIdentifierID, PatientIdentifierUUID, PatientID, 
                PatientIdentifierTypeID, DataSourceTypeID, Value, RowVersion, 
                CreatedByUserID, convert(datetime, CreatedDateTime) as CreatedDateTime, 
                UpdatedByUserID, convert(datetime, UpdatedDateTime) as UpdatedDateTime, 
                RowEffectiveDateTime, RowInactiveDateTime, EDWLastModifiedDTS
            FROM Patient.PatientIdentifier;
        """,
        "data_source_type": """
            USE CareManagementWorkflow;
            SELECT DISTINCT DataSourceTypeID, DataSourceTypeUUID, Code,
                Name, Description, IsSystem, InactiveDate, RowVersion, 
                CreatedByUserID, convert(datetime, CreatedDateTime) as CreatedDateTime, 
                UpdatedByUserID, convert(datetime, UpdatedDateTime) as UpdatedDateTime, 
                EDWLastModifiedDTS
            FROM Reference.DataSourceType;
        """,
        "patient_identifier_type": """
            USE CareManagementWorkflow;
            SELECT DISTINCT PatientIdentifierTypeID, PatientIdentifierTypeUUID, Code, Name, 
                Description, IsSystem, InactiveDate, RowVersion, CreatedByUserID, 
                convert(datetime, CreatedDateTime) as CreatedDateTime, UpdatedByUserID, 
                convert(datetime, UpdatedDateTime) as UpdatedDateTime, EDWLastModifiedDTS
            FROM Reference.PatientIdentifierType;
        """,
        "patient_crosswalk": """
            USE Foundation;
            SELECT DISTINCT *
            FROM IntegratedCore.PatientCrosswalk
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "patient_identifier": """
            USE FOUNDATION;
            SELECT DISTINCT *
            FROM DataCore.PatientIdentifier
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "reporting_viewable_definitions": """
            USE SAM;
            SELECT DISTINCT *
            FROM Measures.ReportingViewAbleDefinitions
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "summary_able": """
            USE SAM;
            SELECT DISTINCT *
            FROM Measures.SummaryAble
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "factcarenote": """
            USE POPULATIONHEALTH;
            SELECT DISTINCT BindingID, BindingNM, LastLoadDTS, PatientCareNoteID, PatientID, EpisodeID, 
                CareProgramKey, CareTeamMemberKey, CareNoteTypeCD, CareNoteTypeDSC, CareNoteActionTypeCD, 
                CareNoteActionTypeDSC, CareNoteActionTypeGroupNM, AdministrativeFLG, CommunicationVisitFLG, 
                CareNoteContactResultDSC, CareNoteContactSuccessFLG, CareNoteTitleTXT, 
                REPLACE(REPLACE(CareNoteTXT, '"', ''), '''', ''), NoteDTS, NoteDT, MonthStartDTS, DurationInMinutesNBR, 
                MissingDurationFLG, CareNoteStatusCD, CareNoteStatusDSC, CareNoteStatusReasonCD, CareNoteStatusReasonDSC, 
                CreateDTS, CreatedByUserID, CreatedByUserNM, CreatedByCareTeamKey, StatusDTS, StatusedByUserID, 
                StatusedByUserNM, StatusedByCareTeamKey, UpdateDTS, UpdatedByUserID, UpdatedByUserNM,
                UpdatedByCareTeamKey, DischargedStatus, EpisodeEnrolledFLG, CurrentPhaseCD, EpisodeStatusCD, 
                FullClosureReasonCD, EnrolledInMultipleCareProgramsFLG, EpisodePhaseCD, 
                EnrolledWithSuccessfulDirectCommunicationFLG, EnrolledWithUnsuccessfulDirectCommunicationFLG, 
                CurrentlyEnrolledWithSuccessfulDirectCommunicationFLG, CurrentlyEnrolledWithUnsuccessfulDirectCommunicationFLG, 
                RowNBR
            FROM CareManagement.factCareNote
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "dimcareprogram": """
            USE POPULATIONHEALTH;
            SELECT DISTINCT bindingid, bindingnm, lastloaddts, careprogramkey, careprogramdsc, careprogramgroup, activeflg, 
                careprogramcaveat, canskipqualify, canskipoutreach, targetdurationdays, outreachtargetnbr, 
                convert(datetime, inactivedatetime) as inactivedatetime, createdbyuserid, 
                convert(datetime, createddatetime) as createddatetime, updatedbyuserid, 
                convert(datetime, updateddatetime) as updateddatetime, roweffectivedatetime, firstepisodeopendt
            FROM CareManagement.dimCareProgram
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "factepisode": """
            USE POPULATIONHEALTH;
            SELECT DISTINCT *
            FROM CareManagement.FactEpisode
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """,
        "clientidentifiers": """
            USE POPULATIONHEALTH;
            SELECT DISTINCT bindingid, bindingnm, lastloaddts, episodeid, edwpatientid, replace(replace(clientidentifiers, '"', ''),'''', '')
            FROM CareManagement.ClientIdentifiers
            WHERE substring(convert(varchar, LastLoadDTS, 120), 1, 10) = ?;
        """
    },
    "SOMOS": {
        "assessments": """SELECT DISTINCT * FROM somos.assessments;""",
        "flexible_referral_fields": """SELECT DISTINCT * FROM somos.flexible_referral_fields;""",
        "form_responses": """SELECT DISTINCT * FROM somos.form_responses;""",
        "goals": """SELECT DISTINCT * FROM somos.goals;""",
        "note_options": """SELECT DISTINCT * FROM somos.note_options;""",
        "notes": """
            SELECT DISTINCT id_notes, note_id, seeker_id, seeker_profile_id, author_user_id, replace(note_text, '"', '') as note_text, note_type, 
                note_type_id, subdomain, note_created_at, note_updated_at, is_deleted, entry_date, update_date 
            FROM somos.notes;
            """,
        "programs": """SELECT DISTINCT * FROM somos.programs;""",
        "referral_status": """SELECT DISTINCT * FROM somos.referral_status;""",
        "referrals": """SELECT DISTINCT * FROM somos.referrals;""",
        "sdoh_code_mappings": """SELECT DISTINCT * FROM somos.sdoh_code_mappings;""",
        "seeker_context": """SELECT DISTINCT * FROM somos.seeker_context;""",
        "seeker_profile_audit_log": """SELECT DISTINCT * FROM somos.seeker_profile_audit_log;""",
        "seeker_profile_custom_values": """SELECT DISTINCT * FROM somos.seeker_profile_custom_values;""",
        "seeker_profiles": """SELECT DISTINCT * FROM somos.seeker_profiles;""",
        "seeker_sms_messages": """SELECT DISTINCT * FROM somos.seeker_sms_messages;""",
        "user_activity": """
            SELECT DISTINCT id_user_activity, user_id, Source, activity_type, replace(activity_data, '"', '') as activity_data, category, 
                search_result_count, related_object, related_object_type, activity_entry_date, subdomain, followed_url, postal, cookie, 
                user_agent, device_type, screen_size, engagement, program_numeric_id, link_url, ip_address, ip_organization, 
                session_id, session_click_sequence, session_begin_time, session_end_time, session_time_in_minutes, entry_date, 
                update_date
            FROM somos.user_activity;
            """,
        "user_external_identifiers": """SELECT DISTINCT * FROM somos.user_external_identifiers;""",
        "user_favorites": """SELECT DISTINCT * FROM somos.user_favorites;""",
        "worker_group_assignment_history": """SELECT DISTINCT * FROM somos.worker_group_assignment_history;""",
        "worker_groups": """SELECT DISTINCT * FROM somos.worker_groups;""",
        "worker_profiles": """SELECT DISTINCT * FROM somos.worker_profiles;""",
        "vw_etl_metadata": """SELECT DISTINCT * FROM somos.vw_etl_metadata;""",
    },
    "SOMOS_MEMBER_INSIGHTS": {
        "Claim_status": """SELECT DISTINCT * FROM somos_member_insights.Claim_status;""",
        "Claims": """SELECT DISTINCT * FROM somos_member_insights.Claims;""",
        "encounters": """SELECT DISTINCT * FROM somos_member_insights.encounters;""",
        "encounters_metadata": """SELECT DISTINCT * FROM somos_member_insights.encounters_metadata;""",
        "flexible_referral_fields": """SELECT DISTINCT * FROM somos_member_insights.flexible_referral_fields;""",
        "forms": """SELECT DISTINCT * FROM somos_member_insights.forms;""",
        "goals": """SELECT DISTINCT * FROM somos_member_insights.goals;""",
        "notes": """
            SELECT DISTINCT id_notes, note_id, seeker_id, seeker_profile_id, author_user_id, replace(note_text, '"', '') as note_text, note_type, 
                note_type_id, subdomain, note_created_at, note_updated_at, is_deleted, entry_date, update_date 
            FROM somos_member_insights.notes;
        """,
        "programs": """SELECT DISTINCT * FROM somos_member_insights.programs;""",
        "referrals": """SELECT DISTINCT * FROM somos_member_insights.referrals;""",
        "sdoh_code_sets": """SELECT DISTINCT * FROM somos_member_insights.sdoh_code_sets;""",
        "seeker_profile_custom_values": """SELECT DISTINCT * FROM somos_member_insights.seeker_profile_custom_values;""",
        "seeker_profiles": """SELECT DISTINCT * FROM somos_member_insights.seeker_profiles;""",
        "user_activity": """
            SELECT DISTINCT id_user_activity, user_id, Source, activity_type, replace(activity_data, '"', '') as activity_data, category, 
                search_result_count, related_object, related_object_type, activity_entry_date, subdomain, followed_url, postal, 
                cookie, user_agent, device_type, screen_size, engagement, program_numeric_id, link_url, ip_address, ip_organization, 
                id_all_activity, session_id, session_click_sequence, session_begin_time, session_end_time, session_time_in_minutes, 
                entry_date, update_date
            FROM somos_member_insights.user_activity;
        """,
        "user_external_identifiers": """SELECT DISTINCT * FROM somos_member_insights.user_external_identifiers;""",
        "worker_profiles": """SELECT DISTINCT * FROM somos_member_insights.worker_profiles;""",
        "vw_etl_metadata": """SELECT DISTINCT * FROM somos_member_insights.vw_etl_metadata;""",
    }
}
