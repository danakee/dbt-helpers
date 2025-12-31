USE [OperationsAnalyticsStage]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[spLoadOPAfactReservation]
@eid UNIQUEIDENTIFIER = NULL
----------------------------------------------------------------------------------------------------------------
--  
--  Author: FlightSafety 
--  
--  Modified: 11/18/2019  
--  
--  Description: 
--  
--      Technical Story 202303:Textron - Report Ops data against Textron Centers - EDW Dev - Enhancement
--      Modified to update the Textron Centers
--  
--      DAK - Removed Training Authority logic to new tables [dbo].[Drv_TrainingAuthority] and 
--      [dbo].[lkpTrainingAuthority]
--
--  Usage:  
--
--      DECLARE @eid uniqueidentifier = NEWID();  EXEC [OperationsAnalyticsStage].[dbo].[spLoadOPAfactReservation] @eid;
--
--  Check status in logging table:
--
--      SELECT 
--          * 
--      FROM 
--          [OperationsAnalyticsStage].[dbo].[DataWarehouse_Change_Audit_Log] AS [l]
--      WHERE 
--          [l].[TableName] = '[OperationsAnalytics].[dbo].[factReservation]'
--      ORDER BY 
--          [l].[Id] DESC;
--
--  Modified: 11/21/2019  
--
--  Description: Added AHADDT column as part of CEO Scorecard requirements
--
--  Modified: 03/08/2020 
--            BUG 212801 - Over-reporting Reservations (factReservation includes child reservations)
--
--  Modified: 05/01/2020
--            Technical Story 221793 - Exclude Child Reservation from Count (modify ReservationTotal column)
--
--  Modified: 05/28/2020
--            Technical Story 219469 - Client Retention Report Enhancements (data)
--            Added ReturnForTraining columns 
--            Note:  These two columns will ultimate move out to a Checkride item dimension
--
--  Modified: 10/12/2020
--  Modified By: Anila Nuthalapati
--            Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data)
--
--  Modified: 01/16/2021
--  Technical Story 246522: Override Delivery Center 1590 to 3090
--
--  Modified: 04/01/2021
--  Technical Story 252391: factReservation refactoring - Data Dev
--
--  Modified: 06/01/2021
--  Bug 258129 - Table [factReservation] has several date columns that are integer columns and should be date or 
--  datetime:
--
--      [ReservationBookingDate]
--      [CourseCompletedDate]
--      [InsuranceStartDate]
--      [InsuranceExpireDate]
--      [ERecordApprovedTimestamp]
--      [ERecordSignatureTimestamp]
--
--  Modified: 11/22/2021
--  Technical Story 267568: ROT Timeliness - Correction to Late Rule
--      
--
--  Modfied : 12/06/2021
--  Techinal Story 266018 - iPad Report EDW development
--      
--
--  Modfied : 02/15/2022
--  BUG 266539 - factReservation: Incorrect CourseCompletedDate
--
--
--  Modified:   03/08/2022
--  Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number

--  Modified:   03/23/2022 
--  Technical Story 272755 - Add column [DeliveryMethod]
--
--  Modified:   04/08/2022
--  Technical Requirement 267451 - Add column [FromProgramId] 
--
--  Modified:   05/12/2022
--  Technical Story 269286 - SAP-BW Migration: Refactor stored procedure [spLoadOPAfactReservation]
--  to remove dependencies
--
--  Modifed: 07/20/2022
--  BUG 277378 - Missing Reservations - Hourly with instructor [PRD]
--
--  Modified:  11/4/2022
--  Technical Story 282292 - Update OnTimeFlag logic in EDW

--  Modified:  11/17/2022
--  Technical Story 288610 - factReservation: Refer to Dummy Hourly Course in dimCourse

--  Modified:  07/07/2024
--  BUG 362186 - Missing Company Codes in OPS -Reservation

--  Modified:10/16/2024
--  BUG 382458 - "CourseCompletedDate" discrepancy between 1X Factreservation and EDW Factreservation table.

--  Modified:11/18/2024
--  BUG 385192 - ROT bug

--  Modified: 03/02/2025
--  Implemented the logic to replace hardcoded ROTTargetDays to use Stage_BMD_CustomerAttribute

--  Modified: 06/23/2025
--  User Story 417148 Additional Fields - Training Class ID - FSM 61.58 Effort

--  Modified: 06/23/2025
--  BUG 424875 1X - Customer Profile Dashboard

--  Modified: 11/17/2025
--  User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429
----------------------------------------------------------------------------------------------------------------
AS
BEGIN -- Begin stored procedure

    SET NOCOUNT ON;
    SET XACT_ABORT ON;  
    
    -- Get GUID if one is not passed in
    IF @eid IS NULL
        SET @eid = NEWID();

    -----------------------------------------------------------------------------------------------
    -- BUG 385192 - ROT Bug - Begin
    -----------------------------------------------------------------------------------------------
    DECLARE @AsOfDate DATE = 
	CASE 
        WHEN DATEPART(HOUR, GETDATE()) BETWEEN 12 AND 23
            THEN CAST(GETDATE() AS DATE)
        ELSE DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    END;
    -----------------------------------------------------------------------------------------------
    -- BUG 385192 - ROT Bug - End
    -----------------------------------------------------------------------------------------------

    -----------------------------------------------------------------------------------------------
    -- Get all Contracts and Orders per RESV with Dups Removed
    -- Eliminate canceled orders and orders with no RESV
    -- (2266356 rows affected) in 11s
    -----------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#OrderSummary];
    WITH [ResvContractOrder] AS (
    SELECT 
         [ODRESV]
        ,[ODORDR]
        ,[ODRCNT]
        ,[ODMMID]
        ,MAX([ODTYPE]) OVER (PARTITION BY [ODRESV]) AS [ODTYPE]
        ,MAX([ODPOTY]) OVER (PARTITION BY [ODRESV]) AS [ODPOTY]
        -- Count the number of multiple rows per RESV
        ,COUNT(1) OVER (
            PARTITION BY 
                [ODRESV]) AS [MultiCount]
        -- Add sequence number column to consistenly handle multiple occurrences
        ,ROW_NUMBER() OVER (
            PARTITION BY 
                [ODRESV]
            ORDER BY
                [RRN] DESC) AS [ContOrdSeqNum] --> Use latest RRN as the determining row
    FROM 
        [OperationsAnalyticsStage].[dbo].[Stage_FPPXOD] AS [x]
    WHERE 
        1=1
        AND [x].[ODCFLG] <> 'C' -- eliminate canceled orders
        AND [x].[ODRESV] <> ''  -- eliminate empty ODRESV
    )
    SELECT 
         [ODRESV]
        ,[ODORDR]
        ,[ODRCNT]
        ,[ODMMID]
        ,[ODTYPE]
        ,[ODPOTY]
        ,[MultiCount]
        ,[ContOrdSeqNum] --> Use latest RRN as the determining row
    INTO 
        [#OrderSummary]
    FROM 
        [ResvContractOrder] AS [co]
    WHERE 
        [ContOrdSeqNum] = 1;

    ---------------------------------------------------------------------------------------------------
    -- Extracts *TBA Instructor records
    -- (44217 rows affected) in 4s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#TBAInstructor];
    SELECT 
        [ah].[AHID]
    INTO 
        [#TBAInstructor] 
    FROM 
        [OperationsAnalyticsStage].[dbo].[Stage_FPCRAH] AS [ah]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAD] AS [ad]
            ON [ah].[AHID] = [ad].[ADAHID]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAU] AS [au]
            ON [au].[AUID] = [ad].[ADAUID]
    WHERE 
        [ah].[AHSDT] >= 1120101
    	AND [au].[AUINST] = '*TBA'
    GROUP BY 
        [ah].[AHID];
    
    ---------------------------------------------------------------------------------------------------
    -- Extract Ipad Fields
    -- (107491 rows affected) in 0s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#IpadAttributes];
    SELECT
         [CLNT_EXTID] AS [ClntId]
        ,[CLNT_ORDER] AS [ClntOrder]
        ,[CLNT_NOTES] AS [ClntNotes]
        ,[CLNT_DATE_ORDER] AS [ClntDateOrder]
    INTO 
        [#IpadAttributes]
    FROM 
        [OperationsAnalyticsStage].[dbo].[Stage_CLIENT_EXTENSION]
    WHERE 
        [CLNT_EXTID] IS NOT NULL;
    
    ---------------------------------------------------------------------------------------------------
    -- Extracts *NO or numeric Instructor records
    -- (908888 rows affected) in 15s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#NonTBAInstructor];
    SELECT 
        [ah].[AHID]
    INTO 
        [#NonTBAInstructor]
    FROM 
        [OperationsAnalyticsStage].[dbo].[Stage_FPCRAH] AS [ah]
    	LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAD] AS [ad]
            ON [ah].[AHID] = [ad].[ADAHID]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAU] AS [au]
            ON [au].[AUID] = [ad].[ADAUID]  
    WHERE 
        [ah].[AHSDT] >= 1120101
    	AND (ISNUMERIC([au].[AUINST]) = 1 OR [au].[AUINST] = '*NO') 
    GROUP BY 
        [ah].[AHID];
    ---------------------------------------------------------------------------------------------------
    -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - Begin
    ---------------------------------------------------------------------------------------------------

    ---------------------------------------------------------------------------------------------------
    -- Extracts Reservation Header IDs with Instructor
    -- (14171593 rows affected) in 55s
    ---------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #HeaderIdWithInstructor1;
	SELECT 
        ADAUID
		,AUID
		,ADAHID
		,AHID
		,AHRESV
		,AUINST
		,ADINST
		,AUINNM
		,AHCRS
	INTO 
        #HeaderIdWithInstructor1
    FROM 
        OperationsAnalyticsStage.dbo.Stage_FPCRAU
	    LEFT JOIN OperationsAnalyticsStage.dbo.Stage_FPCRAD
		    ON ADAUID = AUID
	    LEFT JOIN OperationsAnalyticsStage.dbo.Stage_FPCRAH
		    ON ADAHID = AHID
    WHERE 
        AHSDT >= 1120101
	    AND ISNUMERIC(AUINST) = 1
    GROUP BY 
        ADAUID
		,AUID
		,ADAHID
		,AHID
		,AHRESV
		,AUINST
		,ADINST
		,AUINNM
		,AHCRS; 

    ---------------------------------------------------------------------------------------------------
    -- Extracts Unique Header IDs
    -- (922537 rows affected) in 6s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS #HeaderIdWithInstructor; 
    SELECT 
        AHID
    INTO 
        #HeaderIdWithInstructor 
    FROM 
        #HeaderIdWithInstructor1
    GROUP BY 
        AHID;

    ---------------------------------------------------------------------------------------------------
    -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - End
    ---------------------------------------------------------------------------------------------------
    
    ---------------------------------------------------------------------------------------------------
    -- Extract Reservation Detail Records
    -- (1266105 rows affected) in 4:55s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#ReservationDetail];
    SELECT 
         [ah].[AHID]
        ,[ah].[AHCTR]
        ,[ah].[AHCCST]
        ,[ah].[AHRESV]
        ,[ah].[AHCRS]
        ,[ah].[AHSDT]
        ,[ah].[AHCCDT]
        ,[ah].[AHADDT]
        ,[ah].[AHMATL]
    	,[rm].[RMMATL]
        -----------------------------------
        -- Technical Story 269286 - Begin
        -----------------------------------
        ,[cust].[Customer]
        ,[ah].[AHMSTR]
        ,[cust].[CustomerGroup]
        ,IIF([cust].[CustomerGroup] = 14, 'N', 'Y') AS [RevenueFlag]
        -----------------------------------
        -- Technical Story 269286 - End
        -----------------------------------
        --,CASE WHEN AHCRS = '*HRTRN' OR AUINST IN ('*NO', '*TBA') THEN 'N' ELSE 'Y' END AS InstructorFlag
        ,CASE 
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - Begin
        ----------------------------------------------------------------------------
            --WHEN [ah].[AHCRS] = '*HRTRN' THEN 'N' 
            WHEN [ah].[AHCRS] = '*HRTRN' AND [hiwi].[AHID] IS NOT NULL THEN 'Y' 
			WHEN [ah].[AHCRS] = '*HRTRN' AND [hiwi].[AHID] IS NULL THEN 'N'  
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - End
        ----------------------------------------------------------------------------	  
            WHEN [TBA].[AHID] IS NULL AND [NonTBA].[AHID] IS NOT NULL THEN 'Y'
            WHEN [TBA].[AHID] = [NonTBA].[AHID] THEN 'Y'
            WHEN [TBA].[AHID] IS NOT NULL AND [NonTBA].[AHID] IS NULL THEN 'N'
            WHEN [TBA].[AHID] IS NOT NULL AND [NonTBA].[AHID] IS NOT NULL THEN 'Y' 
        END AS [InstructorFlag]
        ,CASE WHEN [ah].[AHCRS] = '*HRTRN' THEN 'Y' ELSE 'N' END AS [HourlyFlag]
        --,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([ri].[RICDAT]) AS [RICDAT]
        ,TRY_CONVERT(date, 
            STUFF(STUFF(IIF([ri].[RICDAT] < 1000101, '19', '20') + RIGHT([ri].[RICDAT], 6), 5, 0, '-'), 8, 0, '-')) AS [RICDAT]
        --,[RICDAT]
        ,ISNULL(CONVERT(int, [ri].[RIRREV]), 0) AS [RIRREV]
        ,CASE 
            WHEN ISNULL(CONVERT(int, [ri].[RIRREV]), 0) = 1 THEN 'Y' 
            ELSE 'N' 
        END AS [ReOpenedFlag_Old]
        ,CASE 
            WHEN ISNULL(DATEDIFF(DAY
                ,TRY_CONVERT(date, 
                    STUFF(STUFF(IIF([ah].[AHCCDT] < 1000101, '19', '20') + RIGHT([ah].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-'))
                --[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([ah].[AHCCDT]), 
                ,TRY_CONVERT(date, 
                    STUFF(STUFF(IIF([ri].[RICDAT] < 1000101, '19', '20') + RIGHT([ri].[RICDAT], 6), 5, 0, '-'), 8, 0, '-'))
                --[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([ri].[RICDAT])
                ), 0) < 10 THEN 'Y' 
            ELSE 'N' 
            END AS [OnTimeFlag_Old]
        ,CASE 
            WHEN [RMC].[RMTYPE] IS NULL THEN 'N' 
            WHEN [rmc].[RMTYPE] = 'OP'  THEN 'Y' 
            ELSE 'N' 
        END AS [ResvReopenFlag]
        --------------------------------------------------------------------------------------------------
        -- Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data) - Begin
        --------------------------------------------------------------------------------------------------
    	--,CASE WHEN ISNULL(DATEDIFF(DAY, [OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate](AH.AHCCDT), [OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate](RI.RICDAT)), 0) < 10 THEN 'Y' ELSE 'N' END AS ROTOnTimeFlag
        --------------------------------------------------------------------------------------------------
        -- Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data) - End
        --------------------------------------------------------------------------------------------------
        ,[ah].[AHCTR] + [ah].[AHRESV] AS [ReservationKey]
        ,[ah].[AHCLRF]
        ,CASE 
            WHEN [rs].[AHRS_RESERVATION] IS NOT NULL AND [cl].[CLSSN] IS NOT NULL THEN 'OLR-C'
            WHEN [rs].[AHRS_RESERVATION] IS NOT NULL AND [cl].[CLSSN] IS NULL THEN 'OLR-T'
            ELSE 'ORN' 
        END AS [ON_LINE_RESV_FLAG]
        ,[ah].[AHLOCT]
        ,[td].[TM_TABLE_DETAIL_DESCRIPTION] AS [ReopenReason]
    	,[ah].[AHPRID]
    	,CASE WHEN [ah].[AHID] = [ah].[AHPRID] THEN 'Y' ELSE 'N' END AS [IsParentReservation_]
    	,MAX([ah].[Updated]) AS [ReservationLastUpdated]
        ---------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - Begin
        ---------------------------------------------------------------------------
    	,[ah].[AHRGUS] AS [ReservationBookingUser]
    
        -----------------------------------
        -- Bug 258129
        -----------------------------------
    	--,AHRGDT as ReservationBookingDate
        --,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([AHRGDT]) AS [ReservationBookingDate]
        ,TRY_CONVERT(date, 
            STUFF(STUFF(IIF([ah].[AHRGDT] < 1000101, '19', '20') + RIGHT([ah].[AHRGDT], 6), 5, 0, '-'), 8, 0, '-')) AS [ReservationBookingDate]
        
        -----------------------------------
        -- Bug 258129
        -----------------------------------
    	--,AHLSDT as CourseCompletedDate   
        -----------------------------------------------------------------------
        -- BUG 266539 - factReservation: Incorrect CourseCompletedDate - Begin
        -----------------------------------------------------------------------	
        ,TRY_CONVERT(date, 
            STUFF(STUFF(IIF([ah].[AHCCDT] < 1000101, '19', '20') + RIGHT([ah].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-')) AS [ReservationDateEnded]
        ----------------------------------------------------------------------------------------------------------------------
        -- BUG 382458 - "CourseCompletedDate" discrepancy between 1X Factreservation and EDW Factreservation table. - Begin
        ----------------------------------------------------------------------------------------------------------------------
        --,TRY_CONVERT(date, 
        --    STUFF(STUFF(IIF([ah].[AHLSDT] < 1000101, '19', '20') + RIGHT([ah].[AHLSDT], 6), 5, 0, '-'), 8, 0, '-')) AS [CourseCompletedDate]
        ,TRY_CONVERT(date, 
            STUFF(STUFF(IIF([ah].[AHCCDT] < 1000101, '19', '20') + RIGHT([ah].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-')) AS [CourseCompletedDate]
        --------------------------------------------------------------------------------------------------------------------
        -- BUG 382458 - "CourseCompletedDate" discrepancy between 1X Factreservation and EDW Factreservation table. - End
        --------------------------------------------------------------------------------------------------------------------
        --,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([ah].[AHLSDT]) AS [ReservationDateEnded]
    	--,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([ah].[AHCCDT]) AS [CourseCompletedDate]
        -----------------------------------------------------------------------
        -- BUG 266539 - factReservation: Incorrect CourseCompletedDate - End
        -----------------------------------------------------------------------
    
    	,[ah].[AHCTR] + [ah].[AHRESV] AS [ReservationMasterNumber]
    
        -----------------------------------
        -- Bug 258129
        -----------------------------------
    	--,[ri].RIESRT as ERecordApprovedTimestamp 
    
        ,CAST([OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate](CAST([ri].[RIESRT] / 1000000000 AS int)) AS datetime) + 
            CAST(CAST(STUFF(STUFF(STUFF(
                RIGHT(REPLICATE('0', 9) + CAST(CAST([ri].[RIESRT] - (CAST([ri].[RIESRT] / 1000000000 AS int) * 1000000000.) AS int) AS varchar(9)), 9)
            , 3, 0, ':'), 6, 0, ':'), 9, 0, '.') AS time) AS datetime) AS [ERecordApprovedTimestamp]
    
        ,[ri].[RIESRU] AS [ERecordApprover]
        -----------------------------------
        -- Bug 258129
        -----------------------------------
    	--,[ri].[RIESTM] AS [ERecordSignatureTimestamp]
        ,CAST([OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate](CAST([ri].[RIESTM] / 1000000000 AS int)) AS datetime) + 
            CAST(CAST(STUFF(STUFF(STUFF(
                RIGHT(REPLICATE('0', 9) + CAST(CAST([ri].[RIESTM] - (CAST([ri].[RIESTM] / 1000000000 AS int) * 1000000000.) AS int) AS varchar(9)), 9)
            , 3, 0, ':'), 6, 0, ':'), 9, 0, '.') AS time) AS datetime) AS [ERecordSignatureTimestamp]
    
    	,[ri].[RIESUS] AS [ERecordSignatureId]
    	,[ah].[AHFCID] AS [CertificateNo]
    	,[ah].[AHMSTR] AS [CertificateHolder]
    	,[ah].[AHCSNM] AS [CertificateHolderName]
    	
        -----------------------------------
        -- Bug 258129
        -----------------------------------
        --,AHIFDT as InsuranceStartDate
        --,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([AHIFDT]) AS [InsuranceStartDate]
        ,TRY_CONVERT(date, 
            STUFF(STUFF(IIF([ah].[AHIFDT] < 1000101, '19', '20') + RIGHT([ah].[AHIFDT], 6), 5, 0, '-'), 8, 0, '-')) AS [InsuranceStartDate]
    	--,AHPTDT as InsuranceExpireDate
        --,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([AHPTDT]) AS [InsuranceExpireDate]
        ,TRY_CONVERT(date, 
            STUFF(STUFF(IIF([ah].[AHPTDT] < 1000101, '19', '20') + RIGHT([ah].[AHPTDT], 6), 5, 0, '-'), 8, 0, '-')) AS [InsuranceExpireDate]
        -----------------------------------
        -- Bug 258129
        -----------------------------------
    
        ,[ah].[AHITYP]  AS [InsuranceType]
        ,[ah].[AHIVAL]  AS [InsuranceValue]
    	,ISNULL([cv].[CVADES], '') AS [ApprovedCourseDescr]
    	,[ah].[AHCCUS]  AS [CourseCompletingUser]
        ,[ah].[AHDESC]  AS [SchedulingDescr]
        ,[ah].[AHVDES]  AS [RecordDescr]
    	,[rm].[RMSP]    AS [IsSinglePilot_]
    	,[ah].[AHTRNT]  AS [TrainingObjective]
    	,[rc1].[RCACTN] AS [ActionToTakeIfSelected]
        --------------------------------------------------------------------------------------
        --266018 - iPad Report EDW development (Added below 3 columns as per Ipad Requirement)
        ---------------------------------------------------------------------------------------
    	,[Ipad].[ClntOrder]     AS [ClntOrder]
    	,[Ipad].[ClntNotes]     AS [ClntNotes]
    	,[Ipad].[ClntDateOrder] AS [ClntDateOrder]
        --------------------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - End
        --------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
        --------------------------------------------------------------------------------------
    	,[ah].[AHTAIL] AS [AircraftTailNo]
        --------------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
        --------------------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        ,[rdm].[DeliveryMethod]
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[ah].[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------
    INTO 
        [#ReservationDetail]
    FROM 
        [OperationsAnalyticsStage].[dbo].[Stage_FPCRAH] AS [ah]
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - Begin
        ----------------------------------------------------------------------------
		LEFT JOIN  #HeaderIdWithInstructor hiwi
		    ON [hiwi].[AHID] = [ah].[AHID] 
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - End
        ----------------------------------------------------------------------------
    	--LEFT hash JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAD] AS [ad]
        --    ON AH.AHID = AD.ADAHID
        --LEFT hash JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAU] AS [au]
        --    ON AD.ADAUID = AU.AUID
        LEFT HASH JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAHRS] AS [rs]
            ON  [ah].[AHRESV] = [rs].[AHRS_RESERVATION] 
            AND [ah].[AHCTR]  = [rs].[AHRS_CENTER]
        LEFT HASH JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAHRH] AS [rh] 
            ON [rh].[AHRH_HEADER_ID] = [rs].[AHRS_HEADER_ID]
        LEFT HASH JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXCL] AS [cl] 
            ON [cl].[CLSSN] = [rh].[AHRH_ADD_USER]
        LEFT OUTER JOIN [#NonTBAInstructor] AS [NonTBA]
            ON [NonTBA].[AHID] = [ah].[AHID]
        LEFT OUTER JOIN [#TBAInstructor] AS [TBA]
            ON [TBA].[AHID] = [ah].[AHID]
        -----------------------------------
        -- Technical Story 269286 - Begin
        -----------------------------------
        --LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[BI0_PCUSTOMER] AS [cust]
        --    ON [cust].[CUSTOMER] = [ah].[AHMSTR]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimCustomer] AS [cust]
            ON [ah].[AHMSTR] = [cust].[Customer]
        -----------------------------------
        -- Technical Story 269286 - End
        -----------------------------------
        -------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - Begin
        -------------------------------------------------------------------------
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCARI] AS [ri]
            ON [ri].[RIAHID] = [ah].[AHID]
        LEFT OUTER JOIN (
            SELECT 
                [RMAHID], [RMMATL], [RMCHID], MAX([RMCVID]) AS [RMCVID], MAX([RMSP]) AS [rmsp]
            FROM 
                [OperationsAnalyticsStage].[dbo].[Stage_FPCRRM]
            GROUP BY 
                [RMAHID], [RMMATL], [RMCHID]) AS [rm]
    	    ON  [rm].[RMAHID] = [ah].[AHID]
            AND [rm].[RMMATL] = [ah].[AHMATL]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXCV] AS [cv]
            ON  [cv].[CVCHID] = [rm].[RMCHID]
            AND [cv].[CVID]   = [rm].[RMCVID]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRRC] AS [rc1]
            ON  [rc1].[RCAHID] = [ah].[AHID]
            AND [rc1].[RCACTN] = 'PROCARD' 
        -------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - End
        -------------------------------------------------------------------------
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXRMC] AS [rmc]
            ON  [rmc].[RMAHID] =  [ah].[AHID]
            AND [rmc].[RMTYPE] ='OP'
            AND [rmc].[RMCGID] = (
                SELECT 
                    MAX([rmc1].[RMCGID]) 
                FROM 
                    [OperationsAnalyticsStage].[dbo].[Stage_FPPXRMC] AS [rmc1] 
                WHERE 
                    [rmc].[RMAHID] = [rmc1].[RMAHID]
                    AND [rmc].[RMTYPE] = [rmc1].[RMTYPE]) 
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_RM_CHANGE_HISTORY] AS [rc]
            ON  [rc].[RMH_CHANGE_ID] = [rmc].[RMCGID]
            AND [rc].[RMH_TYPE] = [rmc].[RMTYPE]
            AND [rc].[RMH_VERSION_TYPE] = 'ROREASON'       
            AND [rc].[RMH_VALUE] = (
                SELECT 
                    MAX([rc1].[RMH_VALUE]) 
                FROM 
                    [OperationsAnalyticsStage].[dbo].Stage_RM_CHANGE_HISTORY AS RC1 
                WHERE 
                    [rc].[RMH_CHANGE_ID] = [rc1].[RMH_CHANGE_ID] 
                	AND [rc].[RMH_TYPE] = [rc1].[RMH_TYPE] 
                	AND [rc].[RMH_VERSION_TYPE] = [rc1].[RMH_VERSION_TYPE])
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_TM_TABLE_DETAILS] AS [td]
            ON  [td].[TM_TABLE_APPLICATION]  = 'RES_MAN' 
            AND [td].[TM_TABLE_DETAIL_TABLE] = 'REOPENREASON' 
            AND [td].[TM_TABLE_DETAIL_KEY]   = [rc].[RMH_VALUE]
        ---------------------------------------------------------------------------
        --266018 - iPad Report EDW development
        ---------------------------------------------------------------------------
    	LEFT OUTER JOIN [#IpadAttributes] AS [Ipad]
    		ON [ah].[AHSSN] = [Ipad].[ClntId] 
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Drv_ReservationDeliveryMethod] AS [rdm]
            ON  [ah].[AHID] = [rdm].[AHID]
            AND [rdm].[IsDeleted] = 0
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
    WHERE 
        [ah].[AHSDT] >= 1120101
    GROUP BY 
         [ah].[AHID]
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - Begin
        ----------------------------------------------------------------------------
		,[hiwi].[AHID]  
	    ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - End
        ----------------------------------------------------------------------------
    	,[ah].[AHCTR]
    	,[ah].[AHCCST]
    	,[ah].[AHRESV]
    	,[ah].[AHCRS]
    	,[ah].[AHSDT]
    	,[ah].[AHCCDT]
    	,[ah].[AHMATL]
    	,[rm].[RMMATL]
    	--,[ADAHID]
    	,CASE 
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - Begin
        ----------------------------------------------------------------------------
            --WHEN [ah].[AHCRS] = '*HRTRN' THEN 'N' 
            WHEN [ah].[AHCRS] = '*HRTRN' AND [hiwi].[AHID] IS NOT NULL THEN 'Y' 
			WHEN [ah].[AHCRS] = '*HRTRN' AND [hiwi].[AHID] IS NULL THEN 'N'  
        ----------------------------------------------------------------------------
        -- BUG 277378 - Missing Reservations - Hourly with instructor [PRD] - End
        ----------------------------------------------------------------------------
    		WHEN [TBA].[AHID] IS NULL AND [NonTBA].[AHID] IS NOT NULL THEN 'Y'
    		WHEN [TBA].[AHID] = [NonTBA].[AHID] THEN 'Y'
    		WHEN [TBA].[AHID] IS NOT NULL AND [NonTBA].[AHID] IS NULL THEN 'N'
    		WHEN [TBA].[AHID] IS NOT NULL AND [NonTBA].[AHID] IS NOT NULL THEN 'Y' 
        END
        -----------------------------------
        -- Technical Story 269286 - Begin
        -----------------------------------
        ,[cust].[Customer]
        ,[ah].[AHMSTR]
        ,[cust].[CustomerGroup]
        -----------------------------------
        -- Technical Story 269286 - End
        -----------------------------------
        ,[rmc].[RMTYPE]
        ,[ri].[RICDAT]
        ,[ri].[RIRREV] 
        ,[TBA].[AHID]
        ,[NonTBA].[AHID]
        ,[ah].[AHCLRF]
        ,CASE 
            WHEN [rs].[AHRS_RESERVATION] IS NOT NULL AND [cl].[CLSSN] IS NOT NULL THEN 'OLR-C'
            WHEN [rs].[AHRS_RESERVATION] IS NOT NULL AND [cl].[CLSSN] IS NULL THEN 'OLR-T'
            ELSE 'ORN' 
        END 
        ,[ah].[AHLOCT] 
        ,[td].[TM_TABLE_DETAIL_DESCRIPTION]
        ,[ah].[AHADDT]
    	,[ah].[AHPRID]
        ---------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - Begin
        ---------------------------------------------------------------------------
    	,[ah].[AHRGUS]
    	,[ah].[AHRGDT]
    	,[ah].[AHLSDT]
    	,[ri].[RIESRT]  
    	,[ri].[RIESRU]  
    	,[ri].[RIESTM]  
    	,[ri].[RIESUS]  
    	,[ah].[AHFCID]  
    	,[ah].[AHMSTR]  
    	,[ah].[AHCSNM]  
    	,[ah].[AHIFDT]  
    	,[ah].[AHPTDT]  
        ,[ah].[AHITYP]  
        ,[ah].[AHIVAL]  
    	,ISNULL([cv].[CVADES], '')  
    	,[ah].[AHCCUS]  
    	,[ah].[AHDESC]  
        ,[ah].[AHVDES]  
     	,[rm].[RMSP]  
    	,[ah].[AHTRNT] 
    	,[rc1].[RCACTN]  
    	,[Ipad].[ClntOrder]
    	,[Ipad].[ClntNotes]
    	,[Ipad].[ClntDateOrder]
        -----------------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - End
        -----------------------------------------------------------------------------------
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
        -----------------------------------------------------------------------------------
    	,[ah].[AHTAIL] 
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
        -----------------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        ,[rdm].[DeliveryMethod] 
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[ah].[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------

    ---------------------------------------------------------------------------------------------------
    -- Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data) - Begin
    -- (678573 rows affected) in 25s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#WeekEndCntByReservationHeader];
    SELECT 
        [AHID]
        ,COUNT([d].[Date]) AS [WeekEndCntByReservationHeader]
    INTO 
        [#WeekEndCntByReservationHeader]
    FROM 
        [#ReservationDetail] AS [r]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimDate] AS [d]
            --ON [d].[Date] BETWEEN [OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([r].[AHCCDT]) AND [r].[RICDAT]
            ON [d].[Date] BETWEEN  
                TRY_CONVERT(date, 
                    STUFF(STUFF(IIF([r].[AHCCDT] < 1000101, '19', '20') + RIGHT([r].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-'))
                AND [r].[RICDAT]
    WHERE 
        [d].[WeekDay] IN ('Saturday', 'Sunday')  
    GROUP BY 
        [r].[AHID];
    ---------------------------------------------------------------------------------------------------
    -- Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data) - End
    ---------------------------------------------------------------------------------------------------

    ---------------------------------------------------------------------------------------------------
    -- Generate Reservation Summary from Reservation Detail
    -- (1266105 rows affected) in 58s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#ReservationSummary];
    SELECT 
         [rd].[AHID]
        ,[rd].[AHCTR]
        ,[rd].[AHCCST]
        ,[rd].[AHRESV]
        ,[rd].[AHCRS]	  
        ,[rd].[AHSDT] 
        ,[rd].[AHCCDT]
        ,[rd].[AHADDT]
        ,[rd].[AHMATL]
    	,[rd].[RMMATL]
        -----------------------------------
        -- Technical Story 269286 - Begin
        -----------------------------------
        ,[rd].[Customer]
        ,[rd].[AHMSTR]
        ,[rd].[CustomerGroup]
        -----------------------------------
        -- Technical Story 269286 - End
        -----------------------------------
        ,[rd].[RevenueFlag]
        ,[rd].[InstructorFlag]
        ,[rd].[HourlyFlag]
        ,[rd].[ReOpenedFlag_Old]
        ,[rd].[OnTimeFlag_Old]
        ,[rd].[ResvReopenFlag]
    	,[wr].[WeekEndCntByReservationHeader]
    	-----------------------------------------------------------------------------
    	--  Technical Story 267568: ROT Timeliness - Correction to Late Rule -- Begin
    	-----------------------------------------------------------------------------
    	,IIF(ISNULL(
            DATEDIFF(DAY
            --,[OperationsAnalyticsStage].[dbo].[fn_DB2toSQLDate]([rd].[AHCCDT])
            ,TRY_CONVERT(date, 
                STUFF(STUFF(IIF([rd].[AHCCDT] < 1000101, '19', '20') + RIGHT([rd].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-'))
            ,[rd].[RICDAT]), 0)
            - ISNULL([wr].[WeekEndCntByReservationHeader], 0) <= 10, 'Y', 'N') AS [ROTOnTimeFlag]
    	-----------------------------------------------------------------------------
    	--  Technical Story 267568: ROT Timeliness - Correction to Late Rule -- End
    	-----------------------------------------------------------------------------
        ,[rd].[RICDAT]
        ,[rd].[RIRREV]
        ,[os].[ODRESV]
        ,[os].[ODTYPE]  
        ,[os].[ODPOTY]  
        --,CASE WHEN [rd].[AHCRS] = '*HRTRN' THEN 99 ELSE [cmat].[DIVISION] END AS [DIVISION]
        --,CASE WHEN [rd].[AHCRS] = '*HRTRN' THEN '999' ELSE [cmat].[Ph2] END AS [PROGRAM]
        --,CMAT.[Group]
        ,CASE WHEN [rd].[AHCRS] = '*HRTRN' THEN 99    ELSE [mat].[Division] END AS [DIVISION]
        ,CASE WHEN [rd].[AHCRS] = '*HRTRN' THEN '999' ELSE [mat].[Program]  END AS [PROGRAM]
        ,[matgrp].[MaterialGroup] AS [Group] 
        ,[rd].[AHCLRF]
        ,[rd].[ON_LINE_RESV_FLAG]
        ,[rd].[AHLOCT]
        ,[rd].[ReopenReason]
    	,[rd].[AHPRID]
    	,[rd].[IsParentReservation_]
    	,[rd].[ReservationLastUpdated]
        -------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - Begin
        -------------------------------------------------------------------------
    	,[rd].[ReservationBookingUser]
    	,[rd].[ReservationBookingDate]
    	,[rd].[ReservationDateEnded]
    	,[rd].[CourseCompletedDate]
    	,[rd].[ReservationMasterNumber]
    	,[rd].[ERecordApprovedTimestamp] 
    	,[rd].[ERecordApprover]
    	,[rd].[ERecordSignatureTimestamp]
    	,[rd].[ERecordSignatureId]
    	,[rd].[CertificateNo]
    	,[rd].[CertificateHolder]
    	,[rd].[CertificateHolderName]
    	,[rd].[InsuranceStartDate]
    	,[rd].[InsuranceExpireDate]
        ,[rd].[InsuranceType]
        ,[rd].[InsuranceValue]
    	,[rd].[ApprovedCourseDescr]
    	,[rd].[CourseCompletingUser]
    	,[rd].[SchedulingDescr] 
        ,[rd].[RecordDescr] 
    	,[rd].[IsSinglePilot_] 
    	,[rd].[TrainingObjective]
    	,[rd].[ActionToTakeIfSelected]
        -------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - End
        -------------------------------------------------------------------------
        --,CASE WHEN HourlyFlag = 'N' THEN 1 ELSE 0 END AS [ReservationTotal] 
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'N' THEN 1 ELSE 0 END AS [ReservationsFinal]
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'N' AND RIRREV > 0 THEN 1 ELSE 0 END AS [ReservationsReopened]
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'N' AND
        --           ISNULL(DATEDIFF(DAY, AHCCDT, RICDAT), 0) < 10 THEN 1 ELSE 0 END AS ReservationsOnTime
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'N' AND InstructorFlag = 'Y' THEN 1 ELSE 0 END AS ReservationWithInstructor
        --,CASE WHEN AHCCST = 'F' AND InstructorFlag = 'N' THEN 1 ELSE 0 END AS ReservationWithNoInstructor
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'N' AND RevenueFlag = 'Y' THEN 1 ELSE 0 END AS ReservationRevenue
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'N' AND RevenueFlag = 'N' THEN 1 ELSE 0 END AS ReservationNonRevenue
        --,CASE WHEN AHCCST = 'F' AND HourlyFlag = 'Y' THEN 1 ELSE 0 END AS ReservationHourly
    	,[rd].[ClntOrder]
    	,[rd].[ClntNotes]
    	,[rd].[ClntDateOrder]
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
        -----------------------------------------------------------------------------------
    	,[rd].[AircraftTailNo]
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
        -----------------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        ,[rd].[DeliveryMethod]
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ,TRY_CONVERT(date, STUFF(STUFF(IIF([rd].[AHCCDT] < 1000101, '19', '20') + RIGHT([rd].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-')) AS [AHCCDT_Date]
		,IIF(DATEPART(WEEKDAY, DATEADD(Day, 0, TRY_CONVERT(date, STUFF(STUFF(IIF([rd].[AHCCDT] < 1000101, '19', '20') + RIGHT([rd].[AHCCDT], 6), 5, 0, '-'), 8, 0, '-')))) IN (7)
            , 1, 0) AS [StartDateOffSet]
		,IIF(DATEPART(WEEKDAY, DATEADD(Day, 0, [rd].[RICDAT])) IN (7), 1, 0) AS [RICDATDateOffSet]
		,IIF(DATEPART(WEEKDAY, DATEADD(Day, 0, GETDATE() - 1)) IN (7), 1, 0) AS [CurrentDateOffSet]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[rd].[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------
    INTO 
        [#ReservationSummary]
    FROM 
        [#ReservationDetail] AS [rd]
        --LEFT OUTER JOIN [MSR].[Stage_CourseMaterial] AS [cmat]
        --    ON [ah].[AHMATL] = [cmat].[Material]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimMaterial] AS [mat]
            ON [mat].[Material] = [rd].[AHMATL]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimMaterialGroup] AS [matgrp]
            ON [matgrp].[Id] = [mat].[MaterialGroupId] 
        LEFT OUTER JOIN [#OrderSummary] AS [os]
            ON [os].[ODRESV] = [rd].[ReservationKey]
        -------------------------------------------------------------------------------------------------
        -- Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data) - Begin
        -------------------------------------------------------------------------------------------------
        LEFT OUTER JOIN [#WeekEndCntByReservationHeader] AS [wr]
            ON [rd].[AHID] = [wr].[AHID];
        -------------------------------------------------------------------------------------------------
        -- Technical Story 237417 - ROT Timeliness - Change from calendar to business days (data) - End
        -------------------------------------------------------------------------------------------------    

    ---------------------------------------------------------------------------------------------------
    -- The logic below is to update the Textron Reservation Centers as part of the Story 
    -- 199361:Textron - Report Ops data against Textron Centers - EDW Dev
    ---------------------------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------------------------
    -- Load Textron Centers into temp table
    -- (27 rows affected)
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#TextronCenters];
    SELECT 
         [TM_TABLE_DETAIL_STATUS]
        ,[TM_TABLE_APPLICATION]
        ,[TM_TABLE_DETAIL_TABLE]
        ,[TM_TABLE_DETAIL_KEY]    
        ,[TM_TABLE_DETAIL_DESCRIPTION]
        ,[TM_TABLE_DETAIL_ALTERNATE_KEY]
        ,[TM_TABLE_DETAIL_ALTERNATE_KEY_1] 
    INTO 
        [#TextronCenters]
    FROM 
        [OperationsAnalyticsStage].[dbo].[Stage_TM_TABLE_DETAILS]
    WHERE 
        [TM_TABLE_APPLICATION] ='SITELB' 
        AND [TM_TABLE_DETAIL_TABLE] = 'DELVCONVERSION';

    ---------------------------------------------------------------------------------------------------
    -- Extract the Product Hierarchy for Hourly Reservations based on ADBSIM
    -- (15159 rows affected)
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#ResvHourly];
    SELECT 
         [r].[AHID]
        ,[r].[AHCTR]
    	,[ad].[ADAHID]
    	,MIN([ad].[ADID]) AS [ADID]
    	,ISNULL([XAT1].[ATTRTP], [XAT2].[ATTRTP]) AS [ATTRTP]
    	,[xsi].[SIPRDH]
    INTO 
        [#ResvHourly]
    FROM 
        [#ReservationSummary] AS [r]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAD] AS [ad]
            ON  [ad].[ADAHID] = [r].[AHID]
            AND [ad].[ADSDAT] = [r].[AHSDT]  
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXAT] AS [XAT1]
            ON  [ad].[ADMODT] = [XAT1].[ATMODT]
            AND [ad].[ADACTV] = [XAT1].[ATACTV]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXAT] AS [XAT2]
            ON  [ad].[ADMODT] = [XAT2].[ATMODT]
            AND [XAT2].[ATACTV] = '*'
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXSI] AS [xsi]
            ON [ad].[ADBSIM] = [xsi].[SISIM]
    WHERE 
        [r].[AHSDT] >= 1190801
        AND [r].[HourlyFlag] = 'Y'
        AND ISNULL([XAT1].[ATTRTP], [XAT2].[ATTRTP]) = 'S'
    GROUP BY 
         [r].[AHID]
        ,[r].[AHCTR]
    	,[ad].[ADAHID]
    	,ISNULL([XAT1].[ATTRTP], [XAT2].[ATTRTP])
    	,[xsi].[SIPRDH]; 
    
    ---------------------------------------------------------------------------------------------------
    -- Eliminate the duplicates
    -- (15030 rows affected)
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#ReservationHourly];
    SELECT 
         [r1].[AHID]
        ,[r1].[AHCTR]
    	,[r1].[SIPRDH]
    INTO 
        [#ReservationHourly]
    FROM 
        [#ResvHourly] AS [r1]
    WHERE 
        [ADID] = (
            SELECT 
                MIN([r2].[ADID])
    	    FROM 
                [#ResvHourly] AS [r2]
            WHERE 
                [r1].[AHID] = [r2].[AHID]); 
    
    ---------------------------------------------------------------------------------------------------
    -- Combine Non Hourly and Hourly  
    -- (442727 rows affected)
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#TextronReservation1];
    SELECT 
         [AHID]
        ,[AHCTR]
    	,[PROGRAM]
    INTO 
        [#TextronReservation1]
    FROM 
        [#ReservationSummary]
    WHERE 
        [HourlyFlag] = 'N'
        AND [AHSDT] >= 1190801
    
    UNION
    
    SELECT 
         [AHID]
        ,[AHCTR]
    	,[SIPRDH]
    FROM 
        [#ReservationHourly];
    
    ---------------------------------------------------------------------------------------------------
    -- Load Textron Reservations
    -- (89253 rows affected)
    ---------------------------------------------------------------------------------------------------
    -- Extract Textron Reservation with specific product hierarchies
    DROP TABLE IF EXISTS [#TextronReservation];
    SELECT 
         [tr].[AHID]
        ,[tr].[AHCTR]
        --,[PROGRAM]
        --,[TM_TABLE_DETAIL_ALTERNATE_KEY] AS [TextronCenter]
        ,[cr].[MasterCenterId] AS [TextronCenter]
    INTO 
        [#TextronReservation]
    FROM 
        [#TextronReservation1] AS [tr]
        INNER JOIN [#TextronCenters] AS [tc]
            ON [tr].[AHCTR] + SUBSTRING([tr].[PROGRAM], 1, 5) = [tc].[TM_TABLE_DETAIL_KEY]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenterRollup] AS [cr]
            ON [tc].[TM_TABLE_DETAIL_ALTERNATE_KEY] = [cr].[RollupCenterId]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenter] AS [c]
            ON [cr].[MasterCenterId] = [c].[Id]
        INNER JOIN [OperationsAnalytics].[dbo].[DimCenterCategory] AS [cc]
            ON [cc].[Id] = [c].[CenterCategoryID]
    WHERE 
        [cc].[Id] = 7
    
    UNION
    
    -- Extract Textron Reservation with all product hierarchies
    SELECT DISTINCT 
         [tr].[AHID]
        ,[tr].[AHCTR]
        --,[PROGRAM]
        --,[TM_TABLE_DETAIL_ALTERNATE_KEY] AS [TextronCenter]
        ,[cr].[MasterCenterId] AS [TextronCenter]
    FROM 
        [#TextronReservation1] AS [tr]
        INNER JOIN [#TextronCenters] AS [tc]
            ON [tr].[AHCTR] + '*ALL' = [tc].[TM_TABLE_DETAIL_KEY]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenterRollup] AS [cr]
            ON [cr].[RollupCenterId] = [tc].[TM_TABLE_DETAIL_ALTERNATE_KEY]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenter] AS [c]
            ON [cr].[MasterCenterId] = [c].[Id]
        INNER JOIN [OperationsAnalytics].[dbo].[DimCenterCategory] AS [cc]
            ON [cc].[Id] = [c].[CenterCategoryID]
    WHERE 
        [tr].[AHCTR] IN (1173, 6103, 6115);

    ---------------------------------------------------------------------------------------------------
    -- Get FromProgramId Info
    -- Currently [RRN] column is used to eliminate multiples on [AHID] and [ProgramKey]
    -- There is probable a better way to eliminate duplicates but we need the business logic (possibly 
    -- include material in the filtering/join condition?)
    -- (58124 rows affected) in 4s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#FromProgram];
    WITH [FromProgram] AS (
    SELECT   
         [fp].[DIAHID]
        ,[fp].[DIDTFM]
        ,[dp].[ProgramKey]
        ,[dp].[Id] AS [FromProgramId]
        ,[fp].[DIMATL]
        ,[fp].[RRN] 
        ,COUNT(1) OVER (PARTITION BY [DIAHID]) AS [MultiCount]
        ,ROW_NUMBER() OVER (
            PARTITION BY 
                [DIAHID]
            ORDER BY 
                [RRN] DESC) AS [ProgramSeqNum] --> Use latest RRN as the determining row
    FROM
        [OperationsAnalyticsStage].[dbo].[Stage_AS400_FSCADALL_FPCRDI] AS [fp]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimProgram] AS [dp]
            ON LEFT([fp].[DIDTFM], 10) = [dp].[ProgramKey]
    )
    SELECT 
        * 
    INTO 
        [#FromProgram]
    FROM 
        [FromProgram] 
    WHERE 
        [ProgramSeqNum] = 1;

    -----------------------------------------------------------------------------------------------
    -- Final temp table before merge
    -- (1265924 rows affected) in 1:05s
    -- This is gaining rows from multiples on [FromProgrmaId]
    -----------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#MergeSourceResv1];
    SELECT DISTINCT
         [dd].[Year]                    AS [Year]
        ,[dd].[Month]                   AS [Month]
        ,DAY([dd].[Date])               AS [Day]
        ,1                              AS [BusinessUnitId]
        ,IIF([tr].[TextronCenter] IS NOT NULL, [tr].[TextronCenter], [c].[MasterCenterId]) AS [CenterId]
      	,[comp].[Id]                    AS [CompanyId]
        ,[div].[Id]                     AS [DivisionId]
        --,[c].[MasterCenterId]
        ,[prg].[Id]                     AS [ProgramId]
        ,[potype].[Id]                  AS [POTypeId]
        ,[sotype].[Id]                  AS [SalesOrderTypeId]
        ,[ts].[Id]                      AS [TrainingStatusID]
        -------------------------------------------------------------------------------------------
        -- BUG 424875 1X - Customer Profile Dashboard - Begin
        -------------------------------------------------------------------------------------------
        --,[cust].[Id]                    AS [CustomerId]
		,ISNULL([cust].[Id], -1)          AS [CustomerId]
        -------------------------------------------------------------------------------------------
        -- BUG 424875 1X - Customer Profile Dashboard - End
        -------------------------------------------------------------------------------------------
    	,[custgroup].[Id]               AS [CustomerGroupId]
        -------------------------------------------------------------------------------------------
        -- BUG 424875 1X - Customer Profile Dashboard - Begin
        -------------------------------------------------------------------------------------------
        --,[client].[Id]                  AS [ClientId]
		,ISNULL([client].[Id],-1)       AS [ClientId]
        -------------------------------------------------------------------------------------------
        -- BUG 424875 1X - Customer Profile Dashboard - End
        -------------------------------------------------------------------------------------------
        ,[course].[Id]                  AS [CourseId]
    	,[resvmat].[Id]                 AS [ReservationMaterialId]
    	,[trngmat].[Id]                 AS [TrainingMaterialId]
        ,[ct].[Id]                      AS [CourseTypeID]     
        ,[r].[RevenueFlag]
        ,[r].[InstructorFlag]
        ,[r].[HourlyFlag]
        ,[r].[ReOpenedFlag_Old]
        ,[r].[OnTimeFlag_Old]
        ,IIF([r].[IsParentReservation_] = 'Y', 1, 0) AS [ReservationTotal]
        ,[r].[AHID]                     AS [HeaderId]
        ,[r].[Group]
        ,[rt].[Id]                      AS [ReservationTypeId]
        --CASE 
        --      WHEN dimauth.[Authority] = 'Other/Unknown' THEN ISNULL(rl.[Authority_New_Id], 9)   
        --      ELSE dimauth.[Id] 
        --END AS [TrainingAuthorityId],
        ,[dimauth].[Id]                 AS [TrainingAuthorityId]
        ,CASE 
            WHEN CHARINDEX('Test', [r].[ReopenReason]) > 0 THEN 'N'
            WHEN [r].[ResvReopenFlag] = 'N' THEN 'N' 
            WHEN [r].[ResvReopenFlag] = 'Y' AND [lkpROP].[ImpactAccuracyFlag] IS NULL THEN 'Y'
            WHEN [r].[ResvReopenFlag] = 'Y' AND [lkpROP].[ImpactAccuracyFlag] = 'Y' THEN 'Y' 
            WHEN [r].[ResvReopenFlag] = 'Y' AND [lkpROP].[ImpactAccuracyFlag] = 'N' THEN 'N' 
        END AS [ReopenedFlag]
		---------------------------------------------------------------------------
        -- Technical Story 282292 - Update OnTimeFlag logic in EDW - Begin
        ---------------------------------------------------------------------------
        --,CASE 
        --    WHEN CHARINDEX('Test', [r].[ReopenReason]) > 0 THEN 'Y'
        --    WHEN [r].[ROTOnTimeFlag] = 'Y' THEN 'Y'
        --    WHEN [r].[ROTOnTimeFlag] = 'N' AND [lkpROP].[ImpactTimelinessFlag] IS NULL THEN 'N'
        --    WHEN [r].[ROTOnTimeFlag] = 'N' AND [lkpROP].[ImpactTimelinessFlag] = 'Y'   THEN 'N'
        --    WHEN [r].[ROTOnTimeFlag] = 'N' AND [lkpROP].[ImpactTimelinessFlag] = 'N'   THEN 'Y'		
        --END AS [OnTimeFlag]
		---------------------------------------------------------------------------
        -- Technical Story 282292 - Update OnTimeFlag logic in EDW - End
        ---------------------------------------------------------------------------
        ,[r].[ReopenReason]
        ,CAST(NULL AS smallint)         AS [CenterId_Source]
        ,CAST(NULL AS varchar(100))     AS [ContractNumber]
    	,CAST(NULL AS varchar(100))     AS [SalesOrderNumber]
        ,CAST(NULL AS money)            AS [ContractTrainingRevenue]
        ,CAST(NULL AS money)            AS [SalesOrderTrainingRevenue]
        ,CAST(NULL AS money)            AS [TotalRevenue]
        ,CAST(NULL AS money)            AS [SimFlyTimeRevenue]
        ,CAST(NULL AS decimal(6, 2))    AS [SimHrs]
        ,CAST(NULL AS decimal(6, 2))    AS [FlyTimeHrs]
        ,CAST(NULL AS decimal(6, 2))    AS [NonFlyTimeHrs]
        ,CAST(NULL AS money)            AS [RevenuePerSimHr(ByFlyTime)]
    	,[r].[IsParentReservation_]
        ,CAST(NULL AS smallint)         AS [ReturnForTraining(months)]
        ,CAST(NULL AS date)             AS [ReturnForTrainingDate]
        ,[r].[AHLOCT]                   AS [RecordLocator]
        ,[r].[AHRESV]                   AS [ReservationReferenceNumber]
        ,[dd2].[Date]                   AS [ReservationDateAdded]
        ,[dd].[Date]                    AS [ReservationDateStarted]
        -------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - Begin
        -------------------------------------------------------------------------
    	,[r].[ReservationDateEnded]
    	,[r].[ReservationBookingUser]
    	,[r].[ReservationBookingDate]
    	,[r].[ReservationLastUpdated]
    	,[r].[CourseCompletedDate]
    	,[r].[ReservationMasterNumber]
    	,[r].[ERecordApprovedTimestamp] 
    	,[r].[ERecordApprover]
    	,[r].[ERecordSignatureTimestamp]
    	,[r].[ERecordSignatureId]
    	,[r].[CertificateNo]
    	,[r].[CertificateHolder]
    	,[r].[CertificateHolderName]
    	,[r].[InsuranceStartDate]
    	,[r].[InsuranceExpireDate]
        ,[r].[InsuranceType]
        ,[r].[InsuranceValue]
    	,[r].[ApprovedCourseDescr]
    	,[r].[CourseCompletingUser]
    	,[r].[SchedulingDescr]
        ,[r].[RecordDescr] 
    	,[r].[IsSinglePilot_] 
    	,[r].[TrainingObjective]
    	,[r].[ActionToTakeIfSelected]
        ,[r].[AHCTR]
        ,[r].[AHRESV]
        ,[dd].[Date]                AS [AHSDT]
        ,[dd1].[Date]               AS [AHCCDT]
    	,[dd2].[Date]               AS [AHADDT]
    	,[r].[AHCLRF]
        ,[r].[AHID]
    	,[r].[AHPRID]
        ,[r].[AHCCST]
        ,[r].[AHCRS]
        ,[r].[AHMATL]
    	,[r].[RMMATL]
        ,[r].[AHMSTR]
        -----------------------------------
        -- Technical Story 269286 - Begin
        -----------------------------------
        --,[r].[CUST_GROUP]
        ,[r].[CustomerGroup]        AS [CUST_GROUP]
        -----------------------------------
        -- Technical Story 269286 - End
        -----------------------------------
        ,[r].[RICDAT]
        ,[r].[RIRREV]
        ,[r].[ODRESV]
        ,[r].[ODTYPE]
        ,[r].[ODPOTY]
        ,[r].[DIVISION]
        ,[r].[PROGRAM]
    	,[r].[ClntOrder]
    	,[r].[ClntNotes]
        ,CAST(
            CASE LEFT(CAST([r].[ClntDateOrder] AS varchar(7)), 1) 
            WHEN 1 THEN '20' + SUBSTRING(CAST([r].[ClntDateOrder] AS varchar(7)), 2, 2) + '-' + 
                SUBSTRING(CAST([r].[ClntDateOrder] AS varchar(7)), 4, 2) + '-' + 
                RIGHT(CAST([r].[ClntDateOrder] AS varchar(7)), 2)
        END AS date) AS [ClntDateOrder]
        -------------------------------------------------------------------------
        -- Technical Story 252391: factReservation refactoring - Data Dev - End
        -------------------------------------------------------------------------
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
        -----------------------------------------------------------------------------------
    	,[r].[AircraftTailNo]
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
        -----------------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        ,[rdm].[DeliveryMethod]
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Requirement 267451 - Add column [FromProgram] - Begin
        ---------------------------------------------------------------------------
        ,[fp].[FromProgramId]
        ---------------------------------------------------------------------------
        -- Technical Requirement 267451 - Add column [FromProgram] - End
        -------------------------------------------------------------------------- 
        ---------------------------------------------------------------------------
        -- Technical Story 282292 - Update OnTimeFlag logic in EDW - Begin
        ---------------------------------------------------------------------------
       ,CASE
			WHEN [dd].[Date]  < '2022-09-01'
			THEN 
				CASE 
                    WHEN [dimauth].[Id] = 4 AND [ct].[Id] IN (11, 21, 33, 51, 53) THEN 5
                    --WHEN cust.customer in ('0000027769', '0000002615') THEN cust.ROTTargetDays
                    WHEN  custattr.Customer IS NOT NULL THEN custattr.ROTTargetDays
                    ELSE 10
                END
			ELSE
				CASE 
                    --WHEN cust.customer in ('0000027769', '0000002615') THEN cust.ROTTargetDays
                    WHEN  custattr.Customer IS NOT NULL THEN custattr.ROTTargetDays
                    ELSE 5
                END
		END AS [ROTTargetDays]

        ,CASE 
			WHEN [r].[ReservationDateEnded] >  GETDATE() 
			    OR ([ts].[Id] = 7 AND [r].[RICDAT] IS NULL)
				OR [r].[ReservationDateEnded] IS NULL
				OR [r].[HourlyFlag] = 'Y' 
				OR [r].[IsParentReservation_] = 'N'
			    THEN NULL
			ELSE
				CASE 
                    WHEN [r].[RICDAT] IS NOT NULL AND [r].[AHCCDT] IS NOT NULL
				        THEN IIF([r].[RICDAT] >= [r].[AHCCDT_Date],
                            DATEDIFF(DAY, [r].[AHCCDT_Date], [r].[RICDAT]) 
                            - (DATEDIFF(WEEK, [r].[AHCCDT_Date], [r].[RICDAT])) * 2  
                            + [r].RICDATDateOffSet, 0)
                    WHEN [r].[AHCCDT] IS NULL THEN NULL
                    WHEN [r].[RICDAT] IS NULL AND [r].[AHCCDT] IS NOT NULL
                        THEN IIF(@AsOfDate >= [r].[AHCCDT_Date],
                            DATEDIFF(DAY, [r].[AHCCDT_Date], @AsOfDate) 
                            - (DATEDIFF(WEEK, [r].[AHCCDT_Date], @AsOfDate)) * 2 
                            + [r].[CurrentDateOffSet] + [r].[startDateOffSet], 0)
                    WHEN [r].[RICDAT] IS NULL AND [r].[AHCCDT] IS NULL THEN NULL
				END
		END AS [ROTElapsedDays] 
		---------------------------------------------------------------------------
        -- Technical Story 282292 - Update OnTimeFlag logic in EDW - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[r].[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------

    INTO 
        [#MergeSourceResv1]
    FROM 
        [#ReservationSummary] AS [r]
		---------------------------------------------------------------------------
        -- BUG 362186 - Missing Company Codes in OPS -Reservation - Begin
        ---------------------------------------------------------------------------
	    LEFT OUTER JOIN [#TextronReservation] AS [tr]
            ON [tr].[AHID] = [r].[AHID] 
		---------------------------------------------------------------------------
        -- BUG 362186 - Missing Company Codes in OPS -Reservation - End
        ---------------------------------------------------------------------------
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenterRollup] AS [c]
            ON [c].[RollupCenterId] = [r].[AHCTR]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenter] AS [ctr]
		---------------------------------------------------------------------------
        -- BUG 362186 - Missing Company Codes in OPS -Reservation - Begin
        ---------------------------------------------------------------------------
		    --ON [ctr].[Id] = [c].[MasterCenterId]
            ON [ctr].[Id] = IIF([tr].[TextronCenter] IS NOT NULL, [tr].[TextronCenter], [c].[MasterCenterId])  
		---------------------------------------------------------------------------
        -- BUG 362186 - Missing Company Codes in OPS -Reservation - End
        ---------------------------------------------------------------------------
        INNER JOIN [OperationsAnalytics].[dbo].[dimDivision] AS [div]
            ON [div].[Id] = [r].[DIVISION]
        INNER JOIN [OperationsAnalytics].[dbo].[dimProgram] AS [prg] -- To avoid foreign key violations in the target table
            ON [r].[PROGRAM] = [prg].[ProgramKey]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimCompany] AS [comp]
            ON [comp].[Id] = [ctr].[CompanyId]
        -- Training Authority Joins --
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Drv_TrainingAuthority] AS [auth]
            ON  [r].[AHCTR] = [auth].[AHCTR]
            AND [r].[AHRESV] = [auth].[AHRESV]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[lkpTrainingAuthority] AS [lkpta]
            ON ISNULL([auth].[AuthorityList], '') = [lkpta].[AuthorityList]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimTrainingAuthority] AS [dimauth] -- Join to get the dimension key
            ON ISNULL([lkpta].[AuthoritySlicer], 'Other/Unknown') = [dimauth].[Authority] 
        --LEFT OUTER JOIN #RecordLocator AS rl
        --    ON rl.[AHLOCT] = r.[AHLOCT]
		---------------------------------------------------------------------------
        -- BUG 362186 - Missing Company Codes in OPS -Reservation - Begin
        ---------------------------------------------------------------------------
        --LEFT OUTER JOIN [#TextronReservation] AS [tr]
        --    ON [tr].[AHID] = [r].[AHID] 
		---------------------------------------------------------------------------
        -- BUG 362186 - Missing Company Codes in OPS -Reservation - End
        ---------------------------------------------------------------------------
        -- Join for [ReservationDateStarted]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimDate] AS [dd]
            ON [dd].[Db2Date] = [r].[AHSDT]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimDate] AS [dd1]
            ON [dd1].[Db2Date] = [r].[AHCCDT]
        -- Join for ReservationDateAdded
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimDate] AS [dd2]
            ON [dd2].[Db2Date] = [r].[AHADDT]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimPOType] AS [potype]
            ON [potype].[POType] = [r].[ODPOTY]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimSalesOrderType] AS [sotype]
            ON [sotype].[SalesOrderType] = [r].[ODTYPE]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimMaterial] AS [trngmat]
            ON [trngmat].[Material] = [r].[AHMATL]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimMaterial] AS [resvmat]
            ON [resvmat].[Material] = [r].[RMMATL]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimCourseType] AS [ct]
            ON [ct].[CourseType] =    
                CASE 
                    WHEN [r].[AHCRS] = '*HRTRN' THEN  '-1' 
                    WHEN [r].[Group] IS NULL THEN  ' '  
                    ELSE [r].[Group] 
                END
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimTrainingStatus] AS [ts]
            ON [r].[AHCCST] = [ts].[TrainingStatus]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimCustomer] AS [cust]
            ON [r].[AHMSTR] = [cust].[Customer]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_BMD_CustomerAttribute] AS [custattr]
            ON [custattr].[Customer] = [cust].[Customer]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimCustomerGroup] AS [custgroup]
            ON [cust].[CustomerGroup] = [custgroup].[CustomerGroup]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimClient] AS [client]
            ON [client].[Client_Reference_Number] = [r].[AHCLRF]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimCourse] AS [course]
            ON [course].[CourseLookup] =
		---------------------------------------------------------------------------
		-- Technical Story 288610: factReservation: Refer to Dummy Hourly Course in dimCourse - Begin
		---------------------------------------------------------------------------
			CASE 
				WHEN AHCRS = '*HRTRN' THEN '*hourly*'
				ELSE [r].[AHMATL]
			END
		---------------------------------------------------------------------------
		-- Technical Story 288610: factReservation: Refer to Dummy Hourly Course in dimCourse - End
		---------------------------------------------------------------------------
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimReservationType] AS [rt]
            ON [rt].[ReservationType] = [r].[ON_LINE_RESV_FLAG]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[lkpReopenReason] AS [lkpROP]
            ON [lkpROP].[ReopenReason] = [r].[ReopenReason]
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Drv_ReservationDeliveryMethod] AS [rdm]
            ON  [r].[AHID] = [rdm].[AHID]
            AND [rdm].[IsDeleted] = 0
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
    	-- Technical Requirement 267451 - Add column [FromProgramId] - Begin
    	---------------------------------------------------------------------------
        LEFT OUTER JOIN [#FromProgram] AS [fp]
            ON [r].[AHID] = [fp].[DIAHID]
    	---------------------------------------------------------------------------
    	-- Technical Requirement 267451 - Add column [FromProgramId] - End
    	---------------------------------------------------------------------------
    WHERE 
        [r].[AHID] <> 109575248;

    -----------------------------------------------------------------------------------------------
    -- Create second Merge Source Reserveration Table
    -----------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS [#MergeSourceResv2];
	SELECT 
         [Year]
        ,[Month]
        ,[Day]
        ,[BusinessUnitId]
        ,[CenterId]
        ,[CompanyId]
        ,[DivisionId]
        ,[ProgramId]
        ,[POTypeId]
        ,[SalesOrderTypeId]
        ,[TrainingStatusId]
        ,[CustomerId]
        ,[CustomerGroupId]
        ,[ClientId]
        ,[CourseId]
        ,[ReservationMaterialId]
        ,[TrainingMaterialId]
        ,[CourseTypeId]
        ,[RevenueFlag]
        ,[InstructorFlag]
        ,[HourlyFlag]
        ,[ReOpenedFlag_Old]
        ,[OnTimeFlag_Old]
        ,[ReservationTotal]
        ,[HeaderId]
        ,[Group]
        ,[ReservationTypeId]
        ,[TrainingAuthorityId]
        ,[ReOpenedFlag]
        --,[OnTimeFlag]
        ,[ReOpenReason]
        ,[CenterId_Source]
        ,[ContractNumber]
    	,[SalesOrderNumber]
        ,[ContractTrainingRevenue]
        ,[SalesOrderTrainingRevenue]
        ,[TotalRevenue]
        ,[SimFlyTimeRevenue]
        ,[SimHrs]
        ,[FlyTimeHrs]
        ,[NonFlyTimeHrs]
        ,[RevenuePerSimHr(ByFlyTime)]
        ,[IsParentReservation_]
        ,[ReturnForTraining(months)]
        ,[ReturnForTrainingDate]
        ,[RecordLocator]
        ,[ReservationReferenceNumber]
        ,[ReservationDateAdded]      
        ,[ReservationDateStarted]    
        ,[ReservationDateEnded]
        ,[ReservationBookingUser]
        ,[ReservationBookingDate]
        ,[ReservationLastUpdated]
        ,[CourseCompletedDate]
        ,[ReservationMasterNumber]
        ,[ERecordApprovedTimestamp]
        ,[ERecordApprover]
        ,[ERecordSignatureTimestamp]
        ,[ERecordSignatureId]
        ,[CertificateNo]
        ,[CertificateHolder]
        ,[CertificateHolderName]
        ,[InsuranceStartDate]
        ,[InsuranceExpireDate]
        ,[InsuranceType]
        ,[InsuranceValue]
        ,[ApprovedCourseDescr]
        ,[CourseCompletingUser]
        ,[SchedulingDescr]
        ,[RecordDescr]
        ,[IsSinglePilot_]
        ,[TrainingObjective]
        ,[ActionToTakeIfSelected]
        ,[AHCTR]
        ,[AHRESV]
        ,[AHSDT]
        ,[AHCCDT]
        ,[AHADDT]
        ,[AHCLRF]
        ,[AHID]
        ,[AHPRID]
        ,[AHCCST]
        ,[AHCRS]
        ,[AHMATL]
        ,[RMMATL]
        ,[AHMSTR]
        ,[CUST_GROUP]
        ,[RICDAT]
        ,[RIRREV]
        ,[ODRESV]
        ,[ODTYPE]
        ,[ODPOTY]
        ,[DIVISION]
        ,[PROGRAM]
        ,[ClntOrder]
        ,[ClntNotes]
        ,[ClntDateOrder]
		,[AircraftTailNo]
		,[DeliveryMethod]
		,[FromProgramId]
		,[ROTElapsedDays]
		,[ROTTargetDays]
		,CASE 
			WHEN [ROTElapsedDays] <= [ROTTargetDays] THEN 'Y'
			ELSE 'N'
		END AS [OnTimeFlag_new]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------
	INTO 
        [#MergeSourceResv2]
	FROM 
        [#MergeSourceResv1];

	-------------------------------------------------------------------------------------------------------------
    -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - Begin
    -------------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS [#FirstSIMDate];

	SELECT 
         [ah].[AHID]
		,MIN(CASE WHEN [ad].[ADMODT] = 'SIM' AND [ad].[ADSTAT] IN ('S', 'P', 'C') THEN [d].[Date] END) AS [FirstSIMDate]
    INTO 
        [#FirstSIMDate]
	FROM 
        [OperationsAnalyticsstage].[dbo].[Stage_FPCRAH] AS [ah]
	    INNER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAD] AS [ad]
	        ON [ad].[ADAHID] = [ah].[AHID]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimDate] AS [d]
	        ON [d].[Db2Date] = [ad].[ADSDAT]
	 GROUP BY 
        [ah].[AHID]

    -------------------------------------------------------------------------------------------------------------
    -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - End
    -------------------------------------------------------------------------------------------------------------

	--select OnTimeFlag_new, count(OnTimeFlag_new) from #MergeSourceResv1 group by OnTimeFlag_new -- N: 1350203, Y: 19
	--select Reopenreason, charindex('Test', reopenreason) from  #MergeSourceResv1 where reopenreason like '%Test'

    -----------------------------------------------------------------------------------------------
    -- Create final Merge Source Reservation table 
    -----------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS [#MergeSourceResv];
	SELECT 
         [r].[Year]
        ,[r].[Month]
        ,[r].[Day]
        ,[r].[BusinessUnitId]
        ,[r].[CenterId]
        ,[r].[CompanyId]
        ,[r].[DivisionId]
        ,[r].[ProgramId]
        ,[r].[POTypeId]
        ,[r].[SalesOrderTypeId]
        ,[r].[TrainingStatusId]
        ,[r].[CustomerId]
        ,[r].[CustomerGroupId]
        ,[r].[ClientId]
        ,[r].[CourseId]
        ,[r].[ReservationMaterialId]
        ,[r].[TrainingMaterialId]
        ,[r].[CourseTypeId]
        ,[r].[RevenueFlag]
        ,[r].[InstructorFlag]
        ,[r].[HourlyFlag]
        ,[r].[ReOpenedFlag_Old]
        ,[r].[OnTimeFlag_Old]
        ,[r].[ReservationTotal]
        ,[r].[HeaderId]
        ,[r].[Group]
        ,[r].[ReservationTypeId]
        ,[r].[TrainingAuthorityId]
        ,[r].[ReOpenedFlag]
        --,[r].[OnTimeFlag]
        ,[r].[ReOpenReason]
        ,[r].[CenterId_Source]
        ,[r].[ContractNumber]
    	,[r].[SalesOrderNumber]
        ,[r].[ContractTrainingRevenue]
        ,[r].[SalesOrderTrainingRevenue]
        ,[r].[TotalRevenue]
        ,[r].[SimFlyTimeRevenue]
        ,[r].[SimHrs]
        ,[r].[FlyTimeHrs]
        ,[r].[NonFlyTimeHrs]
        ,[r].[RevenuePerSimHr(ByFlyTime)]
        ,[r].[IsParentReservation_]
        ,[r].[ReturnForTraining(months)]
        ,[r].[ReturnForTrainingDate]
        ,[r].[RecordLocator]
        ,[r].[ReservationReferenceNumber]
        ,[r].[ReservationDateAdded]      
        ,[r].[ReservationDateStarted]    
        ,[r].[ReservationDateEnded]
        ,[r].[ReservationBookingUser]
        ,[r].[ReservationBookingDate]
        ,[r].[ReservationLastUpdated]
        ,[r].[CourseCompletedDate]
        ,[r].[ReservationMasterNumber]
        ,[r].[ERecordApprovedTimestamp]
        ,[r].[ERecordApprover]
        ,[r].[ERecordSignatureTimestamp]
        ,[r].[ERecordSignatureId]
        ,[r].[CertificateNo]
        ,[r].[CertificateHolder]
        ,[r].[CertificateHolderName]
        ,[r].[InsuranceStartDate]
        ,[r].[InsuranceExpireDate]
        ,[r].[InsuranceType]
        ,[r].[InsuranceValue]
        ,[r].[ApprovedCourseDescr]
        ,[r].[CourseCompletingUser]
        ,[r].[SchedulingDescr]
        ,[r].[RecordDescr]
        ,[r].[IsSinglePilot_]
        ,[r].[TrainingObjective]
        ,[r].[ActionToTakeIfSelected]
        ,[r].[AHCTR]
        ,[r].[AHRESV]
        ,[r].[AHSDT]
        ,[r].[AHCCDT]
        ,[r].[AHADDT]
        ,[r].[AHCLRF]
        ,[r].[AHID]
        ,[r].[AHPRID]
        ,[r].[AHCCST]
        ,[r].[AHCRS]
        ,[r].[AHMATL]
        ,[r].[RMMATL]
        ,[r].[AHMSTR]
        ,[r].[CUST_GROUP]
        ,[r].[RICDAT]
        ,[r].[RIRREV]
        ,[r].[ODRESV]
        ,[r].[ODTYPE]
        ,[r].[ODPOTY]
        ,[r].[DIVISION]
        ,[r].[PROGRAM]
        ,[r].[ClntOrder]
        ,[r].[ClntNotes]
        ,[r].[ClntDateOrder]
		,[r].[AircraftTailNo]
		,[r].[DeliveryMethod]
		,[r].[FromProgramId]
		,[r].[ROTElapsedDays]
		,[r].[ROTTargetDays]
		--,[r].[OnTimeFlag_new]
		,CASE 
            WHEN CHARINDEX('Test', [r].[ReopenReason]) > 0 THEN 'Y'
            WHEN [r].[OnTimeFlag_new] = 'Y' THEN 'Y'
            WHEN [r].[OnTimeFlag_new] = 'N' AND [lkpROP].[ImpactTimelinessFlag] IS NULL THEN 'N'
            WHEN [r].[OnTimeFlag_new] = 'N' AND [lkpROP].[ImpactTimelinessFlag] = 'Y'   THEN 'N'
            WHEN [r].[OnTimeFlag_new] = 'N' AND [lkpROP].[ImpactTimelinessFlag] = 'N'   THEN 'Y'		
        END AS [OnTimeFlag]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[r].[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------
        -------------------------------------------------------------------------------------------------------------
        -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - Begin
        -------------------------------------------------------------------------------------------------------------
		,[fsd].[FirstSIMDate]
        -------------------------------------------------------------------------------------------------------------
        -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - End
        -------------------------------------------------------------------------------------------------------------
	INTO 
        [#MergeSourceResv]
	FROM 
        [#MergeSourceResv2] AS [r]
	    LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[lkpReopenReason] AS [lkpROP]
		    ON [lkpROP].[ReopenReason] = [r].[ReopenReason]
        LEFT OUTER JOIN [#FirstSIMDate] AS [fsd]
		    ON [fsd].[AHID] = [r].[AHID];

    -- Adjust the temp table for incoming NULLS
    ALTER TABLE [#MergeSourceResv] ALTER COLUMN [HeaderId] numeric(12, 0) NULL;
    ALTER TABLE [#MergeSourceResv] ALTER COLUMN [AHRESV] char(7) NULL;
    ALTER TABLE [#MergeSourceResv] ALTER COLUMN [AHID] numeric(12, 0) NULL;

    ---------------------------------------------------------------------------------------------------------------------
    -- Technical Story 202303 - Textron - Report Ops data against Textron Centers - EDW Dev - Enhancement - 10/31/2019
    -- 274 rows in 7s
    ---------------------------------------------------------------------------------------------------------------------
    UPDATE 
        [r]
    SET 
        [CenterId] = [r].[TM_TABLE_DETAIL_ALTERNATE_KEY]
       ,[CompanyId] = [r].[CompanyIdNew]
    FROM (
        SELECT 
             [p].[ProgramKey]
            ,[i].[SIPRDH]
            ,SUBSTRING([x].[FXDATA], 1, 15) AS [SIM]
            ,[i].[SICTR]
            ,[tc].[TM_TABLE_DETAIL_KEY]
            ,[tc].[TM_TABLE_DETAIL_ALTERNATE_KEY]
            ,[r].[AHID]
            ,[r].[CenterId]
			,[r].[CompanyId]
			,[c].CompanyId as CompanyIdNew
        FROM 
            [#MergeSourceResv] AS [r]
            INNER JOIN [OperationsAnalytics].[dbo].[dimProgram] AS [p]
                ON [p].[Id] = [r].[ProgramId]
            INNER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRFX] AS [x]
                ON  [x].[FXTABL] = 'FPCRAH' 
                AND [x].[FXTKEY] = [r].[AHID]
                AND [x].[FXLABL] = 'RSHSIM'  
            INNER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXSI] AS [i]
                ON [i].[SISIM] = SUBSTRING([x].[FXDATA], 1, 15)
            INNER JOIN [#TextronCenters] AS [tc]
                ON [tc].[TM_TABLE_DETAIL_KEY] = CAST([r].[CenterId] AS varchar(4)) + LEFT([i].[SIPRDH], 5)
            INNER JOIN [OperationsAnalytics].[dbo].[dimCenter] AS [c]
			    ON [c].[Id] = [tc].[TM_TABLE_DETAIL_ALTERNATE_KEY]
        WHERE 
            SUBSTRING([p].[ProgramKey], 1, 5) = '00031'   
            AND [r].[HourlyFlag] = 'N'
            AND [r].[AHSDT] >= '2019-08-01') AS [r];

    ---------------------------------------------------------------------------------------------------
    -- Add Executive Rollup. 
    -- The Executive center would have been deleted above.
    -- (1160324 rows affected) in 59s
    ---------------------------------------------------------------------------------------------------
    INSERT [#MergeSourceResv] (
         [Year]
        ,[Month]
        ,[Day]
        ,[BusinessUnitId]
        ,[CenterId]
        ,[CompanyId]
        ,[DivisionId]
        ,[ProgramId]
        ,[POTypeId]
        ,[SalesOrderTypeId]
        ,[TrainingStatusId]
        ,[CustomerId]
        ,[CustomerGroupId]
        ,[ClientId]
        ,[CourseId]
        ,[ReservationMaterialId]
        ,[TrainingMaterialId]
        ,[CourseTypeId]
        ,[RevenueFlag]
        ,[InstructorFlag]
        ,[HourlyFlag]
        ,[ReOpenedFlag_Old]
        ,[OnTimeFlag_Old]
        ,[ReservationTotal]
        ,[HeaderId]
        ,[Group]
        ,[ReservationTypeId]
        ,[TrainingAuthorityId]
        ,[ReOpenedFlag]
        ,[OnTimeFlag]
        ,[ReOpenReason]
        ,[CenterId_Source]
        ,[ContractNumber]
    	,[SalesOrderNumber]
        ,[ContractTrainingRevenue]
        ,[SalesOrderTrainingRevenue]
        ,[TotalRevenue]
        ,[SimFlyTimeRevenue]
        ,[SimHrs]
        ,[FlyTimeHrs]
        ,[NonFlyTimeHrs]
        ,[RevenuePerSimHr(ByFlyTime)]
        ,[IsParentReservation_]
        ,[ReturnForTraining(months)]
        ,[ReturnForTrainingDate]
        ,[RecordLocator]
        ,[ReservationReferenceNumber]
        ,[ReservationDateAdded]      
        ,[ReservationDateStarted]    
        ,[ReservationDateEnded]
        ,[ReservationBookingUser]
        ,[ReservationBookingDate]
        ,[ReservationLastUpdated]
        ,[CourseCompletedDate]
        ,[ReservationMasterNumber]
        ,[ERecordApprovedTimestamp]
        ,[ERecordApprover]
        ,[ERecordSignatureTimestamp]
        ,[ERecordSignatureId]
        ,[CertificateNo]
        ,[CertificateHolder]
        ,[CertificateHolderName]
        ,[InsuranceStartDate]
        ,[InsuranceExpireDate]
        ,[InsuranceType]
        ,[InsuranceValue]
        ,[ApprovedCourseDescr]
        ,[CourseCompletingUser]
        ,[SchedulingDescr]
        ,[RecordDescr]
        ,[IsSinglePilot_]
        ,[TrainingObjective]
        ,[ActionToTakeIfSelected]
        ,[AHCTR]
        --,[AHRESV]
        --,[AHSDT]
        --,[AHCCDT]
        --,[AHADDT]
        ,[AHCLRF]
        --,[AHID]
        ,[AHPRID]
        ,[AHCCST]
        ,[AHCRS]
        ,[AHMATL]
        ,[RMMATL]
        ,[AHMSTR]
        ,[CUST_GROUP]
        ,[RICDAT]
        ,[RIRREV]
        ,[ODRESV]
        ,[ODTYPE]
        ,[ODPOTY]
        ,[DIVISION]
        ,[PROGRAM]
        ,[ClntOrder]
        ,[ClntNotes]
        ,[ClntDateOrder]
        --------------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
        ---------------------------------------------------------------------------------------
    	,[AircraftTailNo]	
        --------------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
        ---------------------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        ,[rdm].[DeliveryMethod]
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Requirement 267451 - Add column [FromProgramId] - Begin
        ---------------------------------------------------------------------------
        ,[FromProgramId]
        ---------------------------------------------------------------------------
        -- Technical Requirement 267451 - Add column [FromProgramId] - End
        ---------------------------------------------------------------------------
		,[ROTElapsedDays]
		,[ROTTargetDays]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------
		
    )
    SELECT
         [f].[Year]
        ,[f].[Month]
        ,[f].[Day]
        ,[f].[BusinessUnitId]
        ,0
        ,NULL
        ,[f].[DivisionId]
        ,[f].[ProgramId]
        ,[f].[POTypeId]
        ,[f].[SalesOrderTypeId]
        ,[f].[TrainingStatusId]
        ,[f].[CustomerId]
        ,[f].[CustomerGroupId]
        ,[f].[ClientId]
        ,[f].[CourseId]
        ,[f].[ReservationMaterialId]
        ,[f].[TrainingMaterialId]
        ,[f].[CourseTypeId]
        ,[f].[RevenueFlag]
        ,[f].[InstructorFlag]
        ,[f].[HourlyFlag]
        ,[f].[ReOpenedFlag_Old]
        ,[f].[OnTimeFlag_Old]
        ,[f].[ReservationTotal]
        ,[f].[HeaderId]
        ,[f].[Group]
        ,[f].[ReservationTypeId]
        ,[f].[TrainingAuthorityId]
        ,[f].[ReOpenedFlag]
        ,[f].[OnTimeFlag]
        ,[f].[ReOpenReason]
        ,[f].[CenterId_Source]
        ,[f].[ContractNumber]
        ,[f].[SalesOrderNumber] 
        ,[f].[ContractTrainingRevenue]
        ,[f].[SalesOrderTrainingRevenue]
        ,[f].[TotalRevenue]
        ,[f].[SimFlyTimeRevenue]
        ,[f].[SimHrs]
        ,[f].[FlyTimeHrs]
        ,[f].[NonFlyTimeHrs]
        ,[f].[RevenuePerSimHr(ByFlyTime)]
        ,[f].[IsParentReservation_]
        ,[f].[ReturnForTraining(months)]
        ,[f].[ReturnForTrainingDate]
        ,[f].[RecordLocator]
        ,[f].[ReservationReferenceNumber]
        ,[f].[ReservationDateAdded]
        ,[f].[ReservationDateStarted]
        ,[f].[ReservationDateEnded]
        ,[f].[ReservationBookingUser]
        ,[f].[ReservationBookingDate]
        ,[f].[ReservationLastUpdated]
        ,[f].[CourseCompletedDate]
        ,[f].[ReservationMasterNumber]
        ,[f].[ERecordApprovedTimestamp]
        ,[f].[ERecordApprover]
        ,[f].[ERecordSignatureTimestamp]
        ,[f].[ERecordSignatureId]
        ,[f].[CertificateNo]
        ,[f].[CertificateHolder]
        ,[f].[CertificateHolderName]
        ,[f].[InsuranceStartDate]
        ,[f].[InsuranceExpireDate]
        ,[f].[InsuranceType]
        ,[f].[InsuranceValue]
        ,[f].[ApprovedCourseDescr]
        ,[f].[CourseCompletingUser]
        ,[f].[SchedulingDescr]
        ,[f].[RecordDescr]
        ,[f].[IsSinglePilot_]
        ,[f].[TrainingObjective]
        ,[f].[ActionToTakeIfSelected]
        ,[f].[AHCTR]
        --,[f].[AHRESV]
        --,[f].[AHSDT]
        --,[f].[AHCCDT]
        --,[f].[AHADDT]
        ,[f].[AHCLRF]
        --,[f].[AHID]
        ,[f].[AHPRID]
        ,[f].[AHCCST]
        ,[f].[AHCRS]
        ,[f].[AHMATL]
        ,[f].[RMMATL]
        ,[f].[AHMSTR]
        ,[f].[CUST_GROUP]
        ,[f].[RICDAT]
        ,[f].[RIRREV]
        ,[f].[ODRESV]
        ,[f].[ODTYPE]
        ,[f].[ODPOTY]
        ,[f].[DIVISION]
        ,[f].[PROGRAM]
        ,[f].[ClntOrder]
        ,[f].[ClntNotes]
        ,[f].[ClntDateOrder]
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
        -----------------------------------------------------------------------------------
    	,[f].[AircraftTailNo]	
        -----------------------------------------------------------------------------------
        -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
        -----------------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
        ---------------------------------------------------------------------------
        ,[f].[DeliveryMethod]
        ---------------------------------------------------------------------------
        -- Technical Story 272755 - Add column [DeliveryMethod] - End
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        -- Technical Requirement 267451 - Add column [FromProgramId] - Begin
        ---------------------------------------------------------------------------
        ,[f].[FromProgramId]
        ---------------------------------------------------------------------------
        -- Technical Requirement 267451 - Add column [FromProgramId] - End
        ---------------------------------------------------------------------------
		,[f].[ROTElapsedDays]
		,[f].[ROTTargetDays]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - Begin
        ---------------------------------------------------------------------------
		,[f].[AHTCID]
        ---------------------------------------------------------------------------
        -- FSM 61.58 Effort - Add column [AHTCID] - End
        ---------------------------------------------------------------------------
    FROM 
        [#MergeSourceResv] AS [f]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenter] AS [d]
            ON [f].[CenterId] = [d].[Id] 
    WHERE 
        [d].[CenterCategoryID] IN (1, 4);

    ---------------------------------------------------------------------------------------------------
    -- Technical Story 246522: Override Delivery Center 1590 to 3090 - Begin
    -- (22169 rows affected) in 5s
    ---------------------------------------------------------------------------------------------------
    UPDATE 
        [resv]
    SET 
        [resv].[CenterId] = [tc].[TM_TABLE_DETAIL_ALTERNATE_KEY]
       ,[resv].[CompanyId] = [c].[CompanyId] 
    FROM 
        [#MergeSourceResv] AS [resv]
        LEFT OUTER JOIN [OperationsAnalytics].[dbo].[dimProgram] AS [p]
            ON [resv].[ProgramId] = [p].[Id]
        INNER JOIN [#TextronCenters] AS [tc]
            ON CAST([resv].[CenterId] AS char(4)) + SUBSTRING([p].[PROGRAMKEY], 1, 5) = [tc].[TM_TABLE_DETAIL_KEY]
        INNER JOIN [OperationsAnalytics].[dbo].[dimCenter] AS [c]
		    ON [c].[Id] = [tc].[TM_TABLE_DETAIL_ALTERNATE_KEY]
    WHERE 
        [tc].[TM_TABLE_DETAIL_KEY] LIKE '1590%'
        AND [resv].[CenterId] != [tc].[TM_TABLE_DETAIL_ALTERNATE_KEY];
    ---------------------------------------------------------------------------------------------------
    -- Technical Story 246522: Override Delivery Center 1590 to 3090 - End
    ---------------------------------------------------------------------------------------------------

    ---------------------------------------------------------------------------------------------------
    -- Update ContractNumber and SalesOrderNumber
    -- (1825771 rows affected) in 36s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [r]
    SET
         [ContractNumber]   = [x].[ODRCNT]
    	,[SalesOrderNumber] = [x].[ODORDR]
    FROM 
        [#MergeSourceResv] AS [r]
        -- Join to get [AHCTR]
        INNER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAH] AS [ah]
            ON [r].[HeaderId] = [ah].[AHID]
        INNER JOIN [#OrderSummary] AS [x]
            ON [r].[AHCTR] + [r].[ReservationReferenceNumber] = [x].[ODRESV];

    ---------------------------------------------------------------------------------------------------
    -- Get SalesOrderTrainingRevenue from [factRevenue_BOBJ]
    -- (876813 rows affected) in 3:00s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#TrainingRevenue];
    SELECT 
         [SalesDocument]
        ,SUM([TrainingRevenue]) AS [TrainingRevenue]
    INTO 
        [#TrainingRevenue]
    FROM 
        [OperationsAnalytics].[dbo].[factRevenue_BOBJ]
    GROUP BY 
        [SalesDocument];

    ---------------------------------------------------------------------------------------------------
    -- Update SalesOrderTrainingRevenue
    -- (1144904 rows affected) in 5s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
        [SalesOrderTrainingRevenue] = [r].[TrainingRevenue]
    FROM
        [#MergeSourceResv] AS [rs]
        INNER JOIN [#TrainingRevenue] AS [r]
    	    ON [rs].[SalesOrderNumber] = [r].[SalesDocument];

    ---------------------------------------------------------------------------------------------------
    -- Update ContractTrainingRevenue
    -- (374504 rows affected) in 17s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
        [ContractTrainingRevenue] = [r].[TrainingRevenue]
    FROM
        [#MergeSourceResv] AS [rs]
        INNER JOIN [#TrainingRevenue] AS [r]
    	    ON [rs].[ContractNumber] = [r].[SalesDocument];

    ---------------------------------------------------------------------------------------------------
    -- (1337841 rows affected) in 25:36s
    -- Note: The function [fn_db2tosqldatetime] is killing performance here
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
         [FlyTimeHrs]    = [x].[PF]
    	,[NonFlyTimeHrs] = [x].[NonFlyingSimTimeHrs]
    	,[SimHrs]        = [x].[TtlSimHrs]
    FROM
        [#MergeSourceResv] AS [rs]
        INNER JOIN (
    	    SELECT
                 [z].[AHID]
                ,SUM([z].[PF]) AS [PF]
                ,SUM([z].[NonFlyingSimTime]) AS [NonFlyingSimTimeHrs]
                ,SUM([z].[TtlSimHrs]) AS [TtlSimHrs]
    	    FROM (
    	        SELECT
    	    	     [ah].[AHID]
    	            ,[ah].[AHCTR] + [ah].[AHRESV] AS [ctr_resv]
    	            ,[co].[COPRDH] AS [phid]
    	            ,[ad].[ADBSIM] AS [sim]
    	            ,SUM(
    	    	        (DATEDIFF(MINUTE
                            -- Replaced the use of [fn_db2tosqldatetime] for performance enhancement
                            ,TRY_CONVERT(datetime,
                            STUFF(STUFF(IIF([ad].[ADSDAT] < 1000101, '19', '20') + RIGHT([ad].[ADSDAT], 6), 5, 0, '-'), 8, 0, '-') + ' ' +
                            STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST([ad].[ADSTIM] AS varchar) + '.000', 10), 3, 0, ':'), 6, 0, ':'))

                            -- Replaced the use of [fn_db2tosqldatetime] for performance enhancement
                            ,TRY_CONVERT(datetime,
                            STUFF(STUFF(IIF([ad].[ADEDAT] < 1000101, '19', '20') + RIGHT([ad].[ADEDAT], 6), 5, 0, '-'), 8, 0, '-') + ' ' +
                            STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST([ad].[ADETIM] AS varchar) + '.000', 10), 3, 0, ':'), 6, 0, ':'))
    	    			) 
    	    	    )) / 60.0 AS [TtlSimHrs]
                    ,SUM(([ad].[ADDURP]) / 60.0) AS [PF]
                    ,SUM([ad].[ADDURC]) / 60.0 AS [NonFlyingSimTime]
    	        FROM 
                    [OperationsAnalyticsStage].[dbo].[Stage_FPCRAH] AS [ah]
                    -- reason for next bit of code is to get PF time since it is not in factReservation
    	            LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPCRAD] AS [ad]
    	                ON [ah].[AHID] = [ad].[ADAHID]
    	    	    INNER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXSI] AS [d]
    	                ON [ad].[ADBSIM] = [d].[SISIM]
    	            LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXAT] AS [XAT1]
    	    	        ON  [ad].[ADMODT] = [XAT1].[ATMODT] 
                        AND [ad].[ADACTV] = [XAT1].[ATACTV]
    	            LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXAT] AS [XAT2]
    	    	        ON  [ad].[ADMODT] = [XAT2].[ATMODT] 
                        AND [XAT2].[ATACTV] = '*'
    	            LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_FPPXCO] AS [co]
    	    	        ON [ah].[AHMATL] = [co].[COLKUP]
    	        WHERE
                    [d].[SITYPE] = 'SIM'
    	    	    AND ISNULL([XAT1].[ATTRTP], [XAT2].[ATTRTP]) = 'S'
    	    	    AND CAST(LEFT([ah].[AHSDT], 3) AS int) >= 112
    	    	    --AND [ah].[AHCCST] IN ('F','C')
    	        GROUP BY
                     [ah].AHID
                    ,[ad].[ADBSIM]
                    ,[co].[COPRDH]
                    ,[ah].[AHCTR] + [ah].[AHRESV]) AS [z]
    	    GROUP BY 
                [z].[AHID]) AS [x]
    	    ON [rs].[HeaderId] = [x].[AHID];
    
    ---------------------------------------------------------------------------------------------------
    -- Get Contract Revenue
    -- (102170 rows affected) in 1s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#ContractRevenue];
    SELECT
         [y].[ContractNumber]
        ,(COALESCE([y].[ContractTrainingRevenue], 0) + COALESCE([y].[SalesOrderTrainingRevenue], 0)) AS [TotalRevenue]
    INTO
        [#ContractRevenue]
    FROM (
    	SELECT
             [x].[ContractNumber]
    		,[x].[ContractTrainingRevenue] 
    		,SUM([x].[SalesOrderTrainingRevenue]) AS [SalesOrderTrainingRevenue]
    	FROM (
            SELECT DISTINCT  
                 [rs].[ContractNumber]
                ,[rs].[SalesOrderNumber]
                ,[rs].[ContractTrainingRevenue]
                ,[rs].[SalesOrderTrainingRevenue]
    		FROM  
                [#MergeSourceResv] AS [rs]
    		WHERE 
                1= 1		
    			AND [rs].[ContractNumber] <> ''
    			AND [rs].[CenterId] <> 0) AS [x]
    	GROUP BY
             [x].[ContractNumber]
    		,[x].[ContractTrainingRevenue]) AS [y]
    GROUP BY
         [y].[ContractNumber]
        ,[y].[ContractTrainingRevenue] 
        ,[y].[SalesOrderTrainingRevenue];
    
    ---------------------------------------------------------------------------------------------------
    -- Update Total Revenue For All Reservations
    -- (570050 rows affected) in 40s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
        [TotalRevenue] = [c].[TotalRevenue]
    FROM
        [#MergeSourceResv] AS [rs]
        INNER JOIN [#ContractRevenue] AS [c]
    	    ON [rs].[ContractNumber] = [c].[ContractNumber];
    
    ---------------------------------------------------------------------------------------------------
    -- 
    -- (1255046 rows affected) in 5s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
        [TotalRevenue] = [rs].[SalesOrderTrainingRevenue]
    FROM
        [#MergeSourceResv] AS [rs]
    WHERE
        [ContractNumber] = '';
    
    ---------------------------------------------------------------------------------------------------
    -- Create temp table of Contract Sim Hours
    -- (102170 rows affected) in 11s
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#ContractSimHours];
    SELECT
         [ContractNumber]
        ,SUM([SimHrs]) AS [TotalSimHrs]
        ,SUM([FlyTimeHrs]) AS [TotalFlyTimeHrs]
    INTO
        [#ContractSimHours]
    FROM  
        [#MergeSourceResv]
    WHERE
        [ContractNumber] <> '' 
        AND [CenterId] <> 0
    GROUP BY
         [ContractNumber]
        ,[TotalRevenue];
    
    CREATE CLUSTERED INDEX [CI_contractnumber] ON [#ContractSimHours]([ContractNumber]);
    
    ---------------------------------------------------------------------------------------------------
    -- Update Revenue Per Sim Hour
    -- (570050 rows affected) in 28s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
        [RevenuePerSimHr(ByFlyTime)] = CASE WHEN [c].[TotalFlyTimeHrs] = 0 THEN 0 ELSE [rs].[TotalRevenue] / [c].[TotalFlyTimeHrs] END	
    FROM
        [#MergeSourceResv] AS [rs]
        INNER JOIN [#ContractSimHours] AS [c]
    	    ON [rs].[ContractNumber] = [c].[ContractNumber];

    ---------------------------------------------------------------------------------------------------
    -- 
    -- (1255721 rows affected) in 6s
    ---------------------------------------------------------------------------------------------------
    UPDATE 
        [rs]
    SET 
        [RevenuePerSimHr(ByFlyTime)] = CASE WHEN [rs].[FlyTimeHrs] = 0 THEN 0 ELSE [rs].[TotalRevenue] / [rs].[FlyTimeHrs] END
    FROM 
        [#MergeSourceResv] AS [rs]
    WHERE 
        [rs].[ContractNumber] = '';
    
    ---------------------------------------------------------------------------------------------------
    -- Update SimFlyTimeRevenue
    -- (2426248 rows affected) in 12s
    ---------------------------------------------------------------------------------------------------
    UPDATE
        [rs]
    SET
    	[SimFlyTimeRevenue] = [RevenuePerSimHr(ByFlyTime)] * [FlyTimeHrs]
    FROM
        [#MergeSourceResv] AS [rs];
    
    ---------------------------------------------------------------------------------------------------
    -- Update ReturnForTraining columns
    -- (2426248 rows affected) in 1:39s
    ---------------------------------------------------------------------------------------------------
    WITH [ReturnForTraining] AS (
    SELECT 
         [cr].[RCAHID]
        ,MIN([e].[ExpiryMonths]) AS [ReturnForTraining(months)]
    FROM
        [OperationsAnalyticsStage].[dbo].[Stage_FPCRRC] AS [cr]
        LEFT OUTER JOIN [OperationsAnalyticsStage].[dbo].[Stage_CheckrideItemExpiration] AS [e]
            ON [cr].[RCNAME] = [e].[ItemName]
    GROUP BY
         [cr].[RCAHID]
    )
    UPDATE 
        [f]
    SET
         [ReturnForTraining(months)] = [r].[ReturnForTraining(months)]
        ,[ReturnForTrainingDate] = DATEADD(MONTH, [r].[ReturnForTraining(months)], [f].[ReservationDateEnded])
    FROM 
        [#MergeSourceResv] AS [f]
        LEFT OUTER JOIN [ReturnForTraining] AS [r]
            ON [f].[HeaderId] = [r].[RCAHID];

    -----------------------------------------------------------------------------------------------------
    -----------------------------------------------------------------------------------------------------
    -- Truncate and reload [factReservation]
    -----------------------------------------------------------------------------------------------------
    -----------------------------------------------------------------------------------------------------
    DECLARE @SummaryOfChanges TABLE([Change] varchar(20));  
    DECLARE @rc_pre int;
    DECLARE @rc_poe int;
    DECLARE @IsTransSuccess bit = 0;
    
    -- Get initial row count of the fact table
    SELECT @rc_pre = COUNT(*) FROM [OperationsAnalytics].[dbo].[factReservation] WITH (NOLOCK);
    
    IF (SELECT COUNT(*) FROM [#MergeSourceResv]) >= 1000000
    BEGIN -- Begin if statement block
        BEGIN TRY
    
            BEGIN TRANSACTION;
        
            TRUNCATE TABLE [OperationsAnalytics].[dbo].[factReservation];
    
            INSERT INTO [OperationsAnalytics].[dbo].[factReservation] (
    		     [Year]
    		    ,[Month]
    		    ,[Day]
    		    ,[BusinessUnitId]
    		    ,[CenterId]
    		    ,[CompanyId]
    		    ,[DivisionId]
    		    ,[ProgramId]
    		    ,[POTypeId]
    		    ,[SalesOrderTypeId]
    		    ,[TrainingStatusId]
    		    ,[CustomerId]
    		    ,[CustomerGroupId]
    		    ,[ClientId]
    		    ,[CourseId]
    		    ,[ReservationMaterialId]
    		    ,[TrainingMaterialId]
    		    ,[CourseTypeId]
    		    ,[RevenueFlag]
    		    ,[InstructorFlag]
    		    ,[HourlyFlag]
    		    ,[ReOpenedFlag_Old]
    		    ,[OnTimeFlag_Old]
    		    ,[ReservationTotal]
    		    ,[HeaderId]
    		    ,[Group]
    		    ,[ReservationTypeId]
    		    ,[TrainingAuthorityId]
    		    ,[ReOpenedFlag]
    		    ,[OnTimeFlag]
    		    ,[ReOpenReason]
                ,[CenterId_Source]
                ,[ContractNumber]
                ,[SalesOrderNumber]
                ,[ContractTrainingRevenue]
                ,[SalesOrderTrainingRevenue]
                ,[TotalRevenue]
                ,[SimFlyTimeRevenue]
                ,[SimHrs]
                ,[FlyTimeHrs]
                ,[NonFlyTimeHrs]
                ,[RevenuePerSimHr(ByFlyTime)]
    		    ,[IsParentReservation_]
                ,[ReturnForTraining(months)]
                ,[ReturnForTrainingDate]
    		    ,[RecordLocator]
    		    ,[ReservationReferenceNumber]
    		    ,[ReservationDateAdded]
    		    ,[ReservationDateStarted]
    		    ,[ReservationDateEnded]
    		    ,[ReservationBookingUser]
    		    ,[ReservationBookingDate]
    		    ,[ReservationLastUpdated]
    		    ,[CourseCompletedDate]
    		    ,[ReservationMasterNumber]
    		    ,[ERecordApprovedTimestamp]
    		    ,[ERecordApprover]
    		    ,[ERecordSignatureTimestamp]
    		    ,[ERecordSignatureId]
    		    ,[CertificateNo]
    		    ,[CertificateHolder]
    		    ,[CertificateHolderName]
    		    ,[InsuranceStartDate]
    		    ,[InsuranceExpireDate]
    		    ,[InsuranceType]
    		    ,[InsuranceValue]
    		    ,[ApprovedCourseDescr]
    		    ,[CourseCompletingUser]
    		    ,[SchedulingDescr]
    		    ,[RecordDescr]
    		    ,[IsSinglePilot_]
    		    ,[TrainingObjective]
    		    ,[ActionToTakeIfSelected]
    		    ,[AHCTR]
    		    ,[AHRESV]
    	        ,[AHSDT] 
    	        ,[AHCCDT] 
    	        ,[AHADDT] 
    		    ,[AHCLRF]
    		    ,[AHID]
    		    ,[AHPRID]
    		    ,[AHCCST]
    		    ,[AHCRS]
    		    ,[AHMATL]
    		    ,[RMMATL]
    		    ,[AHMSTR]
    		    ,[CUST_GROUP]
    		    ,[RICDAT]
    		    ,[RIRREV]
    		    ,[ODRESV]
    		    ,[ODTYPE]
    		    ,[ODPOTY]
    		    ,[DIVISION]
    		    ,[PROGRAM]
    		    ,[ClntOrder]
    	        ,[ClntNotes]
    	        ,[ClntDateOrder]
                -----------------------------------------------------------------------------------
                -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
                -----------------------------------------------------------------------------------
          	    ,[AircraftTailNo]	
                -----------------------------------------------------------------------------------
                -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
                -----------------------------------------------------------------------------------
                ---------------------------------------------------------------------------
                -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
                ---------------------------------------------------------------------------
                ,[DeliveryMethod]
                ---------------------------------------------------------------------------
                -- Technical Story 272755 - Add column [DeliveryMethod] - End
                ---------------------------------------------------------------------------
    			---------------------------------------------------------------------------
    	        -- Technical Requirement 267451 - Add column [FromProgramId] - Begin
    	        ---------------------------------------------------------------------------
    	        ,[FromProgramId]
    	        ---------------------------------------------------------------------------
    	        -- Technical Requirement 267451 - Add column [FromProgramId] - End
    	        ---------------------------------------------------------------------------
				,[ROTElapsedDays]
				,[ROTTargetDays]
                ---------------------------------------------------------------------------
                -- FSM 61.58 Effort - Add column [AHTCID] - Begin
                ---------------------------------------------------------------------------
		        ,[TrainingClassId]
                ---------------------------------------------------------------------------
                -- FSM 61.58 Effort - Add column [AHTCID] - End
                ---------------------------------------------------------------------------
                -------------------------------------------------------------------------------------------------------------
                -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - Begin
                -------------------------------------------------------------------------------------------------------------
	    	    ,[FirstSIMDate]
                -------------------------------------------------------------------------------------------------------------
                -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - End
                -------------------------------------------------------------------------------------------------------------
        	)
            SELECT 
    		     [Year]
    		    ,[Month]
    		    ,[Day]
    		    ,[BusinessUnitId]
    		    ,[CenterId]
    		    ,[CompanyId]
    		    ,[DivisionId]
    		    ,[ProgramId]
    		    ,[POTypeId]
    		    ,[SalesOrderTypeId]
    		    ,[TrainingStatusId]
    		    ,[CustomerId]
    		    ,[CustomerGroupId]
    		    ,[ClientId]
    		    ,[CourseId]
    		    ,[ReservationMaterialId]
    		    ,[TrainingMaterialId]
    		    ,[CourseTypeId]
    		    ,[RevenueFlag]
    		    ,[InstructorFlag]
    		    ,[HourlyFlag]
    		    ,[ReOpenedFlag_Old]
    		    ,[OnTimeFlag_Old]
    		    ,[ReservationTotal]
    		    ,[HeaderId]
    		    ,[Group]
    		    ,[ReservationTypeId]
    		    ,[TrainingAuthorityId]
    		    ,[ReOpenedFlag]
    		    ,[OnTimeFlag]
    		    ,[ReOpenReason]
                ,[CenterId_Source]
                ,[ContractNumber]
                ,[SalesOrderNumber]
                ,[ContractTrainingRevenue]
                ,[SalesOrderTrainingRevenue]
                ,[TotalRevenue]
                ,[SimFlyTimeRevenue]
                ,[SimHrs]
                ,[FlyTimeHrs]
                ,[NonFlyTimeHrs]
                ,[RevenuePerSimHr(ByFlyTime)]
    		    ,[IsParentReservation_]
                ,[ReturnForTraining(months)]
                ,[ReturnForTrainingDate]
    		    ,[RecordLocator]
    		    ,[ReservationReferenceNumber]
    		    ,[ReservationDateAdded]
    		    ,[ReservationDateStarted]
    		    ,[ReservationDateEnded]
    		    ,[ReservationBookingUser]
    		    ,[ReservationBookingDate]
    		    ,[ReservationLastUpdated]
    		    ,[CourseCompletedDate]
    		    ,[ReservationMasterNumber]
    		    ,[ERecordApprovedTimestamp]
    		    ,[ERecordApprover]
    		    ,[ERecordSignatureTimestamp]
    		    ,[ERecordSignatureId]
    		    ,[CertificateNo]
    		    ,[CertificateHolder]
    		    ,[CertificateHolderName]
    		    ,[InsuranceStartDate]
    		    ,[InsuranceExpireDate]
    		    ,[InsuranceType]
    		    ,[InsuranceValue]
    		    ,[ApprovedCourseDescr]
    		    ,[CourseCompletingUser]
    		    ,[SchedulingDescr]
    		    ,[RecordDescr]
    		    ,[IsSinglePilot_]
    		    ,[TrainingObjective]
    		    ,[ActionToTakeIfSelected]
    		    ,[AHCTR]
    		    ,[AHRESV]
    	        ,[AHSDT] 
    	        ,[AHCCDT] 
    	        ,[AHADDT] 
    		    ,[AHCLRF]
    		    ,[AHID]
    		    ,[AHPRID]
    		    ,[AHCCST]
    		    ,[AHCRS]
    		    ,[AHMATL]
    		    ,[RMMATL]
    		    ,[AHMSTR]
    		    ,[CUST_GROUP]
    		    ,[RICDAT]
    		    ,[RIRREV]
    		    ,[ODRESV]
    		    ,[ODTYPE]
    		    ,[ODPOTY]
    		    ,[DIVISION]
    		    ,[PROGRAM]   
    		    ,[ClntOrder]
    	        ,[ClntNotes]
    		    ,[ClntDateOrder]
                --------------------------------------------------------------------------------------
                -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - Begin
                --------------------------------------------------------------------------------------
    	        ,[AircraftTailNo]
                --------------------------------------------------------------------------------------
                -- Technical Requirement 266891 - RESV-Enhancement_Add Aircraft Tail Number - End
                -------------------------------------------------------------------------------------- 
                ---------------------------------------------------------------------------
                -- Technical Story 272755 - Add column [DeliveryMethod] - Begin
                ---------------------------------------------------------------------------
                ,[DeliveryMethod]
                ---------------------------------------------------------------------------
                -- Technical Story 272755 - Add column [DeliveryMethod] - End
                ---------------------------------------------------------------------------
    			---------------------------------------------------------------------------
    	        -- Technical Requirement 267451 - Add column [FromProgramId] - Begin
    	        ---------------------------------------------------------------------------
    	        ,[FromProgramId]
    	        ---------------------------------------------------------------------------
    	        -- Technical Requirement 267451 - Add column [FromProgramId] - End
    	        ---------------------------------------------------------------------------
				,[ROTElapsedDays]
				,[ROTTargetDays]
                ---------------------------------------------------------------------------
                -- FSM 61.58 Effort - Add column [AHTCID] - Begin
                ---------------------------------------------------------------------------
		        ,[AHTCID]
                ---------------------------------------------------------------------------
                -- FSM 61.58 Effort - Add column [AHTCID] - End
                ---------------------------------------------------------------------------
                -------------------------------------------------------------------------------------------------------------
                -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - Begin
                -------------------------------------------------------------------------------------------------------------
	    	    ,[FirstSIMDate]
                -------------------------------------------------------------------------------------------------------------
                -- User Story 444415 - Add First Sim Session Date to factReservation and OPS-Reservation - ETR 1429 - End
                -------------------------------------------------------------------------------------------------------------
            FROM 
                [#MergeSourceResv];
    
            COMMIT TRANSACTION;
            SET @IsTransSuccess = 1;
    
        END TRY
    
        BEGIN CATCH
    
            -- List error information
            SELECT 
                 ERROR_NUMBER()     AS [ErrorNumber]
                ,ERROR_SEVERITY()   AS [ErrorSeverity]
                ,ERROR_STATE()      AS [ErrorState]
                ,ERROR_LINE()       AS [ErrorLine]
                ,ERROR_PROCEDURE()  AS [ErrorProcedure]
                ,ERROR_MESSAGE()    AS [ErrorMessage];
    
            -- Is transaction uncommittable?
            IF (XACT_STATE()) = -1
            BEGIN
                PRINT N'The transaction is in an uncommittable state.'
                PRINT N'Rolling back the transaction...'
                ROLLBACK TRANSACTION;
                THROW;
            END;
    
            -- Is transaction committable?
            IF (XACT_STATE()) = 1
            BEGIN
                PRINT N'The transaction is committable.'
                PRINT N'Committing the transaction...'
                COMMIT TRANSACTION;
                SET @IsTransSuccess = 1;
            END;
    
        END CATCH
    
    END -- End if statement block

    -----------------------------------------------------------------------------------------------
    -- Update audit log data post execution
    -----------------------------------------------------------------------------------------------
    -- Get row count after table is loaded
    SELECT @rc_poe = COUNT(*) FROM [OperationsAnalytics].[dbo].[factReservation] WITH (NOLOCK);
    
    DECLARE @c INT;
    DECLARE @sp VARCHAR(1000) = 'spLoadOPAfactReservation';
    DECLARE @tbl VARCHAR(1000) = '[OperationsAnalytics].[dbo].[factReservation]';

    -- Rows inserted
    SELECT @c = IIF(@IsTransSuccess = 1, ISNULL(@rc_poe, 0), 0);
    IF @c >= 0 BEGIN
        EXEC [OperationsAnalyticsStage].[dbo].[spLoadDataWarehouseChangeAuditLog_ins] @eid, @c, @sp, @tbl, @rc_pre, @rc_poe;
    END
    
    -- Rows updated
    SELECT @c = 0;
    IF @c >= 0 BEGIN
        EXEC [OperationsAnalyticsStage].[dbo].[spLoadDataWarehouseChangeAuditLog_upd] @eid, @c, @sp, @tbl, @rc_pre, @rc_poe;
    END
    
    -- Rows deleted
    SELECT @c = IIF(@IsTransSuccess = 1, ISNULL(@rc_pre, 0), 0);
    IF @c >= 0 BEGIN
        EXEC [OperationsAnalyticsStage].[dbo].[spLoadDataWarehouseChangeAuditLog_del] @eid, @c, @sp, @tbl, @rc_pre, @rc_poe;
    END

    ---------------------------------------------------------------------------------------------------
    -- Cleanup Temp Tables
    ---------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS [#OrderSummary];
    DROP TABLE IF EXISTS [#TBAInstructor];
    DROP TABLE IF EXISTS [#IpadAttributes];
    DROP TABLE IF EXISTS [#NonTBAInstructor];
    DROP TABLE IF EXISTS [#ReservationDetail];
    DROP TABLE IF EXISTS [#WeekEndCntByReservationHeader];
    DROP TABLE IF EXISTS [#ReservationSummary];
    DROP TABLE IF EXISTS [#TextronCenters];
    DROP TABLE IF EXISTS [#ResvHourly];
    DROP TABLE IF EXISTS [#ReservationHourly];
    DROP TABLE IF EXISTS [#TextronReservation1];
    DROP TABLE IF EXISTS [#TextronReservation];
    DROP TABLE IF EXISTS [#MergeSourceResv1];
	DROP TABLE IF EXISTS [#MergeSourceResv2];
	DROP TABLE IF EXISTS [#MergeSourceResv];
    DROP TABLE IF EXISTS [#TrainingRevenue];
    DROP TABLE IF EXISTS [#ContractRevenue];
    DROP TABLE IF EXISTS [#ContractSimHours];

END -- End stored procedure
GO
