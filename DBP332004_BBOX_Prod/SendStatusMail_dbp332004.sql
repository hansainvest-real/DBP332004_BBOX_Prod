USE [msdb]
GO

/****** Object:  Job [Send status mail]    Script Date: 07.05.2025 19:18:57 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 07.05.2025 19:18:57 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Send status mail', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Sendet eine Statusmail an TCS, wenn Objekte nicht vollständig gemappt sind.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'SYSTEM\U151521', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send status mail for KeyAllocations, Reports and Contract plausibilities]    Script Date: 07.05.2025 19:18:57 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send status mail for KeyAllocations, Reports and Contract plausibilities', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec [dbo].[BIS_MonitoringIntfBatch] @WaitForBatchEnd=''false'', @MailSend=''true''', 
		@database_name=N'BISDSYSDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send mail if unmapped objects]    Script Date: 07.05.2025 19:18:57 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send mail if unmapped objects', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @strMail varchar(max), @send bit=''true'';
/*======================================================================================================================================================*/
 
--   Statusbericht Objektmapping
/*======================================================================================================================================================*/
WITH[Config]AS(SELECT xHubID
                    , xUnitCompanyCode
                                                                               , xUnitAssetCode
                                                                               , xobjnr
                                                                               , fo.xFondsNr
                                                                               , g .xgesellschaftcode
                                                                               , xComment 
                                                                               , x0.xPrefix
                                                                               , TypeOfDelivery = (select xkeymc FROM xlbikeyval WHERE lid=xTypeOfDelivery)
               FROM dbo.xIntfConfiguration c
                                                  LEFT JOIN dbo.xlbiFonds fo ON c.xfondsnr = fo.xfondsnr AND fo.ldelete=0
                                                  LEFT JOIN dbo.xgesellschaft g ON c.xgesellschaftcode = g.xgesellschaftcode and g.ldelete=0
                                                  OUTER APPLY(select xkeyalphanr FROM xlbikeyval where lid=xPrefix)x0(xPrefix)
                                                  WHERE c.ldelete = 0 
                                                    AND(xobjnr IS NULL OR fo.xfondsnr IS NULL OR g.xgesellschaftcode IS NULL OR x0.xPrefix IS NULL) 
                                                                AND(xImportTenant = 1 OR xImportBalances = 1 OR xImportTenantBalances=1)
                                                               AND(CONVERT(CHAR(6),GETDATE(),112) BETWEEN xPeriodValidFrom AND xPeriodValidTo OR CONVERT(CHAR(6),GETDATE(),112) BETWEEN xDataPeriodValidFrom AND xDataPeriodValidTo)
                                                               --AND(xIsSubObject = 0 OR xIsSubObject IS NULL)
                                                               )
SELECT @strMail = CAST(
(SELECT ''<td style="'' + ISNULL(norm+''">'' +                      xHubID           ,red  +''">'')+ ''</td>''  -- Data Hub
      , ''<td style="'' + ISNULL(norm+''">'' +                      TypeOfDelivery   ,red  +''">'')+ ''</td>''  -- Type of Data Delivery
      , ''<td style="'' + ISNULL(norm+''">'' +                      xPrefix          ,red  +''">'')+ ''</td>''  -- Data Prefix
      + ''<td style="'' + ISNULL(norm+''">'' +                      xUnitCompanyCode ,norm +''">'')+ ''</td>''  -- Mandant / Gesellschaft (Quelle)
      + ''<td style="'' + ISNULL(norm+''">'' +                      xUnitAssetCode   ,red  +''">'')+ ''</td>''  -- Asset - Id             (Quelle)
      + ''<td style="'' + ISNULL(norm+''">'' +    ISNULL(O.xobMatchcode,T00.xComment),norm +''">'')+ ''</td>''  -- Description
                  + ''<td style="'' + ISNULL(norm+''">'' +                      xobjnr           ,red  +''">'')+ ''</td>''  -- associated to BISonBOX - property
      , ''<td style="'' + ISNULL(norm+''">'' +                      xgesellschaftcode,red  +''">'')+ ''</td>''  -- associatet to BISonBOX - Company
      + ''<td style="'' + ISNULL(norm+''">'' + convert(VARCHAR(256),xFondsNr)        ,red  +''">'')+ ''</td>''  -- associated to BISonBOX - funds
FROM(SELECT DISTINCT TOP(2000) *,R=ROW_NUMBER()OVER(ORDER BY xHubID,xPrefix,TypeOfDelivery,xUnitAssetCode,xUnitCompanyCode) FROM Config ORDER BY xHubID, xPrefix, TypeOfDelivery, xUnitCompanyCode, xUnitAssetCode) T00
LEFT JOIN BISINTDATA.dbo.xinpObjekte O ON O.xObjekt = T00.xUnitAssetCode
CROSS APPLY(SELECT ''background:red;'',CASE R%2 WHEN 1 THEN''''ELSE''background:beige;''END)color(red,norm)
CROSS APPLY(SELECT ''text-align:right;'',''text-align:left;'',''text-align:center;''       )align(re,li,mi)
FOR XML PATH(''tr''), TYPE) AS VARCHAR(MAX))
IF @strmail IS NULL SET @send=''false'';
/*======================================================================================================================================================*/
 
--  Nachrichtentext zusammensetzen (HTML)
/*======================================================================================================================================================*/
set @strMail = 
 ''<html>''
+''<head>''                                         
+''</head>''
+''<body>''
+''<h2 style="font-family:Arial;">some asset configuration entries require your intervention on Server '' + @@SERVERNAME + ''.</h2>''
+''<h3 style="font-family:Arial;"> Date and Time: '' + convert(varchar(16),ISNULL(getdate(),''''),121) + ''</h3>''
+''<p style="font-family:Arial;">''
+''</p>''
--------------------  Tabelle für schnittstellenverlauf  -----------------------------------------------------------------------------------------------
+''<table cellpadding="1" cellspacing="1" border="1">''
+''<font Color=Black Face=Arial Size=2>''
+''<thead><tr><th colspan="9" style="background-color:#169ade;text-align:left"><h4>unmapped objects</h4></th></tr>''                                            
+       ''<tr><th colspan="3" style="background-color:#169ade;text-align:left">origin</th><th colspan="3" style="background-color:#169ade;text-align:left">asset</th><th colspan="3" style="background-color:#169ade;text-align:left">associated to</th></tr>''
+       ''<tr style="vertical-align:top;background-color:#25afe7"><th>datahub</th><th>datatype</th><th>prefix</th><th>company</th><th>Id</th><th>description</th><th>property</th><th>company</th><th>funds</th></tr>''
+''</thead>''
+ ISNULL(REPLACE(REPLACE(REPLACE(@strMail, ''.'', '','' ),''&lt;'',''<'' ),''&gt;'',''>'' ),'''')
+''</font>''
+''</table>''
----------------------------------------------------------------------------------------------------------------------------------------------------------
+''</body>''
+''</html>''
/*======================================================================================================================================================*/
 
-- Mailversand
/*======================================================================================================================================================*/
DECLARE @MailProfil      varchar (50) =         (SELECT pValue FROM BISDSYSDB.dbo.vInterfaceParameters WHERE Interface=''zJOBPARA'' AND pName=''MailProfil'')
      , @recipients      varchar(256) =         (SELECT pValue FROM BISDSYSDB.dbo.vInterfaceParameters WHERE Interface=''zJOBPARA'' AND pName=''recipients'')
      , @copy_recipients varchar(256) =         (SELECT pValue FROM BISDSYSDB.dbo.vInterfaceParameters WHERE Interface=''zJOBPARA'' AND pName=''copy_recipients'')
      , @subject         varchar(256) = REPLACE((SELECT pValue FROM BISDSYSDB.dbo.vInterfaceParameters WHERE Interface=''zJOBPARA'' AND pName=''subject''),''"'','''') 
 
--SELECT @subject         = ''State of mapped objects''
SELECT @subject = @subject
 
IF EXISTS(SELECT NULL FROM msdb.dbo.sysmail_profile WHERE name = @MailProfil) AND @send=''true'' --  Nachricht wird nur gesendet, wenn Mailprofil vorhanden ist 
EXEC msdb.dbo.sp_send_dbmail
            @profile_name    = @MailProfil,
                        @recipients      = @recipients,
                        @copy_recipients = @copy_recipients,
                                               @importance      = ''normal'',
            @body_format     = ''HTML'',
            @body            = @strMail,
            @subject         = @subject
/*======================================================================================================================================================*/', 
		@database_name=N'BISIBOBDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily at 6 am', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20220113, 
		@active_end_date=99991231, 
		@active_start_time=80000, 
		@active_end_time=235959, 
		@schedule_uid=N'c01b2dd5-ad46-47a8-a14d-e9eac7a1c717'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


