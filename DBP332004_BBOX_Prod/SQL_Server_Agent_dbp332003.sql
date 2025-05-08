USE [msdb]
GO

/****** Object:  Job [BIS_ETL_startInterface_SAT]    Script Date: 08.05.2025 14:52:36 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 08.05.2025 14:52:36 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'BIS_ETL_startInterface_SAT', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Es ist keine Beschreibung verfügbar.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'SYSTEM\U151521', 
		@notify_email_operator_name=N'M.Bittorf', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Clear SSIS - logging when older than one week]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Clear SSIS - logging when older than one week', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'delete from sysssislog where DATEDIFF(WEEK,starttime,getdate()) > 1', 
		@database_name=N'BISDSYSDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS-Package: Start HIRA SAP load for Import DB in box-schema]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS-Package: Start HIRA SAP load for Import DB in box-schema', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\HIRA\SAP_IMPORT\BISON.BOX_test.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0, 
		@proxy_name=N'BIS_Proxy'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [IndexOptimize - BISimportDB]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'IndexOptimize - BISimportDB', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'CmdExec', 
		@command=N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d BISDSYSDB -Q "EXECUTE [dbo].[IndexOptimize] @Databases = ''BISimportDB'', @FragmentationLow = NULL, @FragmentationMedium = ''INDEX_REORGANIZE,INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE'', @FragmentationHigh = ''INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE'', @FragmentationLevel1 = 5, @FragmentationLevel2 = 30, @UpdateStatistics = ''ALL'', @OnlyModifiedStatistics = ''Y'', @LogToTable = ''Y''" -b', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS package: SAP_Fill_BISimportDB]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS package: SAP_Fill_BISimportDB', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\SAP\SAP_Fill_BISimportDB.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 1 /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Custom updates in BISimportDB]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Custom updates in BISimportDB', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'delete
from dbo.SAP_vibdbe
where BISDSYSDB.dbo.BIS_Convert_DATE(nullif(Datumabgang,''99991231''), null) < ''20180101''


update BISimportDB.dbo.SAP_BSEG_ALL 
set PRCTR = right(PRCTR, 8)
where len (PRCTR) = 10

update BISimportDB.dbo.SAP_BSEG
set PRCTR = right(PRCTR, 8)
where len (PRCTR) = 10

update BISimportDB.dbo.SAP_CEPCT
set PRCTR = right(PRCTR, 8)
where len (PRCTR) = 10

update BISimportDB.dbo.SAP_BUT020 
set DATE_FROM = ''1900-01-01''
where DATE_FROM in (''0001-01-01'', ''1101-01-00'')

update BISimportDB.dbo.SAP_BUT020 
set ADDR_VALID_FROM = ''19000101000000''
where ADDR_VALID_FROM in (''10101000000'', ''110101000000'')

update BISimportDB.dbo.SAP_vicncn 
set RECNENDABS = null
where RECNENDABS = ''2099-12-31''

update BISimportDB.dbo.SAP_vicncn 
set RECNEND1ST = null
where RECNEND1ST = ''2099-12-31''

update BISimportDB.dbo.SAP_VIBDOBJASS
set VALIDTO = null
where VALIDTO = ''2099-12-31''

update BISimportDB.dbo.SAP_vicncn 
set RECNEND1ST = null
where RECNEND1ST = ''2099-12-31''

update dbo.SAP_VIBDRO 
set VALIDTO = null
where VALIDTO = ''2099-12-31''

update dbo.SAP_vibdbe
set Datumabgang = null 
where Datumabgang = ''2099-12-31''

-- dynamische Anpassung Startdatum Zgif
;
update cpp
set [value] = convert(varchar(6), dateadd(m,-4,getdate()),112)
--select * 
from BISIBOBDB..xBGPcommonParams cp
inner join BISIBOBDB..xBGPcommonParamsParams cpp on cp.lid = cpp.CommonParamsID
where 1 = 1
and cp.mc = ''HIRA_Zgif''
and cpp.mc = ''begperiod''
and cp.Active = 1
and cpp.Active = 1
', 
		@database_name=N'BISimportDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS package: SAP_RE]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS package: SAP_RE', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\SAP\SAP_RE.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 1 /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: BoBexchg]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: BoBexchg', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=9, 
		@on_fail_action=4, 
		@on_fail_step_id=9, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\bisonbox\BoBexchg_WebClient.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 4 /Par MyProxyPassword;"\"OFUK!U2r3ly?F00\"" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@output_file_name=N'C:\Temp\Log', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: BOBMM]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: BOBMM', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"\\dbp332003\BISSQL\SSIS\bisonbox\BOBMM.dtsx\"" /CHECKPOINTING OFF /SET "\"\Package.Variables[User::parServerName_SDB].Properties[Value]\"";"\"DBP332003\SQL_BISON_TEST\"" /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Reset Lock]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Reset Lock', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if exists (select top 1 xdblock from dbo.bisintdata_lock)
update dbo.bisintdata_lock set xdbLock = 0', 
		@database_name=N'BISINTDATA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Custom updates in BISIBOBDB]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Custom updates in BISIBOBDB', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
/*Update */
/*1 - Asset mit Gebäude OHNE xobjkey9 oder MIT xobjkey9 <> physisch >>> physisch */
update o set 
xobjkey9 = (select lid from dbo.fn_keyval(''Objektklassifzierung'') where xkeyalphanr = ''1'')
--select * 
from xlbiobjekte o
left join xlbikeyval kv on o.xobjkey9 = kv.lid
where o.ldelete = 0
and exists (select null from xlbigrundgeb where lparentid = o.lid and ldelete = 0)
and (xobjkey9 is null or xkeyalphanr <> 1)

/*2 - Asset OHNE Gebäude OHNE xobjkey9 > statistisch */
update o set 
xobjkey9 = (select lid from dbo.fn_keyval(''Objektklassifzierung'') where xkeyalphanr = ''1'')
--select * 
from xlbiobjekte o
left join (select kv.lid, kv.xkeyalphanr, kv.xkeymc , kh.xkeyhmc from xlbikeyval kv inner join xlbikeyhead kh on kv.lparentid = kh.lid and xkeyhmc = ''Objektklassifzierung'') kv
on o.xobjkey9 = kv.lid
where o.ldelete = 0
and not exists (select null from xlbigrundgeb where lparentid = o.lid and ldelete = 0)
and (xobjkey9 is null)


-- Objekte die vor dem 01.01.2018 verkauft wurden, sollen entfernt werden
declare @objL varchar (max) = ''''
select @objL = @objL + obj.xobjnr + '';''
from BISIBOBDB.dbo.xlbiobjekte obj
join BISimportDB.dbo.SAP_vibdbe inp on obj.xobjnr = inp.xObjNr
where BISDSYSDB.dbo.BIS_Convert_DATE(nullif(VALIDTO,''99991231''), null) < ''20180101''

exec BISIBOBDB.dbo.BIS_DltImmoObject @ObjList = @objL, @DltObj = 1', 
		@database_name=N'BISIBOBDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: ImmoInterface]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: ImmoInterface', 
		@step_id=11, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\bisonbox\immoInterface.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 4 /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - packge: bobint]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - packge: bobint', 
		@step_id=12, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\bisonbox\BOBINT.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 4 /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: SAP_FI]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: SAP_FI', 
		@step_id=13, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\SAP\SAP_FI.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 1 /Par AccountPrefix;HIRAsap /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: FIBU_KUM]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: FIBU_KUM', 
		@step_id=14, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"\\dbp332003\BISSQL\SSIS\bisonbox\FIBU_KUM.dtsx\"" /CHECKPOINTING OFF /SET "\"\Package.Variables[User::parServerName_SDB].Properties[Value]\"";"\"DBP332003\SQL_BISON_TEST\"" /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: investor]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: investor', 
		@step_id=15, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"\\dbp332003\BISSQL\SSIS\bisonbox\investor.dtsx\"" /CHECKPOINTING OFF /SET "\"\Package.Variables[User::parServerName_SDB].Properties[Value]\"";"\"DBP332003\SQL_BISON_TEST\"" /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Update Statistics]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Update Statistics', 
		@step_id=16, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.UpdateStatistics @ScanType = ''FULLSCAN''', 
		@database_name=N'BISDSYSDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: reportBatch]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: reportBatch', 
		@step_id=17, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\CITSSIS\bisonbox\reportBatch.dtsx\"" /SERVER "\"DBP332003\SQL_BISON_TEST\"" /ENVREFERENCE 4 /Par parBatchIdent;"\"ML_01\"" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call SSIS - package: cube]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call SSIS - package: cube', 
		@step_id=18, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"\\dbp332003\BISSQL\SSIS\bisonbox\cube.dtsx\"" /CHECKPOINTING OFF /SET "\"\Package.Variables[User::parServerName_SDB].Properties[Value]\"";"\"DBP332003\SQL_BISON_TEST\"" /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [CHCKMAINTJ - CommandLog]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CHCKMAINTJ - CommandLog', 
		@step_id=19, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'IF(SELECT Count(*)FROM BISDSYSDB.dbo.CommandLog WHERE ErrorNumber!=0)!=0
	RAISERROR(''Fehler im DB-Wartungsjob. Nähere Einzelheiten in der Protokolldatei BISDSYSDB.dbo.CommandLog.'',18,1)', 
		@database_name=N'BISDSYSDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [CHCKMAINTJ - Prozesslog bereinigen]    Script Date: 08.05.2025 14:52:36 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CHCKMAINTJ - Prozesslog bereinigen', 
		@step_id=20, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE BISDSYSDB.dbo.TBISPORTAL_PROCESSLOG00 WHERE StartTime <= DATEADD(YEAR,-2,GetDate())', 
		@database_name=N'BISDSYSDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily 00:00', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200924, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'ec1b0df6-ffc1-4d0b-9d10-ab7892b7b1a2'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


