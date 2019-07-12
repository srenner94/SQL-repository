
create   proc [dbo].[sp_log_health] as

set nocount on

declare @drive_free_space_threshold_pct decimal(18,3) = 5.000
      , @big_log_threshold_gb decimal(18,3) = 150.000
      , @shrinkable_log_threshold_pct decimal(18,3) = 50.00
      , @shrinkable_log_free_threshold_gb decimal(18,3) = 10.000

if object_id('tempdb..#db_file_sizes') is not null drop table #db_file_sizes
select dovs.volume_mount_point as drive
     , dovs.logical_volume_name
     , cast(dovs.total_bytes / power(1024.0, 3) as decimal(18, 3)) as total_drive_space_gb
     , cast(dovs.available_bytes / power(1024.0, 3) as decimal(18, 3)) as free_drive_space_gb
     , cast(
       case when dovs.total_bytes = 0.0 then 0.0
            else ((dovs.total_bytes - dovs.available_bytes) / (dovs.total_bytes * 1.0)) * 100.0
       end as decimal(18, 2)) as drive_percent_used
     , cast(
       case when dovs.total_bytes = 0.0 then 0.0
            else (dovs.available_bytes / (dovs.total_bytes * 1.0)) * 100.0
       end as decimal(18, 3)) as drive_percent_free
     , mf.database_id
     , db_name(mf.database_id) as database_name
     , case when mf.database_id <= 4 then 1 else 0 end as is_system_db
     , case mf.type when 0 then 'DATA' when 1 then 'LOGS' else mf.type_desc end as file_type
     , mf.name as file_name
     , mf.physical_name
     , cast((mf.size * 8.0) / power(1024.0, 2) as decimal(18, 2)) as file_size_gb
into #db_file_sizes
from sys.master_files mf
cross apply sys.dm_os_volume_stats(mf.database_id, mf.file_id) dovs

if object_id('tempdb..#ags') is not null drop table #ags
select dhdrs.database_id
     , ag.name as ag_name
     , dhags.primary_replica as ag_primary
into #ags
from sys.dm_hadr_database_replica_states as dhdrs
join sys.availability_groups as ag                  on ag.group_id = dhdrs.group_id
join sys.dm_hadr_availability_group_states as dhags on dhags.group_id = ag.group_id
where dhdrs.is_local = 1

if object_id('tempdb..#dm_db_log_space_usage') is not null drop table #dm_db_log_space_usage
create table #dm_db_log_space_usage (
    dbname nvarchar(128)
    ,database_id smallint
    ,total_log_size_in_bytes bigint	
    ,used_log_space_in_bytes bigint
    ,used_log_space_in_percent real
    ,log_space_in_bytes_since_last_backup bigint
)

declare @stmt varchar(1000) 
select @stmt = 'USE ? INSERT INTO #dm_db_log_space_usage SELECT ''?'' AS dbname, * FROM sys.dm_db_log_space_usage' 
exec sp_msforeachdb @stmt

if object_id('tempdb..#log_space') is not null drop table #log_space
select ddlsu.database_id
     , ddlsu.dbname
     , ag.ag_name
     , ag.ag_primary
     , cast(ddlsu.total_log_size_in_bytes / power(1024.0, 3) as decimal(18, 3)) as total_log_size_gb
     , cast(ddlsu.used_log_space_in_bytes / power(1024.0, 3) as decimal(18, 3)) as used_log_size_gb
     , cast(ddlsu.used_log_space_in_percent as decimal(18, 3)) as used_log_space_percent
     , cast(100.0 - ddlsu.used_log_space_in_percent as decimal(18, 3)) as free_log_space_percent
     , cast((ddlsu.total_log_size_in_bytes - ddlsu.used_log_space_in_bytes) / power(1024.0, 3) as decimal(18, 3)) as free_log_space_gb
     , cast(ddlsu.log_space_in_bytes_since_last_backup / power(1024.0, 3) as decimal(18, 3)) as log_space_since_backup_gb
into #log_space
from #dm_db_log_space_usage ddlsu
left join #ags ag on ddlsu.database_id = ag.database_id

------------------------------------------------------------------------------
-- get drive space
if object_id('tempdb..#drive_space') is not null drop table #drive_space
select a.drive
     , a.logical_volume_name
     , case when a.log_count > 0 then 1 else 0 end as is_log_drive
     , a.total_drive_space_gb
     , a.free_drive_space_gb
     , a.drive_percent_used
     , a.drive_percent_free
into #drive_space
from (
    select *
         , row_number() over (partition by x.drive
                              order by x.free_drive_space_gb desc) as ord
         , (select count(*)
            from #db_file_sizes x2
            where x2.file_type = 'LOGS'
            and x2.drive = x.drive
            and x.is_system_db <> 1) as log_count
    from #db_file_sizes x
) a
where a.ord = 1
order by a.drive

-- get log info
if object_id('tempdb..#log_result') is not null drop table #log_result
select a.drive
     , a.database_name
     , a.file_name
     , b.ag_primary
     , b.log_space_since_backup_gb
     , b.total_log_size_gb
     , b.free_log_space_gb
     , a.file_size_gb
     , 'use ' + quotename(a.database_name) + '; dbcc shrinkfile (' + quotename(a.file_name) + ', 0, truncateonly)' as sql_shrink
into #log_result
from #db_file_sizes a
join #log_space b on a.database_id = b.database_id
where a.file_type = 'LOGS'
and a.is_system_db = 0
order by a.is_system_db, a.drive, a.file_size_gb desc

------------------------------------------------------------------------------
select *
from #drive_space

-- show issues and opportunities
declare @issues int
select @issues = count(*)
from #drive_space a
where a.is_log_drive = 1
and a.drive_percent_free < @drive_free_space_threshold_pct

if @issues = 0
begin
    select 'none' as issue
end
else
begin
    ;with logdata as (
            select a.drive
             , a.database_name
             , a.file_name
             , b.ag_primary
             , b.log_space_since_backup_gb
             , b.total_log_size_gb
             , b.free_log_space_gb
             , a.file_size_gb
             , cast(
               case when b.total_log_size_gb = 0.0 then 0.0
                    else (b.free_log_space_gb / b.total_log_size_gb) * 100.0
               end as decimal(18,3)) as free_log_space_pct
             , 'use ' + quotename(a.database_name) + '; dbcc shrinkfile (' + quotename(a.file_name) + ', 0, truncateonly)' as sql_shrink
        from #db_file_sizes a
        join #log_space b on a.database_id = b.database_id
        where a.file_type = 'LOGS'
        and a.is_system_db = 0
    )
    select *
    from (
        select '1-big log: run `exec cmmdba.dbo.p_shrink_user_db_logs`' as issue
             , *
        from logdata l
        where l.total_log_size_gb >= @big_log_threshold_gb
        union all
        select top 5
               '2-shrinkable log' as issue
              , *
        from logdata l
        where l.total_log_size_gb < @big_log_threshold_gb
        and (l.free_log_space_pct >= @shrinkable_log_threshold_pct
             or l.free_log_space_gb >= @shrinkable_log_free_threshold_gb)
    ) a
    order by a.issue, case when left(a.issue, 1) = '1' then total_log_size_gb else free_log_space_gb end desc
end

-- show all log info
select a.drive
     , a.database_name
     , b.ag_primary
     , b.log_space_since_backup_gb
     , b.total_log_size_gb
     , b.free_log_space_gb
     , b.used_log_size_gb
     , b.used_log_space_percent
     , a.file_size_gb
from #db_file_sizes a
join #log_space b on a.database_id = b.database_id
where a.file_type = 'LOGS'
order by a.is_system_db, a.drive, a.file_size_gb desc
