CREATE proc [dbo].[sp_ag_health]
    -- filter on the name of the database
    @dbname nvarchar(128) = null
    -- create a filter on is_local
    ,@filter_is_local_value bit = null
    -- create a filter on role
    ,@filter_role_value tinyint = null
    -- the threshold for alerting on log send queue size
    ,@alert_log_send_queue_threshold int = 50000
    -- the threshold for alerting on redo queue size
    ,@alert_redo_queue_threshold int = 50000
    -- enable/disable alerting on syhchronous mode
    ,@alert_if_syhchronous_mode bit = 1
    -- the columns to select in the result set
    ,@select_columns nvarchar(max) = null
    -- the columns to order by in the result set
    ,@orderby_columns nvarchar(max) = null
    -- shortcut to aid certain kinds of monitoring.
    -- Supported modes: 'QUEUE SIZE', 'VERSION'
    ,@mode nvarchar(100) = null
as
------------------------------------------------------------------------------
-- author:   mattmc3
-- version:  0.2.1
-- homepage: https://github.com/mattmc3/sp_ag_health
-- purpose:  report availability group health
------------------------------------------------------------------------------
set nocount on

if @mode = 'VERSION' begin
    select '0.2.1'
end
else if @mode = 'QUEUE SIZE' begin
    set @select_columns =
        'alerts' +
        ',database_name' +
        ',log_send_queue_size_kb' +
        ',redo_queue_size_kb' +
        ',ag_name' +
        ',ag_replica_server' +
        ',is_ag_replica_local' +
        ',ag_replica_role' +
        ',primary_replica' +
        ',synchronization_health_desc' +
        ',connected_state_desc' +
        ',availability_mode_desc' +
        ',is_readable_secondary' +
        ',synchronization_state_desc' +
        ',is_suspended' +
        ',is_joined' +
        ',log_send_rate_kb_per_sec' +
        ',redo_rate_kb_per_sec'
end

if object_id('tempdb..#ag') is not null drop table #ag
select id = identity(int, 1, 1)
     , db_name(dr_state.database_id) as [database_name]
     , ag.name as ag_name
     , ar.replica_server_name as ag_replica_server
     , case
          when ar_state.is_local = 1 then N'LOCAL'
          else N'REMOTE'
       end as is_ag_replica_local
     , case
          when ar_state.role_desc is null then N'DISCONNECTED'
          else ar_state.role_desc
       end as ag_replica_role
     , ag_state.primary_replica
     , dr_state.synchronization_health_desc
     , ar_state.connected_state_desc
     , ar.availability_mode_desc
     , case when ar_state.role <> 2 then ''
            when ar.secondary_role_allow_connections = 0 then 'FALSE'
            else 'TRUE'
       end as is_readable_secondary
     , dr_state.synchronization_state_desc
     , isnull(dr_state.is_suspended, 0)   as is_suspended
     , isnull(dbcs.is_database_joined, 0) as is_joined
     , dr_state.log_send_queue_size as log_send_queue_size_kb
     , dr_state.log_send_rate as log_send_rate_kb_per_sec
     , dr_state.redo_queue_size as redo_queue_size_kb
     , dr_state.redo_rate as redo_rate_kb_per_sec
  into #ag
  from sys.availability_groups as ag
  join sys.availability_replicas as ar
    on ag.group_id = ar.group_id
  join sys.dm_hadr_availability_replica_states as ar_state
    on ar.replica_id = ar_state.replica_id
  join sys.dm_hadr_database_replica_states dr_state
    on ag.group_id = dr_state.group_id
   and dr_state.replica_id = ar_state.replica_id
  join sys.dm_hadr_database_replica_cluster_states as dbcs
    on dr_state.replica_id = dbcs.replica_id
   and dr_state.group_database_id = dbcs.group_database_id
  join sys.dm_hadr_availability_group_states ag_state
    on ag.group_id = ag_state.group_id
 where db_name(dr_state.database_id) = isnull(@dbname, db_name(dr_state.database_id))
   and ar_state.is_local = isnull(@filter_is_local_value, ar_state.is_local)
   and ar_state.role = isnull(@filter_role_value, ar_state.role)

if object_id('tempdb..#alert') is not null drop table #alert
select ag.id as ag_id
     , ag.ag_name
     , nullif(ltrim(rtrim('' +
       case when ag.log_send_queue_size_kb >= @alert_log_send_queue_threshold then N'High log send queue size; '
            else ''
       end +
       case when ag.redo_queue_size_kb >= @alert_redo_queue_threshold then N'High redo queue size; '
            else ''
       end +
       case when ag.availability_mode_desc = 'SYNCHRONOUS_COMMIT' and @alert_if_syhchronous_mode = 1 then 'AG in syhchronous mode; '
            else ''
       end +
       case when ag.synchronization_health_desc not in ('HEALTHY') then 'AG status is: ' + ag.synchronization_health_desc + '; '
            else ''
       end +
       case when ag.is_suspended = 1 then 'AG is suspended; '
            else ''
       end +
       case when ag.connected_state_desc not in ('CONNECTED') then 'AG conn state is: ' + ag.connected_state_desc + '; '
            else ''
       end +
       case when ag.synchronization_state_desc not in ('SYNCHRONIZED', 'SYNCHRONIZING') then 'AG sync state is: ' + ag.synchronization_state_desc + '; '
            else ''
       end)), '') as alerts
  into #alert
  from #ag ag

-- sort by AGs with an alert, then the alert itself, then the db name, whether it's local, and finally the server
if object_id('tempdb..#result') is not null drop table #result
select ord = identity(int,1,1)
     , al.alerts
     , ag.*
  into #result
  from #ag ag
  join #alert al
    on ag.id = al.ag_id
 order by case when exists (select top 1 ''
                              from #alert x
                             where x.ag_name = ag.ag_name
                               and x.alerts is not null)
               then 0
               else 1
          end
     , case when al.alerts is null then 1 else 0 end
     , al.alerts
     , ag.[database_name]
     , ag.is_ag_replica_local
     , ag.ag_replica_server

-- get the results
-- notice that we don't trust @select_columns and @orderby_columns and
-- instead create our own safe SQL. Remeber little Bobby Tables! https://xkcd.com/327/
declare @sel_colsql varchar(max) = null
      , @ord_colsql varchar(max) = null

-- scrub the cols for pattern match
declare @orderby_columns_with_desc varchar(max) = ',' + replace(@orderby_columns, ' ', '') + ','
select @select_columns = ',' + replace(@select_columns, ' ', '') + ','
     , @orderby_columns = replace(@orderby_columns_with_desc, '-', '')


select @sel_colsql = substring((
    select ',' + quotename(c.name)
    from tempdb.sys.columns c
    where object_id = object_id('tempdb..#result')
    and @select_columns like '%,' + c.name + ',%'
    order by patindex('%,' + c.name + ',%', @select_columns)
    for xml path('')
), 2, 999999)

select @ord_colsql = substring((
    select ',' + quotename(c.name) +
           case when @orderby_columns_with_desc like '%,-' + c.name + ',%' then ' desc'
                else ''
           end
    from tempdb.sys.columns c
    where object_id = object_id('tempdb..#result')
    and @orderby_columns like '%,' + c.name + ',%'
    order by patindex('%,' + c.name + ',%', @orderby_columns)
    for xml path('')
), 2, 999999)

if @sel_colsql is null
begin
    set @sel_colsql =
        'alerts' +
        ',database_name' +
        ',ag_name' +
        ',ag_replica_server' +
        ',is_ag_replica_local' +
        ',ag_replica_role' +
        ',primary_replica' +
        ',synchronization_health_desc' +
        ',connected_state_desc' +
        ',availability_mode_desc' +
        ',is_readable_secondary' +
        ',synchronization_state_desc' +
        ',is_suspended' +
        ',is_joined' +
        ',log_send_queue_size_kb' +
        ',log_send_rate_kb_per_sec' +
        ',redo_queue_size_kb' +
        ',redo_rate_kb_per_sec'
end

if @ord_colsql is null
begin
    set @ord_colsql = 'ord'
end

-- pull the results
declare @sql nvarchar(max) = N'select ' + @sel_colsql + N' from #result order by ' + @ord_colsql
exec sp_executesql @sql

-- clean up
if object_id('tempdb..#ag') is not null drop table #ag
if object_id('tempdb..#alert') is not null drop table #alert
if object_id('tempdb..#result') is not null drop table #result
