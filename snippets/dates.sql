--declare variable
DECLARE @start datetime = DATEADD(mm,DATEDIFF(mm,0,GETDATE())-12,0)


--convert a dim_date back to datetime
cast(cast(dim_utc_last_interaction_type_dt_id as varchar(8)) as datetime)

--convert a datetime to dim_date_id
isnull(convert(varchar(8), pas.created_at, 112), -1)

--convert datetime to dim_time_id
isnull(cast(replace(convert(varchar(8), pas.created_at, 8), ':', '') as int), -1)


--change timezones
select getutcdate()
     , getutcdate() at time zone 'UTC' at time zone 'Eastern Standard Time'
     , getdate() at time zone 'Eastern Standard Time'
     , cast(getdate() at time zone 'Eastern Standard Time' as datetime)


--minus x number of days using date_ids
DECLARE @end int = 
    (select two_days_ago
    from (select m.dim_date_id,
            lag(dd.dim_date_id,2, 20160101) over(order by dd.dim_date_id) as two_days_ago
        from (select max(dim_local_tx_created_date_id) as dim_date_id
            from outcomes_mart.dbo.fact_outcomes_tx) m
        right join outcomes_mart.dbo.dim_date dd
        on m.dim_date_id = dd.dim_date_id) d
    where d.dim_date_id is not null)
