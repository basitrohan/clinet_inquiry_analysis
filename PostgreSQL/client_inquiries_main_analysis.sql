--checking indexing if present or not
select
    indexname,
    indexdef 
from pg_indexes 
where tablename = 'inquiries'
order by indexname

--creating necessary indexes
create index if not exists idx_inquiries_created_at on inquiries(created_at)
create index if not exists idx_inquiries_agent on inquiries(agent_id)
create index if not exists idx_inquiries_issue_cat on inquiries(issue_category_id)
create index if not exists idx_inquiries_priority on inquiries(priority_id)
create index if not exists idx_inquiries_resolution_time ON inquiries(resolution_minutes)
create index if not exists idx_inquiries_status_created ON inquiries(status, created_at)

--creating joins to avoid writing joins everytime
create or replace view v_inquiries as 
select 
	i.*, p.priority, p.sla_hours, ch.channel, r.region, a.handled_by, ic.issue_category,
	isc.issue_subcategory
from inquiries as i
join priorities as p on i.priority_id = p.priority_id 
join channels as ch on i.channel_id = ch.channel_id
join regions as r on i.region_id = r.region_id
join agents as a on i.agent_id = a.agent_id
join issue_categories as ic on i.issue_category_id = ic.issue_category_id
join issue_subcategories as isc on i.issue_subcategory_id = isc.issue_subcategory_id

--validation to check the any null values and missing FK links
select
	count(*) as total_rows,
	count(*) filter(where resolved_at is not null) as res_count,
	count(*) filter(where resolved_at is null) as unres_count
from inquiries

--analysis queries
--overall resolved within SLA
select
	count(*) filter(where resolved_within_sla = True) * 100
	/nullif (count(*)filter(where resolved_at is not null),0) as pct_within_sla  
from v_inquiries

--raw counts used to compute the percent
select
	count(*) filter(where resolved_at is not null) as resolved_count,
	count(*) filter(where resolved_within_sla = True) as within_sla_count,
	count(*) filter(where resolved_within_sla = False) as breached_count
from v_inquiries

--SLA % by priority
select priority,
	round(count(*) filter(where resolved_within_sla = True):: numeric 
	--since sla is true & false then to cascade it to numeric to get the % value 
	/nullif (count(*)filter(where resolved_at is not null),0)*100,2) as pct_within_sla,
	count(*) filter(where resolved_at is not null) as resolved_count
from v_inquiries
group by priority
order by pct_within_sla

--Avg resolution minutes by issue category
select 
	issue_category, round(avg(resolution_minutes)::numeric,1) as avg_res_min,
	count(*) as cnt
from v_inquiries
where resolution_minutes is not null
group by issue_category
order by avg_res_min desc

--top issue subcategories (counts + avg resolution)
select 
	issue_category, issue_subcategory, count(*) as cnt,
	round((avg(resolution_minutes)::numeric),1)as avg_mins
from v_inquiries
group by issue_category, issue_subcategory
order by cnt desc
limit 20

--agent performance (>=20 tickets)
select
	handled_by, count(*) as total_handled,
	round(avg(resolution_minutes)::numeric,1) as avg_res_mins,
	round(avg(satisfaction_score)::numeric,2)as avg_sat_score,
	round(count(*) filter(where resolved_within_sla = true)::numeric
            / nullif(count(*) filter (where resolved_at is not null),0) * 100, 2) as pct_within_sla
from v_inquiries
group by handled_by
having count(*) >= 20
order by pct_within_sla asc
limit 50;

--monthly trend: volume, avg resolution, avg satisfaction
select to_char(created_at, 'YYYY-MM') as month,
       count(*) as inquiries,
       round(avg(resolution_minutes)::numeric,1) as avg_resolution_minutes,
       round(avg(satisfaction_score)::numeric,2) as avg_satisfaction
from v_inquiries
group by month
order by month 

--open tickets near SLA breach (hours elapsed / sla_hours)
select 
	agent_id, client_id, channel_id,
	round(extract(epoch from (now()-created_at))/3600 ,3)as hours_open,
	sla_hours,
	round(extract(EPOCH from (now() - created_at))/3600 / sla_hours * 100, 1) as pct_sla_elapsed
from v_inquiries
where resolved_at is null
order by pct_sla_elapsed desc
limit 100;

--individual ticket resolution times compared to agent averages and overall rankings
select 
	handled_by,
	resolution_minutes,
	avg(resolution_minutes)over(partition by handled_by) as agent_avg,
	rank()over(order by resolution_minutes desc) as speed_rank
	from v_inquiries
	limit 20

--agent performance dashboard
with agent_stats as (
    select 
        handled_by,
        round(avg(resolution_minutes),2) as avg_res_time,
        count(*) as ticket_count
    from v_inquiries 
    group by handled_by
),
sla_performance as (
    select 
        handled_by,
        round(avg(case when resolved_within_sla then 1 else 0 end),2) as sla_rate
    from v_inquiries
    group by handled_by
)
select * from agent_stats join sla_performance using (handled_by)

--trend analysis
with monthly_trends as(
	select
		date_trunc('month',created_at) as month,
		count(*) as total_tickets,
		round(avg(resolution_minutes),2) as avg_res_time
	from v_inquiries
	group by month
)
select
	month,
	total_tickets,
	avg_res_time,
	lag(avg_res_time)over(order by month) as per_month_avg,
	(avg_res_time - lag(avg_res_time) over (order by month)) as change
from monthly_trends

--coherent analysis
-- First-time vs repeat resolution efficiency
with first_resolution as (
    select 
        client_id,
        min(created_at) as first_contact_date
    from v_inquiries
    group by client_id
)
select 
    extract(month from f.first_contact_date) as cohort_month,
    round(avg(v.resolution_minutes),2) as avg_resolution_time,
    count(distinct v.client_id) as client_count
from v_inquiries v
join first_resolution f on v.client_id = f.client_id
group by cohort_month
order by cohort_month

--predictive indicator
--tickets likely to breach SLA
select *,
    case when (extract(epoch from (now()-created_at))/3600 / sla_hours) > 0.8 then 'High Risk'
         when (extract(epoch from (now()-created_at))/3600 / sla_hours) > 0.5 then 'Medium Risk'
         else 'Low Risk'
    end as breach_risk
from v_inquiries
where resolved_at is null
limit 20

--cross-departmental impact
-- Channel efficiency by issue type
select 
    channel,
    issue_category,
    round(avg(resolution_minutes),2) as avg_res_time,
    round(avg(satisfaction_score),2) as avg_satisfaction,
    count(*) as volume,
    rank() over (partition by issue_category order by avg(resolution_minutes)) as efficiency_rank
from v_inquiries
group by channel, issue_category

--root casue analysis: why sla breaches happen
-- analyze what factors contribute most to sla breaches
with sla_analysis as (
    select 
        case when resolved_within_sla = true then 'Within_SLA' else 'Breached_SLA' end as sla_status,
        priority,
        channel,
        issue_category,
        region,
        resolution_minutes,
        sla_hours
    from v_inquiries 
    where resolved_at is not null
)
select 
    sla_status,
    priority,
    channel,
    issue_category,
    region,
    count(*) as ticket_count,
    round(avg(resolution_minutes),1) as avg_resolution_time,
    round(avg(sla_hours),1) as avg_sla_hours
from sla_analysis
group by sla_status, priority, channel, issue_category, region
having count(*) > 10  -- only significant patterns
order by sla_status, ticket_count desc

--Performance Degradation Analysis: Why Efficiency Changes Over Time
--identify what drove performance changes month-over-month
with monthly_drivers as (
    select 
        date_trunc('month', created_at) as month,
        issue_category,
        channel,
        priority,
        count(*) as ticket_count,
        round(avg(resolution_minutes),2) as avg_resolution_time,
        round(sum(resolution_minutes),2) as total_time_spent
    from v_inquiries
    where resolution_minutes is not null
    group by month, issue_category, channel, priority
),
monthly_changes as (
    select *,
        lag(avg_resolution_time) over (partition by issue_category, channel, priority order by month) as prev_month_time
    from monthly_drivers
)
select 
    month,
    issue_category,
    channel, 
    priority,
    ticket_count,
    avg_resolution_time,
    prev_month_time,
    round(avg_resolution_time - prev_month_time, 1) as time_change,
    case when avg_resolution_time - prev_month_time > 100 then 'Major_Degradation'
         when avg_resolution_time - prev_month_time > 50 then 'Moderate_Degradation' 
         when avg_resolution_time - prev_month_time < -50 then 'Major_Improvement'
         else 'Stable' end as change_type
from monthly_changes
where prev_month_time is not null
    and (avg_resolution_time - prev_month_time) > 50  -- focus on significant changes
order by time_change desc

--exporting the csv file
create table ml_sla_features as
select inquiry_id,
       priority,
       issue_category,
       issue_subcategory,
       channel,
       region,
       handled_by,
       extract(hour from created_at) as hour_of_day,
       extract(DOW from created_at) as weekday,
       case when resolved_within_sla = true then 0 else 1 end as sla_breach_flag,
       resolution_minutes,  -- Keep for feature engineering
       sla_hours,           -- Keep for calculations
       created_at           -- Keep for time-based features
from v_inquiries
where created_at is not null
--exported the table

