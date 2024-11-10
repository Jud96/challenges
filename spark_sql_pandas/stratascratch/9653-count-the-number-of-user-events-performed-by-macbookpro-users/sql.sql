select event_name,count(*) from playbook_events
where device = 'macbook pro'
group by event_name