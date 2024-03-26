with Assessement_start_event as
(
select
	"id",
	"type" as Event_type,
	"action",
	actor ->> 'type' as user_type,
	actor ->> 'userid' as user_id,
    object ->> 'assessmentid' as assessment_id,
    object ->> 'type' as object_type,
    object ->> 'name' as Assessment_name,
	To_timestamp(eventtime, 'yyyy-mm-dd"T"hh24:mi:ss.ff3"Z"') as start_time
from learninganalytics.assessmentevent
where action='Started'
),
Assessement_submit_event as
(
select
	"id",
	"type" as Event_type,
	"action",
	actor ->> 'type' as user_type,
	actor ->> 'userid' as user_id,
    object ->> 'assessmentid' as assessment_id,
    object ->> 'type' as object_type,
    object ->> 'name' as Assessment_name,
	To_timestamp(eventtime, 'yyyy-mm-dd"T"hh24:mi:ss.ff3"Z"') as submit_time
from learninganalytics.assessmentevent
where action='Submitted'
)
select a.id, 
a. Event_type, 
a.user_type, 
a.user_id, 
a.assessment_id, 
a.assessment_name, 
b.submit_time,
a.start_time, 
AGE(b.submit_time,a.start_time) as test_time, 
ROUND(Extract(epoch FROM (b.submit_time - a.start_time))/60::numeric, 2) AS test_minutes
from Assessement_submit_event b join Assessement_start_event a
on a.assessment_id=b.assessment_id and a.user_id=b.user_id
