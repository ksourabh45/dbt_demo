select student_id,
	sum(score)
from demo.test
group by student_id
