with su as
(select survived,pclass,count(*) from titanic
group by survived,pclass)
select survived,first_class,p2.second_class,third_class
from 
(select survived,count as first_class  from su where pclass = 1) p1
join (select survived,count as second_class  from su where pclass = 2)p2 using(survived)
join (select survived,count as third_class  from su where pclass = 3)p3 using(survived)