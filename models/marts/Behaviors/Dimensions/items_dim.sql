with cte as
(select cast(x.id as int) as id,x.name,c.description from

(select distinct a.num,a.id,b.name from

(select  id,row_number() over() as num from (select product_id,product_name,product_description from {{ref('stg_items')}}),unnest(product_id) as id)a
join
(select name,row_number() over() as num from (select product_name from {{ref('stg_items')}}),unnest(product_name) as name)b
on a.num=b.num) x

join
(select description,row_number() over() as num from (select product_description from {{ref('stg_items')}}),unnest(product_description) as description)c
on x.num=c.num
) ,
dupli as
(
    select *,row_number() over(partition by id order by id) num from cte 
) select distinct id,name,description from dupli where num = 1
