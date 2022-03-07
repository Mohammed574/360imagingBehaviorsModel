with cte as
(
    
    select Courier_ID,DateReceived,DoctorName,REGEXP_EXTRACT_ALL(LineItems,r'item": \{"internalid": "(\d*)"') as product_id,
    REGEXP_EXTRACT_ALL(LineItems,r'"amount": ("null"|[-0-9]+.[0-9]+)') as amount,
    REGEXP_EXTRACT_ALL(LineItems,r'"quantity": ("null"|[0-9]+)') as quantity
    from 
    {{ref('stg_order')}}

),
joins as
(
    select x.Courier_ID,x.DateReceived,x.DoctorName,x.items,replace(x.cost,"null",'-9999.0')cost,replace(y.quantity,"null",'-9999.0')quantity from
    (select a.num ,a.Courier_ID,a.DateReceived,a.DoctorName,a.items,b.Cost from
        (select row_number() over() as num ,Courier_ID,DateReceived,DoctorName,items from(select product_id,Courier_ID,DateReceived,DoctorName from cte),unnest(product_id) as items)a
        join
        (select Cost,row_number() over() as num from (select amount from cte),unnest(amount) as Cost)b
        on a.num=b.num)x
    join
    (select quantity,row_number() over() as num from (select quantity from cte),unnest(quantity) as quantity)y
    on x.num=y.num
),
fix_type as
( select Courier_ID,DateReceived,DoctorName,items,cast(REGEXP_EXTRACT(cost,r'"?([-0-9]+)"?') as float64) cost,cast(REGEXP_EXTRACT(quantity,r'"?([-0-9]+)"?') as float64) quantity from joins)
select Courier_ID,DateReceived,DoctorName,cast(items as int) items,nullif(cost,-9999.0) cost,nullif(quantity,-9999.0) quantity from fix_type
