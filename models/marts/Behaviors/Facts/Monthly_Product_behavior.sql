with cte as
(
    select

    items,
    cost,
    quantity,
    a.Month,
    a.MonthName,
    a.year,
    a.CurrentMonth
    from {{ref('lineitemFact')}} join {{ref('Date')}} a on DateReceived = a.Date
),
date as
(
    select items,Month,monthname,year,CurrentMonth,sum(cost) cost,sum(quantity) quantity 
     from cte group by year , monthname, month,Currentmonth,items
) select * from date