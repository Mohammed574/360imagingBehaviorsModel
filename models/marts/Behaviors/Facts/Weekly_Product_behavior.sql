with cte as
(
    select

    items,
    cost,
    quantity,
    a.WeekOfMonth,
    a.WeekOfYear,
    a.MonthName,
    a.Year,
    a.CurrentWeek
    from {{ref('lineitemFact')}} join {{ref('Date')}} a on DateReceived = a.Date
),
date as
(
    select items,WeekOfMonth,WeekOfYear,monthname,year,CurrentWeek,sum(cost) cost,sum(quantity) quantity 
     from cte group by year , monthname, WeekOfYear,WeekOfMonth,CurrentWeek,items
) select * from date