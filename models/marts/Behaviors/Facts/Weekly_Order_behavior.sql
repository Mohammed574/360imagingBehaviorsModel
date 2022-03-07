with cte as
(
    select

    Courier_ID,
    BillingStatus,
    EstimatedTotal,
    taxtotal,
    shippingcost,
    a.WeekOfMonth,
    a.WeekOfYear,
    a.MonthName,
    a.Year,
    a.CurrentWeek
    from {{ref('OrdersFact')}} join {{ref('Date')}} a on DateReceived = a.Date
),
date as
(
    select Courier_ID,BillingStatus,WeekOfMonth,WeekOfYear,monthname,year,CurrentWeek,sum(EstimatedTotal) EstimatedTotal,sum(taxtotal) taxtotal,sum(shippingcost) shippingcost,count(Courier_ID) numberOfOrders
     from cte group by year , monthname, WeekOfYear,WeekOfMonth,CurrentWeek,Courier_ID,BillingStatus
) select * from date