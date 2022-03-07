with cte as
(
    select

    Courier_ID,
    BillingStatus,
    EstimatedTotal,
    taxtotal,
    shippingcost,
    a.Month,
    a.MonthName,
    a.year,
    a.CurrentMonth
    from {{ref('OrdersFact')}} join {{ref('Date')}} a on DateReceived = a.Date
),
date as
(
    select  Courier_ID,BillingStatus,Month,Monthname,year,currentmonth,sum(EstimatedTotal) EstimatedTotal,sum(taxtotal) taxtotal,sum(shippingcost) shippingcost,count(Courier_ID) numberOfOrders
     from cte group by Courier_ID,BillingStatus,Month,Monthname,year,currentmonth
) select * from date