with cte as
(
select Courier_ID,DateReceived,BillingStatus,sum(EstimatedTotal) EstimatedTotal,sum(taxtotal) taxtotal,sum(shippingcost) shippingcost,count(Courier_ID) numberOfOrders from {{ref('OrdersFact')}} group by DateReceived,courier_ID,BillingStatus
),
date as
(
    select

    Courier_ID,
    DateReceived,
    BillingStatus,
    EstimatedTotal,
    taxtotal,
    shippingcost,
    numberOfOrders,
    a.Day,
    a.Weekday,
    a.WeekDayName,
    a.DOWInMonth,
    a.DayOfYear,
    a.Year,
    a.MonthName,
    a.IsWeekend,
    a.IsHoliday,
    a.CurrentDay
    from cte join {{ref('Date')}} a on DateReceived = a.Date
) select * from date