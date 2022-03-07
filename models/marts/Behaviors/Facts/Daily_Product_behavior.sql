with cte as
(
select DateReceived,items,sum(cost) cost,sum(quantity) quantity from {{ref('lineitemFact')}} group by DateReceived,items
),
date as
(
    select

    DateReceived,
    items,
    cost,
    quantity,
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