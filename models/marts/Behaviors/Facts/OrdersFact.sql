select
distinct Courier_ID,
DateReceived,
BillingStatus,
EstimatedTotal,
taxtotal,
shippingcost
from {{ref('stg_order')}}