select REGEXP_EXTRACT_ALL(LineItems,r'item": \{"internalid": "(\d*)"') as product_id,
REGEXP_EXTRACT_ALL(LineItems,r'item": \{"internalid": "\d*", "name": "([ -:0-9a-zA-Z]+)"') as product_name,
REGEXP_EXTRACT_ALL(LineItems,r'"description": "([-: 0-9a-zA-Z\\@$/&().]+)') as product_description,
FROM {{source('RawData','stg_orders')}}