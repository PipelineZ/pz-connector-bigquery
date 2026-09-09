INSERT INTO {{ sink('bq', 'orders_out') }}
select id, name
from {{ source('bq', 'orders_in') }}
order by id
