-- Use the `ref` function to select from other models

select *
from "portfolio_website"."analytics"."my_first_dbt_model"
where id = 1