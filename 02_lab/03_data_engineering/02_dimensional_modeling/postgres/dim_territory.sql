DROP TABLE IF EXISTS dim_territory;
CREATE TABLE dim_territory AS 
select 
	territory_id 
	, "group" as territory_group
	, country_region_code 
	, name as territory
from sales.sales_territory st