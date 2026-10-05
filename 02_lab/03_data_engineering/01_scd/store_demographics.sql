SELECT 
    s.business_entity_id,
    s.name
    -- dito natin idadagdag yung mga na-extract na fields
FROM sales.store AS s
LEFT JOIN LATERAL XMLTABLE(
    XMLNAMESPACES(
        'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey' AS sur
    ),
    '/sur:StoreSurvey'
    PASSING CAST(s.demographics AS xml)
    COLUMNS
        annual_sales numeric PATH 'sur:AnnualSales',
        annual_revenue numeric PATH 'sur:AnnualRevenue',
        bank_name text PATH 'sur:BankName', 
        business_type text PATH 'sur:BusinessType',
        year_opened integer PATH 'sur:YearOpened',
        square_footage integer PATH 'sur:SquareFeet',
        brands text PATH 'sur:Brands',
        internet text PATH 'sur:Internet',
        specialty text PATH 'sur:Specialty',
        number_employees integer PATH 'sur:NumberEmployees'
        

) AS demo ON true;


