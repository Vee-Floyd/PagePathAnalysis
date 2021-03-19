import re
#Months to run the query for (query default is set to 'append' so delete table if a month is run more than once
months=['2020-09-01','2020-10-01','2020-11-01','2020-12-01','2021-01-01','2021-02-01']
#Case statement categorizes pages based on URL structure
caseStatement='''
        CASE
        when key='page_location' 
        THEN 
            Case 
            when value.string_value LIKE "%/new-and-featured/%" THEN 'New And Featured'
            when value.string_value LIKE "%/collections/%" THEN 'Collections'
            when value.string_value LIKE "%/styles/%" THEN 'Styles'
            when value.string_value LIKE "%/customer/%" THEN 'Customer'
            when value.string_value LIKE "%/checkout/%" THEN 'Checkout'
            ELSE 'Product Page'
            END
        ELSE NULL
        END as pagecategory
        '''
#BigQuery Project ID
project_id='firebase-test-dev-286022'
#Exclude checkout/confirmation pages based on url structure
regexexclude = re.compile(
    r'REGEXTOREMOVECHECKOUTPAGES')
#Shapley Value by Category destination table
categoryDestinationTable='analytics_242508648.OnSitePathCategoryShapleyValues'
#Shapley Value by page destination table
mainDestinationTable='analytics_242508648.OnSitePathShapleyValues'
#Pathlength metrics table
PathLengthDestinationTable='analytics_242508648.OnSitePathLengthMetrics'
SankeyTable='analytics_242508648.SankeyTable'
SankeyPagePathLevel = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
