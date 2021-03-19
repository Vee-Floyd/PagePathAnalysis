import pandas as pd
import pandas_gbq
import regex as regex
import tqdm
import pydata_google_auth
import Helper
import re
import Config

SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

credentials = pydata_google_auth.get_user_credentials(
    SCOPES,
    # Set auth_local_webserver to True to have a slightly more convienient
    # authorization flow. Note, this doesn't work if you're running from a
    # notebook on a remote sever, such as over SSH or with Google Colab.
    auth_local_webserver=True,
)

#Query to get Shapley values for pages & Categories. The query loops through the event level GA4 data for every month (shapley values are calculated at the monthly level)
for m in Config.months:
    df = pandas_gbq.read_gbq(
        f"""SELECT
    '{m}' as Month,
    Path,
    PathCategory,
    sum(Transactions.Transactions) as Transactions
    FROM
    (
    #Paths Table
    (SELECT 
    a.user_pseudo_id,
    SessionID,
    string_agg(pageurl, ',,,' ORDER BY a.event_date asc, a.event_timestamp asc) as Path,
    string_agg(pagecategory, ',,,' ORDER BY a.event_date asc, a.event_timestamp asc) as PathCategory
    FROM
    (select 
    parse_date('%Y%m%d',event_date) as event_date,
    event_timestamp,
    user_pseudo_id,
    CASE
    when key='ga_session_id' THEN value.int_value 
    ELSE NULL
    END as SessionID,
    params.key
    FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
    UNNEST(event_params) as params
    where event_name in ('page_view') and key ='ga_session_id' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
    )
     a 
    
    LEFT JOIN 
    
    
    (select 
    parse_date('%Y%m%d',event_date) as event_date,
    event_timestamp,
    user_pseudo_id,
    CASE
    when key='page_location' THEN value.string_value 
    ELSE NULL
    END as pageurl,
        {Config.caseStatement},
    params.key
    FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
    UNNEST(event_params) as params
    where event_name in ('page_view') and key ='page_location' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
    )
     b
    
    on a.event_date=b.event_date AND a.user_pseudo_id=b.user_pseudo_id AND a.event_timestamp=b.event_timestamp
    
    
    
    GROUP BY
    a.user_pseudo_id,
    SessionID
    
    ) Paths
    
    RIGHT JOIN
    
    #Transactions
    (
    SELECT 
    a.user_pseudo_id,
    SessionID,
    count(distinct TransactionID) as Transactions
    FROM
    (select 
    parse_date('%Y%m%d',event_date) as event_date,
    user_pseudo_id,
    event_timestamp,
    CASE
    when key='ga_session_id' THEN value.int_value 
    ELSE NULL
    END as SessionID,
    params.key
    FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
    UNNEST(event_params) as params
    where event_name in ('purchase') and key ='ga_session_id' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
    )
     a 
    
    LEFT JOIN 
    
    
    (select 
    parse_date('%Y%m%d',event_date) as event_date,
    user_pseudo_id,
    event_timestamp,
    CASE
    when key='transactions_id' THEN value.int_value 
    ELSE NULL
    END as TransactionID,
    params.key
    FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
    UNNEST(event_params) as params
    where event_name='purchase' and key='transactions_id' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
    )
     b
    
    on a.event_date=b.event_date AND a.user_pseudo_id=b.user_pseudo_id and b.event_timestamp = a.event_timestamp
    
    
    GROUP BY
    a.user_pseudo_id,
    SessionID
    
    HAVING transactions > 0
    ) Transactions
    
    ON
    
    Paths.user_pseudo_id = Transactions.user_pseudo_id AND Paths.SessionID = Transactions.SessionID)
    
    GROUP BY Path,PathCategory
    
    """,
        project_id='firebase-test-dev-286022',
        credentials=credentials,
    )



#Vee to remove after demo
    #df.to_csv('InitialBQExport.csv')
#Paths are converted into a list of pages (excluding checkout pages)
    df['Channel'] = df['Path'].apply(Helper.get_channel)
#Vee to remove after demo
   #df.to_csv('InitialdfplusChannel.csv')
    ls_channels = list(set([x for sublist in df['Channel'].values.tolist() for x in sublist if not Config.regexexclude.match(x)]))
    # Vee to remove after demo
    #ls_channelsdf=pd.DataFrame(ls_channels).to_csv('LSChannels.csv')
#Touchpoints per path are determined to help assign shapely values
    df['Touchpoints'] = df['Channel'].apply(lambda x: len(x))
    # Vee to remove after demo
    #df.to_csv('DFWithTouchpoints.csv')
#shapley value calculated and assigned its respective month
    df_shapley_final = Helper.shapley_faster(df, ls_channels, 'Channel')
    df_shapley_final['Month']=f"{m}"
#Upload shapley values to BigQuery
    df_shapley_final.to_gbq(
        destination_table=Config.mainDestinationTable,
        project_id=Config.project_id,
        chunksize=None,
        reauth=False,
        if_exists='append',
        auth_local_webserver=False,
        table_schema=None,
        location=None,
        progress_bar=True,
        credentials=credentials)



#Repeat the above process but for category shapley values
    df['Channel'] = df['PathCategory'].apply(Helper.get_channel)
    ls_channels = list(set([x for sublist in df['Channel'].values.tolist() for x in sublist if not Config.regexexclude.match(x)]))
    df['Touchpoints'] = df['Channel'].apply(lambda x: len(x))
    Categorydf_shapley_final = Helper.shapley_faster(df, ls_channels, 'Channel')
    Categorydf_shapley_final['Month'] = f"{m}"
    Categorydf_shapley_final.to_gbq(
        destination_table=Config.categoryDestinationTable,
        project_id=Config.project_id,
        chunksize=None,
        reauth=False,
        if_exists='append',
        auth_local_webserver=False,
        table_schema=None,
        location=None,
        progress_bar=True,
        credentials=credentials)


#SQL Query to get path length metrics (AOV, Transactions & Revenue by path legnth)
    pathLengthdf = pandas_gbq.read_gbq(
        f"""
        SELECT
        '{m}' as Month,
        (Length(Path)-Length(Replace(Path,',',''))+1) as PathLength,
        sum(Transactions) as Transactions,
        sum(Revenue) as Revenue,
        sum(Revenue)/sum(Transactions) as AOV
        FROM
        (
        
        SELECT
        #Transactions.user_pseudo_id,
        #Transactions.SessionID,
        Path,
        sum(Transactions.Transactions) as Transactions,
        sum(Transactions.revenue) as Revenue
        FROM
        (
        #Paths Table
        (SELECT 
        a.user_pseudo_id,
        SessionID,
        string_agg(pageurl, ',,,' ORDER BY a.event_date asc, a.event_timestamp asc) as Path
        FROM
        (select 
        parse_date('%Y%m%d',event_date) as event_date,
        event_timestamp,
        user_pseudo_id,
        CASE
        when key='ga_session_id' THEN value.int_value 
        ELSE NULL
        END as SessionID,
        params.key
        FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
        UNNEST(event_params) as params
        where event_name in ('page_view') and key ='ga_session_id' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
        )
         a 
        
        LEFT JOIN 
        
        
        (select 
        parse_date('%Y%m%d',event_date) as event_date,
        event_timestamp,
        user_pseudo_id,
        CASE
        when key='page_location' THEN value.string_value 
        ELSE NULL
        END as pageurl,
        params.key
        FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
        UNNEST(event_params) as params
        where event_name in ('page_view') and key ='page_location'  and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
        )
         b
        
        on a.event_date=b.event_date AND a.user_pseudo_id=b.user_pseudo_id AND a.event_timestamp=b.event_timestamp
        
        
        
        GROUP BY
        a.user_pseudo_id,
        SessionID
        
        ) Paths
        
        RIGHT JOIN
        
        #Transactions
        (
        SELECT 
        a.user_pseudo_id,
        SessionID,
        count(distinct TransactionID) as Transactions,
        sum(Revenue) as Revenue
        FROM
        (select 
        parse_date('%Y%m%d',event_date) as event_date,
        user_pseudo_id,
        event_timestamp,
        CASE
        when key='ga_session_id' THEN value.int_value 
        ELSE NULL
        END as SessionID,
        params.key
        FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
        UNNEST(event_params) as params
        where event_name in ('purchase') and key ='ga_session_id' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
        )
         a 
        
        LEFT JOIN 
        
        
        (select 
        parse_date('%Y%m%d',event_date) as event_date,
        user_pseudo_id,
        event_timestamp,
        ecommerce.purchase_revenue_in_usd as revenue,
        CASE
        when key='transactions_id' THEN value.int_value 
        ELSE NULL
        END as TransactionID,
        params.key
        FROM  `firebase-test-dev-286022.analytics_242508648.events_202*` ,
        UNNEST(event_params) as params
        where event_name='purchase' and key='transactions_id' and date_trunc(parse_date('%Y%m%d',event_date),MONTH)='{m}'
        )
         b
        
        on a.event_date=b.event_date AND a.user_pseudo_id=b.user_pseudo_id and b.event_timestamp = a.event_timestamp
        
        
        GROUP BY
        a.user_pseudo_id,
        SessionID
        
        HAVING transactions > 0
        ) Transactions
        
        ON
        
        Paths.user_pseudo_id = Transactions.user_pseudo_id AND Paths.SessionID = Transactions.SessionID)
        
        GROUP BY Path
        )
        Group by pathlength
        """,
        project_id='firebase-test-dev-286022',
        credentials=credentials,
    )
#Upload pathlength metrics to BigQuery
    pathLengthdf.to_gbq(
        destination_table=Config.PathLengthDestinationTable,
        project_id=Config.project_id,
        chunksize=None,
        reauth=False,
        if_exists='append',
        auth_local_webserver=False,
        table_schema=None,
        location=None,
        progress_bar=True,
        credentials=credentials)

