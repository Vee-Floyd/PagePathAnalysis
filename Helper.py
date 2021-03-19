import pandas as pd
import re
import Config

#Creates channels columns
def get_channel(string):
    # a = string.replace('["', '')
    # a = a.replace('"]', '')
    b = list(dict.fromkeys(string.split(',,,')))
    b = [x for x in b if not Config.regexexclude.match(x)]

    return b

#Calculates shapley values
def shapley_faster(df, chan_ls, col_name):
    '''
    Function to calculate shapley values using the simplified version formula from equation (12) in this paper
    https://drive.google.com/open?id=13-0sEEbOnzIBOcsMcZaUgM6hd6IlcewP
    Note: - the sum of conversions of output dataframe is equal to sum of conversions of input dataframe
          - this is a faster computation method than the function shapley_values

    param: df - dataframe with channel as lists, number of conversions, and touchpoints:
        Channel              | Conversions | Touchpoints
        [Google]             |  1000       | 1
        [Facebook]           | 1000        | 1
        [Google, Facebook]   | 3000        | 2
    param: chan_ls - list of strings with channel names (output of facebook_clean function):
        ['Google', 'Facebook']
    param: col_name - string with the name of the column with the channels:
        'Channel'

    return: dataframe with conversions according to shapley value for each channel:
        Channel     | Conversions
        Google      | 2500
        Facebook    | 2500
    '''

    dict_chan = {
        'Channel': [],
        'Transactions': []
    }

    for chan in chan_ls:
        idx_aux = df[col_name].apply(lambda x: chan in x)
        df_chan_aux = df[idx_aux].copy()

        dict_chan['Channel'].append(chan)
        dict_chan['Transactions'].append((df_chan_aux['Transactions'].values / (df_chan_aux['Touchpoints'].values)).sum())

    df_shapley_channel = pd.DataFrame(dict_chan)

    return df_shapley_channel


def SankeyData(df,month):

    df['Path'].str.split(',,,')
    df['PathToSplit'] = df['Path'].str.split(',,,').values.tolist()
    Top10df = pd.DataFrame(df['PathToSplit'].values.tolist()).iloc[:, :10]
    Top10df = Top10df.join(df['Transactions'], how='left')
    Top10df=Top10df.groupby(by=Config.SankeyPagePathLevel).agg({'Transactions': 'sum'}).reset_index()
    Top10df.add_prefix("_")
    Top10df["Month"]=month
    return Top10df