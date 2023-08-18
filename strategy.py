import pandas as pd
import datetime
import pm4py

def extract_start_end(df):
    dataframe = pm4py.format_dataframe(df.copy(), case_id='id', activity_key='activity', timestamp_key='start_time')
    log = pm4py.convert_to_event_log(dataframe)
    net, initial_marking, final_marking = pm4py.discover_dfg(log)
    
    start_act = list(initial_marking.keys())[0]
    end_act = list(final_marking.keys())[0]
    return start_act, end_act

def strategy_1(sep_df, known=['OEM']):
    # concatonated df
    fed_log = pd.concat(sep_df, ignore_index=True)

    resources = list(fed_log["resource"].unique())

    # after knowing the sink
    resources.remove(known[0])

    # Strategy 1 : start time of sink- end time of source
    # grab sink df
    sink_df = fed_log[fed_log['resource'].isin(known)]
    # get the start activity
    sink_start_act,_  = extract_start_end(sink_df)

    time_diff_matrix = {}
    for source in resources:
        # grab source df
        source_df = fed_log[fed_log['resource']==source]
        _, source_end_act = extract_start_end(source_df)

        for id in sink_df['id'].unique():
            # record time difference
            time_diff ={}
            # get the start activity time of the right side (Sink side)
            try:
                sink_start_time = datetime.strptime(
                    sink_df.loc[(sink_df['id'] == id) & (sink_df['activity'] == int(sink_start_act))]['start_time'].values[0], 
                    '%Y-%m-%d %H:%M:%S')
            except IndexError:
                continue
            
            for source_id in source_df['id'].unique():
                # get source end_time
                try:
                    source_end_time = datetime.strptime(
                        source_df.loc[(source_df['id'] == source_id) & (source_df['activity'] == int(source_end_act))]['start_time'].values[0], 
                        '%Y-%m-%d %H:%M:%S')
                # end activity not included in log, ignore it
                except IndexError:
                    continue
                # if time difference is less than 0
                if (sink_start_time-source_end_time).total_seconds() < 0:
                    time_diff[source_id] = 99999999
                else:
                    time_diff[source_id] = (sink_start_time-source_end_time).total_seconds()
        
            # select the one that have minimal time interval
            corr_case_with_sink = min(time_diff, key=time_diff.get)
            time_diff_matrix[f'{id}-{source}'] = corr_case_with_sink
            # make new merged df
            df_to_concat = source_df.loc[(source_df['id'] == corr_case_with_sink)]
            
            df_to_concat.loc[:, 'id'] = id
            sink_df = pd.concat( [sink_df,df_to_concat], ignore_index=True)
    return sink_df

def strategy_2(sep_df, known=['OEM']):
    # concatonated df
    fed_log = pd.concat(sep_df, ignore_index=True)

    resources = list(fed_log["resource"].unique())
    #TODO: create algorithm to find sink among all resources

    # after knowing the sink
    resources.remove(known[0])

    # Strategy 1 : start time of sink- end time of source
    # grab sink df
    sink_df = fed_log[fed_log['resource'].isin(known)]
    # get the start activity
    _,sink_end_act  = extract_start_end(sink_df)

    time_diff_matrix = {}
    for source in resources:
        # grab source df
        source_df = fed_log[fed_log['resource']==source]
        source_start_act, _ = extract_start_end(source_df)

        for id in sink_df['id'].unique():
            # record time difference
            time_diff ={}
            # get the start activity time of the right side (Sink side)
            try:
                sink_end_time = datetime.strptime(
                    sink_df.loc[(sink_df['id'] == id) & (sink_df['activity'] == int(sink_end_act))]['start_time'].values[0], 
                    '%Y-%m-%d %H:%M:%S')
            except IndexError:
                continue
            
            for source_id in source_df['id'].unique():
                # get source end_time
                try:
                    source_start_time = datetime.strptime(
                        source_df.loc[(source_df['id'] == source_id) & (source_df['activity'] == int(source_start_act))]['start_time'].values[0], 
                        '%Y-%m-%d %H:%M:%S')
                # end activity not included in log, ignore it
                except IndexError:
                    continue

                # if time difference is less than 0
                if (sink_end_time-source_start_time).total_seconds() < 0:
                    time_diff[source_id] = 99999999
                else:
                    time_diff[source_id] = (sink_end_time-source_start_time).total_seconds()
            
            # select the one that have minimal time interval
            corr_case_with_sink = min(time_diff, key=time_diff.get)
            time_diff_matrix[f'{id}-{source}'] = corr_case_with_sink
            # make new merged df
            df_to_concat = source_df.loc[(source_df['id'] == corr_case_with_sink)]
            
            df_to_concat.loc[:, 'id'] = id
            sink_df = pd.concat( [sink_df,df_to_concat], ignore_index=True)
    return sink_df

def strategy_3(sep_df, known=['OEM']): 
    # concatonated df
    fed_log = pd.concat(sep_df, ignore_index=True)

    resources = list(fed_log["resource"].unique())
    #TODO: create algorithm to find sink among all resources
    known = ['OEM']
    # after knowing the sink
    resources.remove(known[0])

    # Strategy 1 : start time of sink- end time of source
    # grab sink df
    sink_df = fed_log[fed_log['resource'].isin(known)]
    # get the start activity
    _,sink_end_act  = extract_start_end(sink_df)

    time_diff_matrix = {}
    for source in resources:
        # grab source df
        source_df = fed_log[fed_log['resource']==source]
        _, source_end_act = extract_start_end(source_df)

        for id in sink_df['id'].unique():
            # record time difference
            time_diff ={}
            # get the start activity time of the right side (Sink side)
            try:
                sink_end_time = datetime.strptime(
                    sink_df.loc[(sink_df['id'] == id) & (sink_df['activity'] == int(sink_end_act))]['start_time'].values[0], 
                    '%Y-%m-%d %H:%M:%S')
            except IndexError:
                continue
            
            for source_id in source_df['id'].unique():
                # get source end_time
                try:
                    source_end_time = datetime.strptime(
                        source_df.loc[(source_df['id'] == source_id) & (source_df['activity'] == int(source_end_act))]['start_time'].values[0], 
                        '%Y-%m-%d %H:%M:%S')
                # end activity not included in log, ignore it
                except IndexError:
                    continue

                # if time difference is less than 0
                if (sink_end_time-source_end_time).total_seconds() < 0:
                    time_diff[source_id] = 99999999
                else:
                    time_diff[source_id] = (sink_end_time-source_end_time).total_seconds()
            
            # select the one that have minimal time interval
            corr_case_with_sink = min(time_diff, key=time_diff.get)
            time_diff_matrix[f'{id}-{source}'] = corr_case_with_sink
            # make new merged df
            df_to_concat = source_df.loc[(source_df['id'] == corr_case_with_sink)]
            
            df_to_concat.loc[:, 'id'] = id
            sink_df = pd.concat( [sink_df,df_to_concat], ignore_index=True)