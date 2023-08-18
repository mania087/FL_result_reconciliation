import random

def federate_log(df,
                 list_of_act:list = [[100,200,300], [400,500,600], [700,800,900,1000]],
                 list_of_resource:list = ['Tier 2','Tier 1', 'OEM']
                 ):
    # remove id
    test = df.copy()
    test = test.drop('id', axis=1)
    # change box
    test['box'] = test.apply(lambda row: row.session + '_' + str(row.box), axis=1)
    # remove session
    test = test.drop('session', axis=1)

    # separate DF
    separated_df = []
    base = 0
    for index,acts in enumerate(list_of_act):
        inst_df = test.loc[test['activity'].isin(acts)]

        # generate ID
        #inst_df['id'] = inst_df.apply(lambda row: str(row.box), axis=1)

        # list case box
        for cases in inst_df['box'].unique():
            # append with unique_integer
            rand_int = random.randint(base,base+10000)
            inst_df.loc[inst_df['box'] == cases, 'id'] = cases + '_' + inst_df['resource'] + '_' + str(rand_int)

        #change resource
        inst_df.loc[:,'resource'] = list_of_resource[index]

        inst_df.drop('box',axis=1,inplace=True)
        separated_df.append(inst_df.reset_index())

        base += 10000
    
    return separated_df

