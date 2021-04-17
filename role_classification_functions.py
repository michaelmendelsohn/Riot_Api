import pandas as pd
import math

def lane_by_position(df,mins=[4,5,6,7,8],top_anchor = [0,14600], mid_anchor = [7300,7300], bot_anchor = [14600,0]):

    df_mins=df[df['minute'].isin(mins)].copy()
    df_mins['dist_top']=[math.dist([i,j],top_anchor) for i,j in zip(df_mins['pos_x'],df_mins['pos_y'])]
    df_mins['dist_mid']=[math.dist([i,j],mid_anchor) for i,j in zip(df_mins['pos_x'],df_mins['pos_y'])]
    df_mins['dist_bot']=[math.dist([i,j],bot_anchor) for i,j in zip(df_mins['pos_x'],df_mins['pos_y'])]
    df_mins['min_dist']=df_mins[['dist_top','dist_mid','dist_bot']].min(axis=1)

    lane_assignment=[]
    #iterate through each row
    for i in range(len(df_mins)):

        # for each row, check which distance is = to the minimum distances
        if df_mins.iloc[i].dist_bot==df_mins.iloc[i].min_dist:
            lane_assignment.append("BOTTOM")
            
        elif df_mins.iloc[i].dist_mid==df_mins.iloc[i].min_dist:
            lane_assignment.append("MIDDLE")
            
        elif df_mins.iloc[i].dist_top==df_mins.iloc[i].min_dist:
            lane_assignment.append("TOP")

    #lane_assignment
    df_mins['pos_lane']=lane_assignment
    df_mins.head(15)
    
    #make df smaller for group by
    df_mins2=df_mins[['champion_name','gameId','pos_lane']].copy()
    #take the mode of where the champion's calculated lane should be'
    position_flag_df=df_mins2.groupby(['champion_name','gameId']).agg(lambda x:x.value_counts().index[0])
    
    df_merge=df.merge(position_flag_df,on=['champion_name','gameId'])
    #want to return OG dataframe, but with another column of the new mapping
    return df_merge

def jungler_by_smite(df):

        #prep_q3 = "SELECT distinct champion_name, gameId, spell0, spell1 FROM df_soup where queue not in (\"5v5 ARAM games\", \"URF games\")"
        #prep3 = psql.sqldf(prep_q3, locals())
        #prep3
        # "Smite"

        df['all_spells'] = df.spell0.map(str) + df.spell1.map(str)

        jungle_flag=[]
        jungle=False
        for j in range(len(df)):
            
            if "Smite" in df.iloc[j]['all_spells']:
                jungle=True
            jungle_flag.append(jungle)
            jungle=False
        df['jungle_flag']=jungle_flag
        df.drop(['all_spells'], axis=1,inplace=True)

        #list of gameIds to loop thru
        gameIdList=list(df['gameId'].drop_duplicates())
        #check for games with more than 2 jungles
        for id in gameIdList:
           
            a=df[(df['gameId'] == id) & (df['minute'] == 4) &  (df['jungle_flag']==True)]

            for i in range(len(a)-2): #doesn't run if 2 junglers

                #sort junglers by jungle cs. chop off jungler   with least cs, until there are only 2 junglers
                a=df[(df['gameId'] == id) & (df['minute'] == 4) &  (df['jungle_flag']==True)]
                b=a[['jungleMinionsKilled','champion_name']].sort_values(['jungleMinionsKilled'],ascending=True)

                #replace jungle flag for all gameId, champion names for top index
                df.loc[(df['gameId'] == id) & (df['champion_name']== df.at[b.index[0],'champion_name']),'jungle_flag']=False

        return df

def support_by_item(df):
      #prep_q2 = "SELECT distinct champion_name, gameId, item0, item1, item2, item3, item4, item5 FROM df_soup where queue not in (\"5v5 ARAM games\", \"URF games\")"
      #prep2 = psql.sqldf(prep_q2, locals())

      supp_items=[ 'Frostfang','Shard of True Ice',  'Steel Shoulderguards',  'Runesteel Spaulders',  'Pauldrons of Whiterock',  'Relic Shield',
        "Targon's Buckler", 'Bulwark of the Mountain',  'Spectral Sickle', 'Harrowing Crescent', 'Black Mist Scythe']
      #should only need to check 1 row per champion / game, so let's filter on only 1 minute
      df2=df[df['minute'].isin([1])].copy()
      df2['all_items'] = df2.item0.map(str) + df2.item1.map(str) + df2.item2.map(str) + df2.item3.map(str) + df2.item4.map(str) + df2.item5.map(str) 

      support_flag=[]
      support=False
      for j in range(len(df2)):
          for si in supp_items:
              if si in df2.iloc[j]['all_items']:
                  support=True
          support_flag.append(support)
          support=False
      df2['support_flag']=support_flag
      
      #merge with original df, so that we just return the OG + the new flag
      df3=df.merge(df2[['champion_name','gameId','support_flag']],on=['champion_name','gameId'])

      #loop thru all games
      gameIdList=list(df3['gameId'].drop_duplicates())
      for id in gameIdList:

        #for the game, grab all bot laners to check for cases where there isn't 2 supports & 2 adcs
        adc=df3[(df3['gameId'] == id) & (df3['minute'] == 4) &  (df3['support_flag']==False) & (df3['pos_lane']=="BOTTOM") & (df3['jungle_flag']==False)]
        supp=df3[(df3['gameId'] == id) & (df3['minute'] == 4) &  (df3['support_flag']==True) & (df3['jungle_flag']==False)]
        #print(len(adc), " ", len(supp))
        if (len(adc) + len(supp)==4): # Only do if there are 4 bot laners. I don't wanna mess with another weird case
          if len(adc)>2: #doesn't run if only 2 (or fewer) ADCs
            for i in range(len(adc)-2): 
                #print("ADC Reclassified to support! Game ID: ", id)
                #sort bot laners by minions killed. chop off ADC with least cs, until there are only 2 ADCs

                adc=df3[(df3['gameId'] == id) & (df['minute'] == 4) &  (df3['support_flag']==False) & (df3['pos_lane']=="BOTTOM") & (df3['jungle_flag']==False)]
                b=adc[['minionsKilled','champion_name']].sort_values(['minionsKilled'],ascending=True)
                #print(adc)
                #replace suppport flag for all gameId, champion names for top index. 
                # e.g. someone was mislabeled as an adc when rlly they were the support due to selling supp item
                df3.loc[(df3['gameId'] == id) & (df3['champion_name']== df3.at[b.index[0],'champion_name']),'support_flag']=True
         
          elif len(supp)>2: #doesn't run if only 2 (or fewer) Supports
            for i in range(len(supp)-2): 
                #print("Support Reclassified to ADC! Game ID: ", id)
                #sort bot laners by minions killed. chop off support  with most cs, until there are only 2 supports
                supp=df3[(df3['gameId'] == id) & (df['minute'] == 4) &  (df3['support_flag']==True) & (df3['jungle_flag']==False)]
                b=supp[['minionsKilled','champion_name']].sort_values(['minionsKilled'],ascending=False)

                #replace suppport flag for all gameId, champion names for top index. 
                #(someone was mislabeled as an support when rlly they were the adc due to both having a support item) (????? this probably won't ever happen)
                df3.loc[(df3['gameId'] == id) & (df3['champion_name']== df3.at[b.index[0],'champion_name']),'support_flag']=False
      
      return df3

def new_lane_classification(df,mins=[3,4,5,6,7,8,9,10],top_anchor = [1850,14150], mid_anchor = [8000,8000], bot_anchor = [14150,1850],cleaning=False):
    df_pos=lane_by_position(df, mins, top_anchor, mid_anchor, bot_anchor)

    df_jungle=jungler_by_smite(df_pos)
    df_supp=support_by_item(df_jungle)

    final_role_list=[]
    #change from needing to columns (role & lane) to just one column (TOP, MID , JUNGLE, ADC, SUPPORT)
    for j in range(len(df_supp)):
        #lane=df_jungle.iloc[j]['pos_lane']
        #role=df_jungle.iloc[j]['role']
        final_role=df_supp.iloc[j]['pos_lane']
        if df_supp.iloc[j]['support_flag'] == True:
            final_role = "SUPPORT"
        if df_supp.iloc[j]['jungle_flag'] == True:
            final_role="JUNGLE"
        if (final_role == "BOTTOM"):
            final_role = "BOT_CARRY"
        
        final_role_list.append(final_role)

    df_supp['final_role']=final_role_list

    #flag games with incorrect role classifcation (checks for 2 members in each role per game)
    if cleaning:
        role_df=df_supp[['gameId','minute','final_role']]
        role_df_2=role_df[role_df['minute']==1][['gameId','final_role']]
        role_df_2['cnt']=[1 for i in range(len(role_df_2))]
        role_df_3=role_df_2.groupby(['gameId','final_role']).sum()
        #should be 2 roles for each position each game. "diff" column tells us if there how far off we are from 2
        role_df_3['diff']=abs(role_df_3['cnt']-2)
        #reset index so we can grab the gameId column after the groupby
        role_df_3.reset_index(inplace=True)
        #Get a list of just gameIds with a diff
        diff_df = role_df_3[['gameId','diff']].copy().drop_duplicates()
        diff_df_2=diff_df[diff_df['diff']>0].copy()
        #Assign value of True to games wiht a classification error
        diff_df_2['role_error_flag']=[True for i in range(len(diff_df_2))]
        diff_df_3=diff_df_2[['gameId','role_error_flag']].copy().drop_duplicates()
        
        df_final=df_supp.merge(diff_df_3,how='left',on=['gameId'])
        #this fills the null values from the role_error_flag merge to be False
        #Also fills in the item columns which has no items in it

        df_final.fillna(value=False, inplace=True)
        return df_final
    else:
        return df_supp 

def score_error(df):

    role_df=df[['gameId','minute','final_role']]

    num_games=len(role_df[['gameId']].copy().drop_duplicates())

    role_df_2=role_df[role_df['minute']==1][['gameId','final_role']]

    #arbitrary counter column with 1s in it, used to aggregate # of positions in each game
    role_df_2['cnt']=[1 for i in range(len(role_df_2))]
    role_df_3=role_df_2.groupby(['gameId','final_role']).sum()
    #should be 2 roles for each position each game. "diff" column tells us if there how far off we are from 2
    role_df_3['diff']=abs(role_df_3['cnt']-2)
    #should be num_games * 5 rows in this table. a missing row means a "diff" of 2
    score=role_df_3['diff'].sum() + ((num_games*5) - len(role_df_3['diff']))*2
    return score
