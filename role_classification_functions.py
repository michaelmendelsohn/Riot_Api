import pandas as pd
import math
import numpy as np
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

#work done in summoner_rift_position_mapping notebook & locations .xls file

top_area=Polygon([(0,16000),(0,6000),(3950,6000),(4800,11700),(11000,12500),(11000,16000)])
mid_area=Polygon([(6000,3700),(9500,5000),(11500,7500),(12500,10500),(10500,12000),(7000,10500),(5000,9000),(4200,6000)])
bot_area=Polygon([(16000,0),(6000,0),(6000,3500),(12000,4000),(13000,10000),(16000,10000)])

def classify_position(x,y):
    pt = Point(x,y)
    if bot_area.contains(pt):
        return "BOTTOM"
    elif mid_area.contains(pt):
        return "MIDDLE"
    elif top_area.contains(pt):
        return "TOP"
    else:
        return "None"
    
    

def lane_by_position(df,mins=[3,4,5,6,7,8,9,10],top_anchor = [0,14600], mid_anchor = [7300,7300], bot_anchor = [14600,0]):

    df_mins=df[df['minute'].isin(mins)].copy()
    # df_mins['dist_top']=[math.dist([i,j],top_anchor) for i,j in zip(df_mins['posX'],df_mins['posY'])]
    # df_mins['dist_mid']=[math.dist([i,j],mid_anchor) for i,j in zip(df_mins['posX'],df_mins['posY'])]
    # df_mins['dist_bot']=[math.dist([i,j],bot_anchor) for i,j in zip(df_mins['posX'],df_mins['posY'])]
    # df_mins['min_dist']=df_mins[['dist_top','dist_mid','dist_bot']].min(axis=1)


    # lane_assignment=[]
    # #iterate through each row
    # for i in range(len(df_mins)):

    #     # for each row, check which distance is = to the minimum distances
    #     if df_mins.iloc[i].dist_bot==df_mins.iloc[i].min_dist:
    #         lane_assignment.append("BOTTOM")
            
    #     elif df_mins.iloc[i].dist_mid==df_mins.iloc[i].min_dist:
    #         lane_assignment.append("MIDDLE")
            
    #     elif df_mins.iloc[i].dist_top==df_mins.iloc[i].min_dist:
    #         lane_assignment.append("TOP")

    # #lane_assignment
    # df_mins['posLane']=lane_assignment

    #No longer want to use this distance function. instead, check the polygon of the ap that the point is within

    df_mins['posLane'] = np.vectorize(classify_position)(df_mins['posX'], df_mins['posY'])
    #make df smaller for group by
    df_mins2=df_mins[['summonerName','gameId','posLane']].copy()
    df_mins2 = df_mins2[df_mins2['posLane']!="None"]
    #take the mode of where the champion's calculated lane should be'
    position_flag_df=df_mins2.groupby(['summonerName','gameId']).agg(lambda x:x.value_counts().index[0])
    
    df_merge=df.merge(position_flag_df,on=['summonerName','gameId'])
    #want to return OG dataframe, but with another column of the new mapping
    return df_merge

def jungler_by_smite(df):
        # "Smite"

        df['all_spells'] = df.spell0.map(str) + df.spell1.map(str)

        jungleFlag=[]
        jungle=False
        for j in range(len(df)):
            
            if "Smite" in df.iloc[j]['all_spells']:
                jungle=True
            jungleFlag.append(jungle)
            jungle=False
        df['jungleFlag']=jungleFlag
        df.drop(['all_spells'], axis=1,inplace=True)

        #list of gameIds to loop thru
        gameIdList=list(df['gameId'].drop_duplicates())
        #check for games with more than 2 jungles
        for id in gameIdList:
           
            a=df[(df['gameId'] == id) & (df['minute'] == 4) &  (df['jungleFlag']==True)]

            for i in range(len(a)-2): #doesn't run if 2 junglers

                #sort junglers by jungle cs. chop off jungler   with least cs, until there are only 2 junglers
                a=df[(df['gameId'] == id) & (df['minute'] == 4) &  (df['jungleFlag']==True)]
                b=a[['jungleMinionsKilled','summonerName']].sort_values(['jungleMinionsKilled'],ascending=True)

                #replace jungle flag for all gameId, champion names for top index
                df.loc[(df['gameId'] == id) & (df['summonerName']== df.at[b.index[0],'summonerName']),'jungleFlag']=False

        return df

def support_by_item(df):

    new_supp_items=[ 'Frostfang','Shard of True Ice',  'Steel Shoulderguards',  'Runesteel Spaulders',  'Pauldrons of Whiterock',  'Relic Shield',
        "Targon's Buckler", 'Bulwark of the Mountain',  'Spectral Sickle', 'Harrowing Crescent', 'Black Mist Scythe']
    old_supp_items=['Shard of True Ice', 'Black Mist Scythe', 'Face of the Mountain', 'Frostfang', "Frost Queen's Claim", "Eye of the Oasis","Eye of the Equinox",
        "Talisman of Ascension", "Targon's Brace", "Ancient Coin", "Nomad's Medallion", "Spellthief's Edge"]
    supp_items=new_supp_items+old_supp_items
    #should only need to check 1 row per champion / game, so let's filter on only 1 minute
    df2=df[df['minute'].isin([1])].copy()
    df2['all_items'] = df2.item0.map(str) + df2.item1.map(str) + df2.item2.map(str) + df2.item3.map(str) + df2.item4.map(str) + df2.item5.map(str) 
    
    supportFlag=[]
    support=False
    for j in range(len(df2)):
        for si in supp_items:
            if si in df2.iloc[j]['all_items']:
                support=True
                
        supportFlag.append(support)
        support=False
    df2['supportFlag']=supportFlag
      
    #merge with original df, so that we just return the OG + the new flag
    #need to change this to go by summonerName instead of championName so that it works with blind pick games...
    df3=df.merge(df2[['summonerName','gameId','supportFlag']],on=['summonerName','gameId'])

    #loop thru all games
    gameIdList=list(df3['gameId'].drop_duplicates())
    for id in gameIdList:

        #for the game, grab all bot laners to check for cases where there isn't 2 supports & 2 adcs
        adc=df3[(df3['gameId'] == id) & (df3['minute'] == 4) &  (df3['supportFlag']==False) & (df3['posLane']=="BOTTOM") & (df3['jungleFlag']==False)]
        supp=df3[(df3['gameId'] == id) & (df3['minute'] == 4) &  (df3['supportFlag']==True) & (df3['jungleFlag']==False)]
        #print(len(adc), " ", len(supp))
        if (len(adc) + len(supp)==4): # Only do if there are 4 bot laners. I don't wanna mess with another weird case
            if len(adc)>2: #doesn't run if only 2 (or fewer) ADCs
                for i in range(len(adc)-2): 
                    #print("ADC Reclassified to support! Game ID: ", id)
                    #sort bot laners by minions killed. chop off ADC with least cs, until there are only 2 ADCs

                    adc=df3[(df3['gameId'] == id) & (df3['minute'] == 4) &  (df3['supportFlag']==False) & (df3['posLane']=="BOTTOM") & (df3['jungleFlag']==False)]
                    b=adc[['minionsKilled','summonerName']].sort_values(['minionsKilled'],ascending=True)
                    #print(adc)
                    #replace suppport flag for all gameId, champion names for top index. 
                    # e.g. someone was mislabeled as an adc when rlly they were the support due to selling supp item
                    print("gameID: ", id)
                    print("ADC LEN: ",len(adc))
                    print("b: ",b)
                    df3.loc[(df3['gameId'] == id) & (df3['summonerName']== df3.at[b.index[0],'summonerName']),'supportFlag']=True
            
            elif len(supp)>2: #doesn't run if only 2 (or fewer) Supports
                for i in range(len(supp)-2): 
                    #print("Support Reclassified to ADC! Game ID: ", id)
                    #sort bot laners by minions killed. chop off support  with most cs, until there are only 2 supports
                    supp=df3[(df3['gameId'] == id) & (df3['minute'] == 4) &  (df3['supportFlag']==True) & (df3['jungleFlag']==False)]
                    b=supp[['minionsKilled','summonerName']].sort_values(['minionsKilled'],ascending=False)

                    #replace suppport flag for all gameId, champion names for top index. 
                    #(someone was mislabeled as an support when rlly they were the adc due to both having a support item) (????? this probably won't ever happen)
                    print("gameID: ", id)
                    print("supp LEN: ",len(supp))
                    print("b: ",b)
                    df3.loc[(df3['gameId'] == id) & (df3['summonerName']== df3.at[b.index[0],'summonerName']),'supportFlag']=False
      
    return df3

def new_lane_classification(df,mins=[3,4,5,6,7,8,9,10],top_anchor = [1850,14150], mid_anchor = [8000,8000], bot_anchor = [14150,1850],cleaning=False):
    df_pos=lane_by_position(df, mins, top_anchor, mid_anchor, bot_anchor)

    df_jungle=jungler_by_smite(df_pos)
    df_supp=support_by_item(df_jungle)

    final_role_list=[]
    #change from needing to columns (role & lane) to just one column (TOP, MID , JUNGLE, ADC, SUPPORT)
    for j in range(len(df_supp)):
        #lane=df_jungle.iloc[j]['posLane']
        #role=df_jungle.iloc[j]['role']
        finalRole=df_supp.iloc[j]['posLane']
        if df_supp.iloc[j]['supportFlag'] == True:
            finalRole = "SUPPORT"
        if df_supp.iloc[j]['jungleFlag'] == True:
            finalRole="JUNGLE"
        if (finalRole == "BOTTOM"):
            finalRole = "BOT_CARRY"
        
        final_role_list.append(finalRole)

    df_supp['finalRole']=final_role_list

    #flag games with incorrect role classifcation (checks for 2 members in each role per game)
    if cleaning:
        role_df=df_supp[['gameId','minute','finalRole']]
        role_df_2=role_df[role_df['minute']==1][['gameId','finalRole']]
        role_df_2['cnt']=[1 for i in range(len(role_df_2))]
        role_df_3=role_df_2.groupby(['gameId','finalRole']).sum()
        #should be 2 roles for each position each game. "diff" column tells us if there how far off we are from 2
        role_df_3['diff']=abs(role_df_3['cnt']-2)
        #reset index so we can grab the gameId column after the groupby
        role_df_3.reset_index(inplace=True)
        #Get a list of just gameIds with a diff
        diff_df = role_df_3[['gameId','diff']].copy().drop_duplicates()
        diff_df_2=diff_df[diff_df['diff']>0].copy()
        #Assign value of True to games wiht a classification error
        diff_df_2['roleErrorFlag']=[True for i in range(len(diff_df_2))]
        diff_df_3=diff_df_2[['gameId','roleErrorFlag']].copy().drop_duplicates()
        
        df_final=df_supp.merge(diff_df_3,how='left',on=['gameId'])
        #this fills the null values from the roleErrorFlag merge to be False
        #Also fills in the item columns which has no items in it

        df_final.fillna(value=False, inplace=True)
        return df_final
    else:
        return df_supp 

def score_error(df):

    role_df=df[['gameId','minute','finalRole']]

    num_games=len(role_df[['gameId']].copy().drop_duplicates())

    role_df_2=role_df[role_df['minute']==1][['gameId','finalRole']]

    #arbitrary counter column with 1s in it, used to aggregate # of positions in each game
    role_df_2['cnt']=[1 for i in range(len(role_df_2))]
    role_df_3=role_df_2.groupby(['gameId','finalRole']).sum()
    #should be 2 roles for each position each game. "diff" column tells us if there how far off we are from 2
    role_df_3['diff']=abs(role_df_3['cnt']-2)
    #should be num_games * 5 rows in this table. a missing row means a "diff" of 2
    score=role_df_3['diff'].sum() + ((num_games*5) - len(role_df_3['diff']))*2
    return score
