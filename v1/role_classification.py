import pandas as pd
import numpy as np
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from collections import Counter
from itertools import compress


top_area=Polygon([(0,16000),(0,6000),(3950,6000),(4800,11700),(11000,12500),(11000,16000)])
mid_area=Polygon([(6000,3700),(9500,5000),(11500,7500),(12500,10500),(10500,12000),(7000,10500),(5000,9000),(4200,6000)])
bot_area=Polygon([(16000,0),(6000,0),(6000,3500),(12000,4000),(13000,10000),(16000,10000)])


# Finds list of match_ids we still need to slurp for specified table_name
def find_to_classify(db_engine):
    
    query=f""" 
            SELECT 
                distinct a.matchId
            FROM  riot_api.lol_match_details a 
            inner join riot_api.lol_match_timeline b on a.matchid = b.matchid
            left join (select matchid, max(minute) as max_minute
						from lol_match_timeline 
                        group by 1) max_mins on a.matchid = max_mins.matchid
            where a.matchId not in (select distinct matchId from riot_api.lol_match_roles) and max_minute > 2
                  and queueName in ('5v5 Ranked Flex games', '5v5 Ranked Solo games',
                                    '5v5 Draft Pick games', '5v5 Blind Pick games',
                                    'Clash games') """
    df = pd.read_sql(query, con=db_engine)
    return list(df.matchId)

def classify_jungler(match_list_to_classify, db_engine):
    
    if len (match_list_to_classify)==0:
        print('empty list to classify')
        return None
    
    else:
        query=f"""  SELECT 
                        matchid,
                        participantid,
                        teamName,
                        neutralMinionsKilled as totaljungleMinionsKilled,
                        lower(concat( coalesce(summoner1Name, ''),  ' ', coalesce(summoner2Name,'') )) as summonerSpells
                    FROM riot_api.lol_match_details
                    where matchId in {tuple(match_list_to_classify)} """
        df = pd.read_sql(query, con=db_engine)

        df['jungle_flag'] = [True if 'smite' in spells else False  for spells in df.summonerSpells ]
        num_junglers = df.groupby(by=['matchid','teamName'])['jungle_flag'].sum().reset_index().rename(columns={'jungle_flag': 'num_junglers_by_team'},inplace=False)
        df2 = df.merge(num_junglers, left_on=['matchid','teamName'], right_on=['matchid','teamName'])

        matches_w_too_many_junglers = df2[(df2['jungle_flag']==True) & (df2['num_junglers_by_team'] > 1)][['matchid','teamName']]
        matches_w_too_many_junglers = matches_w_too_many_junglers.drop_duplicates()
        print (f'{len(matches_w_too_many_junglers)} matches with more than 2 smites:')

        for match_id, team  in zip(matches_w_too_many_junglers.matchid, matches_w_too_many_junglers.teamName) :
            for index in df2[(df2['matchid'] == match_id) & (df2['teamName'] == team) & (df2['jungle_flag']==True)].sort_values(by='totaljungleMinionsKilled', ascending=False).index[1:]:
                df2.loc[index,'jungle_flag'] = False

        return df2[['matchid','teamName','participantid','jungle_flag']]


def classify_support(match_list_to_classify, db_engine):
    current_supp_items = [ "Spellthief's Edge", 'Frostfang', 'Shard of True Ice',  'Steel Shoulderguards',
                           'Runesteel Spaulders',  'Pauldrons of Whiterock',  'Relic Shield', "Targon's Buckler",
                           'Bulwark of the Mountain',  'Spectral Sickle', 'Harrowing Crescent', 'Black Mist Scythe']
    past_supp_items = ['Face of the Mountain', "Frost Queen's Claim", 'Eye of the Oasis', 'Eye of the Equinox',
                       'Talisman of Ascension', "Targon's Brace", 'Ancient Coin', "Nomad's Medallion"]
    supp_items = [item.lower() for item in current_supp_items + past_supp_items]

    if len (match_list_to_classify)==0:
        print('empty list to classify')
        return None
    
    else:
        query=f"""  SELECT 
                            matchid,
                            participantid,
                            teamName,
                            totalMinionsKilled,
                            lower(concat( coalesce(item0Name,''),  ' ',coalesce(item1Name,''),  ' ', coalesce(item2Name,''),  ' ',
                                        coalesce(item3Name,''),  ' ', coalesce(item4Name,''),  ' ',coalesce(item5Name,''), ' ',
                                        coalesce(item6Name,''))) as itemNames
                        FROM riot_api.lol_match_details 
                        where matchId in {tuple(match_list_to_classify)} """
        df = pd.read_sql(query, con=db_engine)

        # Flag supports by the support item
        df['support_flag'] = [any(supp_item in all_items for supp_item in supp_items ) for all_items in df.itemNames ]

        # check for games with more than2 supports
        num_supports = df.groupby(by=['matchid','teamName'])['support_flag'].sum().reset_index().rename(columns={'support_flag': 'num_supports_by_team'},inplace=False)
        df2 = df.merge(num_supports, left_on=['matchid','teamName'], right_on=['matchid','teamName'])

        # For games with >2 supports, declassify the champion with more CS
        matches_w_too_many_supports = df2[(df2['support_flag']==True) & (df2['num_supports_by_team'] > 1)][['matchid','teamName']]
        matches_w_too_many_supports = matches_w_too_many_supports.drop_duplicates()
        print (f'{len(matches_w_too_many_supports)} matches with more than 2 champs with support items:')

        for match_id, team  in zip(matches_w_too_many_supports.matchid, matches_w_too_many_supports.teamName) :
            for index in df2[(df2['matchid'] == match_id) & (df2['teamName'] == team) & (df2['support_flag']==True)].sort_values(by='totalMinionsKilled', ascending=True).index[1:]:
                df2.loc[index,'support_flag'] = False

        return df2[['matchid','teamName','participantid','support_flag']]

# used with np.vectorize function in determine_roles classify_roles_by_position
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

def classify_roles_by_position(match_list_to_classify, db_engine, minutes_to_pull = [3,4,5,6,7,8]):
    
    if len (match_list_to_classify)==0:
        print('empty list to classify')
        return None
    else:
        query=f"""      SELECT 
                            matchid,
                            participantid,
                            minute,
                            posX,
                            posY
                        FROM riot_api.lol_match_timeline
                        where matchId in {tuple(match_list_to_classify)} and minute in {tuple(minutes_to_pull)} """
        df = pd.read_sql(query, con=db_engine)

        df['pos_lane'] = np.vectorize(classify_position)(df['posX'], df['posY'])
        position_flag_df=df.groupby(['matchid','participantid'])['pos_lane'].agg(lambda x:x.value_counts().index[0])
        return position_flag_df.reset_index()

# used with np.vectorize function in determine_roles
def define_role(jung, supp, poslane):
    if jung:
        return 'JUNGLE'
    elif supp:
        return 'SUPPORT'
    else:
        return poslane
    
def fillin_role(df):
    l = list(df['role'])

    role_dict = {
            'TOP':'TOP' in l,
            'JUNGLE' : 'JUNGLE' in l,
            'MIDDLE' : 'MIDDLE' in l,
            'BOTTOM' : 'BOTTOM' in l,
            'SUPPORT' : 'SUPPORT' in l
        }
        
    # If we're only missing one role, return that. If len <5 then there's a duplicate
    if len(Counter(l)) == 5 and role_dict['TOP'] + role_dict['JUNGLE'] + role_dict['MIDDLE'] + role_dict['BOTTOM'] + role_dict['SUPPORT']==4:
        mask = [not elem for elem in list(role_dict.values())]
        leftover = list(compress(list(role_dict.keys()), mask))[0]
        return leftover
    else:
        return 'Invalid'
        
def determine_roles(db_engine):

    to_classify = find_to_classify(db_engine)
    print(len(to_classify))
    if len(to_classify) == 0:
        print('No roles to classify')
    else:
        jung = classify_jungler(to_classify, db_engine)
        supp = classify_support(to_classify, db_engine)
        pos = classify_roles_by_position(to_classify, db_engine)

        df = jung.merge(supp[['matchid','participantid','support_flag']],
                            how='inner', left_on = ['matchid','participantid'],
                            right_on = ['matchid','participantid']).merge(pos[['matchid','participantid','pos_lane']],
                                                                            how='inner', left_on = ['matchid','participantid'],
                                                                            right_on = ['matchid','participantid'])
        df['role'] = np.vectorize(define_role)(df['jungle_flag'], df['support_flag'], df['pos_lane'])

        # For the matches with a None, see if the None is just the odd man out and there are 9 other unique Roles
        for match_id, teamName, index in zip(list(df[df['role']=='None'].matchid),
                                            list(df[df['role']=='None'].teamName),
                                            list(df[df['role']=='None'].index)):
            x = df[(df['matchid']==match_id) & (df['teamName']==teamName)]
            df.loc[index, 'role'] = fillin_role(x)

        invalid_matches = df.groupby('matchid')['role'].value_counts().reset_index()
        unique_invalid_matches = invalid_matches[invalid_matches['count']!=2].matchid.unique()
        df['validRole'] = [False if df.loc[i,'matchid'] in unique_invalid_matches else True for i in range(len(df)) ]
        df[['matchid','participantid','role','validRole']].to_sql(con=db_engine, name='lol_match_roles', if_exists='append', index = False)
        print( f"{len(df[['matchid','participantid','role','validRole']].matchid.unique())} match roles uploaded, including: ")
        print( f"{df[['matchid','participantid','role','validRole']].matchid.unique()[:10]}")
