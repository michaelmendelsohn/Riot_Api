import mysql.connector
from sqlalchemy import create_engine
import constants
import pandas as pd
from datetime import datetime

def create_mysql_engine (user = 'root', password = 'iamgroot482', port = '3306',
                         host = '127.0.0.1', database = 'world'):

    url="mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(
                user, password, host, port, database
            )
    engine = create_engine(url)
    return engine



def get_matchlist_by_summoner_name(summonername, lolwatcher_connection, region='na1',):
    match_list=[]
    puuid = lolwatcher_connection.summoner.by_name(region, summonername)['puuid']
    while len(match_list)%100 == 0:
        match_list.extend(lolwatcher_connection.match.matchlist_by_puuid(region, puuid, start=len(match_list), count=100))
    return match_list

def stringify_json(stringy):
    return str(stringy).replace("'",'"').replace('True','true').replace('False','false')

def slurp_match_details(lol_watcher, match_id, region='na1'):
    match = lol_watcher.match.by_id(region, match_id)
    item_dict = constants.get_items(lol_watcher)
    spell_dict = constants.get_spells(lol_watcher)
    queues_dict = constants.get_queues()
    match_info = match['info']


    #remove the challenge and perk info, we don't care about that shit. keep everything else
    if 'perks' in match_info['participants'][0].keys():
        [match_info['participants'][i].pop('perks') for i in range(len(match_info['participants'])) ]
    if 'challenges' in match_info['participants'][0].keys():
        [match_info['participants'][i].pop('challenges') for i in range(len(match_info['participants'])) ]

    #Turn the data into a DF format
    df = pd.DataFrame(data=match_info['participants'], columns = list(match_info['participants'][0].keys()))
    df_length = len(df)

    # Add match_specific_data to each participant's row
    df['matchId'] = [match['metadata']['matchId'] for i in range(df_length)]
    df['region'] = [match['info']['platformId'] for i in range(df_length)]
    df['gameDurationSeconds'] = [match['info']['gameDuration'] for i in range(df_length)]
    match_time = datetime.fromtimestamp(int(match['info']['gameCreation']/1000))
    df['gameCreationDate'] = [match_time for i in range(df_length)]
    df['queueId'] = [match['info']['queueId'] for i in range(df_length)]

    # convert the Ids to Strings
    df['winFlag'] =  df['win'].apply(str).map({'True':1, 'False':0})
    df['teamName'] = df['teamId'].apply(str).map({'100':'Blue', '200':'Red'})
    df['item1Name'] = df['item1'].apply(str).map(item_dict)
    df['item2Name'] = df['item2'].apply(str).map(item_dict)
    df['item3Name'] = df['item3'].apply(str).map(item_dict)
    df['item4Name'] = df['item4'].apply(str).map(item_dict)
    df['item5Name'] = df['item5'].apply(str).map(item_dict)
    df['item6Name'] = df['item6'].apply(str).map(item_dict)
    df['summoner1Name'] = df['summoner1Id'].apply(str).map(spell_dict)
    df['summoner2Name'] = df['summoner2Id'].apply(str).map(spell_dict)
    df['queueName'] = df['queueId'].apply(str).map(queues_dict)

    return df

def slurp_match_timeline(lol_watcher, match_id, region='na1'):
    timeline = lol_watcher.match.timeline_by_match(region, match_id)
    match_timeline = lol_watcher.match.timeline_by_match(region, match_id)['info']
    l = len(match_timeline['frames'])

    data_fields = ['participantId','totalGold','level','xp','minionsKilled',
                   'jungleMinionsKilled','totalDamageDoneToChampions','posX','posY']
    col_names = ['matchId', 'minute'] + data_fields

    list_of_lists_timeline = []
    for i in range(l): #loops thru minutes
        for j in match_timeline['frames'][i]['participantFrames']: #loops thru champions
            champ_data=[match_id, i,
                            match_timeline['frames'][i]['participantFrames'][j][data_fields[0]],
                            match_timeline['frames'][i]['participantFrames'][j][data_fields[1]],
                            match_timeline['frames'][i]['participantFrames'][j][data_fields[2]],
                            match_timeline['frames'][i]['participantFrames'][j][data_fields[3]],
                            match_timeline['frames'][i]['participantFrames'][j][data_fields[4]],
                            match_timeline['frames'][i]['participantFrames'][j][data_fields[5]],
                            match_timeline['frames'][i]['participantFrames'][j]['damageStats'][data_fields[6]],
                            match_timeline['frames'][i]['participantFrames'][j]['position']['x'],
                            match_timeline['frames'][i]['participantFrames'][j]['position']['y']]
            list_of_lists_timeline.append(champ_data)

    match_timeline_df = pd.DataFrame(list_of_lists_timeline, columns = col_names)
    return match_timeline_df
