import mysql.connector
from sqlalchemy import create_engine
import constants
import pandas as pd
from datetime import datetime
import concurrent.futures

# Create mysql engine to be used to upload to my local mysql DB
def create_mysql_engine (user = 'root', password = 'iamgroot482', port = '3306',
                         host = '127.0.0.1', database = 'riot_api'):
    #host = '192.168.50.32'
    url="mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(
                user, password, host, port, database
            )
    engine = create_engine(url)
    return engine

# Get a list of all matches in riot API for a summoner. 
# Riot API caps at 100 matches, so this runs multiple times to get the full list
def get_matchlist_by_summoner_name(summonername, lolwatcher_connection, region='na1',):
    match_list=[]
    puuid = lolwatcher_connection.summoner.by_name(region, summonername)['puuid']
    while len(match_list)%100 == 0:
        match_list.extend(lolwatcher_connection.match.matchlist_by_puuid(region, puuid, start=len(match_list), count=100))
    return match_list

# Helper function useful when printing strings of json and we want it to be ingestable in python
def stringify_json(stringy):
    return str(stringy).replace("'",'"').replace('True','true').replace('False','false')

# Create DataFrame of Match Details Data for specified Match Id
def slurp_match_details(lol_watcher, match_id, region='na1'):
    match = lol_watcher.match.by_id(region, match_id)
    match_info = match['info']

    item_dict = constants.get_items(lol_watcher)
    spell_dict = constants.get_spells(lol_watcher)
    queues_dict = constants.get_queues()
    
    # Check for empty game
    if match_info['gameDuration'] == 0:
        print ('Empty Game. Returning None for Match Details.')
        return None
    # if not empty game
    else:
        #remove the challenge and perk info, we don't care about that shit. keep everything else
        #if 'perks' in match_info['participants'][0].keys():
        [match_info['participants'][i].pop('perks') for i in range(len(match_info['participants'])) if 'perks' in match_info['participants'][i].keys() ]
        #if 'challenges' in match_info['participants'][0].keys():
        [match_info['participants'][i].pop('challenges') for i in range(len(match_info['participants'])) if 'challenges' in match_info['participants'][i].keys()]

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
        df['item0Name'] = df['item0'].apply(str).map(item_dict)
        df['item1Name'] = df['item1'].apply(str).map(item_dict)
        df['item2Name'] = df['item2'].apply(str).map(item_dict)
        df['item3Name'] = df['item3'].apply(str).map(item_dict)
        df['item4Name'] = df['item4'].apply(str).map(item_dict)
        df['item5Name'] = df['item5'].apply(str).map(item_dict)
        df['item6Name'] = df['item6'].apply(str).map(item_dict)
        df['summoner1Name'] = df['summoner1Id'].apply(str).map(spell_dict)
        df['summoner2Name'] = df['summoner2Id'].apply(str).map(spell_dict)
        df['queueName'] = df['queueId'].apply(str).map(queues_dict)
        
        df.drop(columns=['allInPings', 'assistMePings', 'baitPings', 'basicPings', 'bountyLevel', 'championTransform', 'commandPings',
                         'dangerPings', 'eligibleForProgression', 'enemyMissingPings', 'enemyVisionPings','getBackPings', 'holdPings',
                         'needVisionPings', 'nexusKills', 'nexusLost', 'nexusTakedowns', 'onMyWayPings', 'visionClearedPings'], inplace=True)
        return df

# Create DataFrame of Match Timeline Data for specified Match Id
def slurp_match_timeline(lol_watcher, match_id, region='na1'):
    timeline = lol_watcher.match.timeline_by_match(region, match_id)
    match_timeline = timeline['info']
    l = len(match_timeline['frames'])

    # Check for Empty Game
    if l == 1:
        print ('Empty Game. Returning None for Match Timeline.')
        return None
    
    # if not empty game
    else:
        data_fields = ['participantId','totalGold','level','xp','minionsKilled',
                    'jungleMinionsKilled','totalDamageDoneToChampions','posX','posY']
        col_names = ['matchId', 'minute'] + data_fields

        # Nested List Comprehension should be faster than previous nested for loop implementation

        list_of_lists_timeline = [[match_id, i,
                                match_timeline['frames'][i]['participantFrames'][j][data_fields[0]],
                                match_timeline['frames'][i]['participantFrames'][j][data_fields[1]],
                                match_timeline['frames'][i]['participantFrames'][j][data_fields[2]],
                                match_timeline['frames'][i]['participantFrames'][j][data_fields[3]],
                                match_timeline['frames'][i]['participantFrames'][j][data_fields[4]],
                                match_timeline['frames'][i]['participantFrames'][j][data_fields[5]],
                                match_timeline['frames'][i]['participantFrames'][j]['damageStats'][data_fields[6]],
                                match_timeline['frames'][i]['participantFrames'][j]['position']['x'],
                                match_timeline['frames'][i]['participantFrames'][j]['position']['y']
                ]  for i in range(l) if match_timeline['frames'][i]['participantFrames'] is not None for j in match_timeline['frames'][i]['participantFrames']]


        match_timeline_df = pd.DataFrame(list_of_lists_timeline, columns = col_names)
        match_timeline_df = match_timeline_df[match_timeline_df.minute.isin([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,20,25,30,35,40,45,50,55,60,65,70])]
        return match_timeline_df

# Calls appropriate slurp function for either match_details or match_timeline
def slurp_data(lol_watcher, match_id, table_name, region='na1'):
   
    if table_name == 'lol_match_details':
        return slurp_match_details(lol_watcher, match_id, region='na1')
    elif table_name == 'lol_match_timeline':
        return slurp_match_timeline(lol_watcher, match_id, region='na1')
    else:
        print("invalid table_name provided. Current optinos are 'lol_match_details', or 'lol_match_timeline'.")
        return None

# Finds list of match_ids we still need to slurp for specified table_name
def find_to_slurp(summoner_name, lol_watcher, table_name, db_engine, region = 'na1'):
    
    match_list = get_matchlist_by_summoner_name(summoner_name, lol_watcher, region = region)
    match_id_df = pd.read_sql(f'SELECT distinct matchid FROM {table_name}', con=db_engine)
    already_slurped_match_list = list(match_id_df.matchid)
    
    return [match for match in match_list if match not in already_slurped_match_list]

## This is the mother function you run.
## summoner_name  (string) - Name of summoner to pull data for
## lol_watcher (object) - Authenticated lol_watcher object
## table_name (string) - Table we want to populate. either 'lol_match_timeline' or 'lol_match_details'
## db_engine (sqlAlchemy Engine Object)- Connection to mySQLDatabase from sqlAlchemy's create_engine.
##                                        Use create_mysql_engine helper function with default settings.
## region (string) - Region account is in. Defaults to NA
def collect_riot_api_data(summoner_name, lol_watcher, table_name, db_engine, region='na1', num_worker_threads=4, match_upload_limit=20):
    # First We run find_to_slurp to avoid pulling data for matches we have already pulled
    to_slurp = find_to_slurp(summoner_name, lol_watcher, table_name, db_engine)
    print (f'{len(to_slurp)} matches to slurp!')
    # Next we use slurp_data to pull the data from Riot API
    # Set Up multiprocessing for this later

    def worker (match_id):
        df_to_upload = slurp_data(lol_watcher, match_id, table_name, region=region)
        if df_to_upload is not None:
            df_to_upload.to_sql(con=db_engine, name=table_name, if_exists='append', index = False)
            print(f"Uploaded match id {match_id} {table_name} data.")
        else: 
            print(f'Match id {match_id} returns None.')

    with concurrent.futures.ThreadPoolExecutor(max_workers = num_worker_threads) as executor:
        to_slurp = to_slurp[:match_upload_limit]
        futures = [ executor.submit(worker, match_id) for match_id in to_slurp ]

        try: 
            for future in concurrent.futures.as_completed(futures):
                future.result()
        except Exception as e:
            print(f"Exception {e} thrown. Cancelling futures.")