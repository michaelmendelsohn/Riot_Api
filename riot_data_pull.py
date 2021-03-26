from riotwatcher import LolWatcher, ApiError
from general import api_key
import pandas as pd

# golbal variables

watcher = LolWatcher(api_key)
my_region = 'na1'

me = watcher.summoner.by_name(my_region, 'Souporsecret')
#print(me)

my_ranked_stats = watcher.league.by_summoner(my_region, me['id'])
#print(my_ranked_stats)
my_matches = watcher.match.matchlist_by_account(my_region, me['accountId'])
# fetch last match detail
last_match = my_matches['matches'][0]
match_detail = watcher.match.by_id(my_region, last_match['gameId'])
participants = []
for row in match_detail['participants']:
    print(row.keys())
    participants_row = {}
    participants_row['participantId'] = row['participantId']
    participants_row['champion'] = row['championId']
    participants_row['spell1'] = row['spell1Id']
    participants_row['spell2'] = row['spell2Id']
    participants_row['win'] = row['stats']['win']
    participants_row['kills'] = row['stats']['kills']
    participants_row['deaths'] = row['stats']['deaths']
    participants_row['assists'] = row['stats']['assists']
    participants_row['totalDamageDealt'] = row['stats']['totalDamageDealt']
    participants_row['goldEarned'] = row['stats']['goldEarned']
    participants_row['champLevel'] = row['stats']['champLevel']
    participants_row['totalMinionsKilled'] = row['stats']['totalMinionsKilled']
    participants_row['role'] = row['timeline']['role']
    participants_row['lane'] = row['timeline']['lane']
    participants.append(participants_row)
match_details_df = pd.DataFrame(participants)
match_details_df2=match_details_df[['participantId','champion','role','lane']].copy()

#----------------------------------------------------
#connect summoner names to participant id
sumnames_index=['participantId','summonerName']
summmonersNamesDF=pd.DataFrame(columns=sumnames_index)
for i in range(10):
    x=[(match_summoners['participantIdentities'][i]['participantId']),
                match_summoners['participantIdentities'][i]['player']['summonerName']]
    sum_names_series = pd.Series(x, index = sumnames_index)
    summmonersNamesDF=summmonersNamesDF.append(sum_names_series,ignore_index="True")
summmonersNamesDF

#----------------------------------------------------
#Grab gold data
match_timeline = watcher.match.timeline_by_match(my_region, last_match['gameId'])

l=len(match_timeline['frames'])
data_fields=['participantId','totalGold','level','xp','minionsKilled','jungleMinionsKilled']
golddf=pd.DataFrame(columns=['minute']+data_fields)

for i in range(l): #loops thru minutes
   
    for j in match_timeline['frames'][i]['participantFrames']:#loops thru champions
        champ_data=[i,  match_timeline['frames'][i]['participantFrames'][j][data_fields[0]],
                        match_timeline['frames'][i]['participantFrames'][j][data_fields[1]],
                        match_timeline['frames'][i]['participantFrames'][j][data_fields[2]],
                        match_timeline['frames'][i]['participantFrames'][j][data_fields[3]],
                        match_timeline['frames'][i]['participantFrames'][j][data_fields[4]],
                        match_timeline['frames'][i]['participantFrames'][j][data_fields[5]]]
       
        champ_data_series = pd.Series(champ_data, index = golddf.columns)
        #print(champ_data_series)
        golddf=golddf.append(champ_data_series,ignore_index="True")
golddf.head(5)

#----------------------------------------------------    
#get champion name -> champion id mapping
# check league's latest version
latest = watcher.data_dragon.versions_for_region(my_region)['n']['champion']
# Lets get some champions static information
static_champ_list = watcher.data_dragon.champions(latest, False, 'en_US')

champ_dict = {} 
static_champ_list['data']['Aatrox']['key']
for champ in static_champ_list['data'].keys(): # iterates thru champ names
   champ_dict[static_champ_list['data'][champ]['key']]=champ
#print(champ_dict)

#----------------------------------------------------   
# Merge everything 
merged_1 = golddf.merge(match_details_df2,how='left',on='participantId')
merged_2 = merged_1.merge(summmonersNamesDF,how='left',on='participantId')

merged_2['champion_name']=merged_2['champion'].apply(str).map(champ_dict)

merged_3=merged_2.sort_values(["champion_name",'minute'])
#merged_3.to_csv("Match_CSV.csv")


