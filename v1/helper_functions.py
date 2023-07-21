import mysql.connector
from sqlalchemy import create_engine

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
