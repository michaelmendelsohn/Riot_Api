import pandas as pd


def prep_comparison_df(summoner_name, db_engine, minutes_to_analyse = [10,14], teammates_names = []):
    teammates_where_sql = "".join([f""" and lower(summonerNameList) LIKE '%{name}%' """ for name in [i.lower() for i in teammates_names]])

    query=f"""      
    SELECT
        det.matchId,
        det.championName,
        det.summonerName,
        rol.role,
        det.winFlag,
        det.teamName,
        case when summoner_basis.teamName = det.teamName then True else False end as teamMate,
        det.queueName,
        det.gameCreationDate,
        time.minute,
        time.participantId,
        time.totalGold,
        time.xp,
        time.minionsKilled,
        time.jungleMinionsKilled,
        time.minionsKilled + time.jungleMinionsKilled as totalCS,
        time.totalDamageDoneToChampions
    FROM riot_api.lol_match_timeline time
    INNER JOIN riot_api.lol_match_details det on time.matchId=det.matchId and time.participantId=det.participantId
    INNER JOIN riot_api.lol_match_roles rol on rol.matchId=det.matchId and rol.participantId=det.participantId
    INNER JOIN (select distinct matchid, teamName, group_concat(summonerName, '') as summonerNameList FROM riot_api.lol_match_details group by 1,2) summoner_basis
                on det.matchid = summoner_basis.matchid

    where rol.validRole = 1 and minute in {tuple(minutes_to_analyse)} and lower(summonerNameList) LIKE '%{summoner_name}%' {teammates_where_sql}
    """
    df = pd.read_sql(query, con=db_engine)
    df_home = df[df['teamMate']==True]
    df_away = df[df['teamMate']==False]

    df_compare = df_home.merge(df_away, how='inner', left_on = ['matchId','role','minute','queueName','gameCreationDate'],
                                                    right_on = ['matchId','role','minute','queueName','gameCreationDate'])
    df_compare['cs_diff'] = df_compare['totalCS_x'] - df_compare['totalCS_y']
    df_compare['xp_diff'] = df_compare['xp_x'] - df_compare['xp_y']
    df_compare['gold_diff'] = df_compare['totalGold_x'] - df_compare['totalGold_y']
    df_compare['dmg_diff'] = df_compare['totalDamageDoneToChampions_x'] - df_compare['totalDamageDoneToChampions_y']

    return df_compare