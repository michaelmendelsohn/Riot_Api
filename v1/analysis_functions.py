import pandas as pd
from collections import Counter

def prep_comparison_df(summoner_name, db_engine, minutes_to_analyse = [10,14], teammates_names = []):
    #teammates_where_sql = "".join([f""" and lower(summonerNameList) LIKE '%{name}%' """ for name in [i.lower() for i in teammates_names]])
    minutes_where_sql = ",".join([str(i) for i in minutes_to_analyse])
    query=f"""      
    SELECT
        det.matchId,
        det.championName,
        lower(det.summonerName) as summonerName,
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

    where rol.validRole = 1 and minute in ({minutes_where_sql}) and lower(summonerNameList) LIKE '%{summoner_name.lower()}%' 
    """ # {teammates_where_sql}
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

def stats_at_min(summoner_name, role, db_engine, minutes_to_analyse =[14], teammates_names=[],
                 beg_timestamp= pd.Timestamp('2018-01-01 15:48:49'), end_timestamp= pd.Timestamp('2025-12-31 15:48:49') ):
    summoner_name = summoner_name.lower()
    df = prep_comparison_df(summoner_name, db_engine, minutes_to_analyse = minutes_to_analyse, teammates_names = teammates_names)
    cols=['role','championName_x','championName_y','winFlag_x','minute','cs_diff','xp_diff','gold_diff','dmg_diff']
    if role == 'ALL ROLES':
        mid_df_10 = df[(df.summonerName_x==summoner_name) &
                    (df.gameCreationDate > beg_timestamp) &
                    (df.gameCreationDate < end_timestamp)][cols]
    else:
        mid_df_10 = df[(df.summonerName_x==summoner_name) & (df.role==role) &
                    (df.gameCreationDate > beg_timestamp) &
                    (df.gameCreationDate < end_timestamp)][cols]
    agg_types={'championName_y':'count','winFlag_x':'mean', 'cs_diff':'mean','xp_diff':'mean', 'gold_diff':'mean', 'dmg_diff':'mean' }
    rename_dict = {'championName_y':'Games', 'winFlag_x':'Winrate', 'cs_diff':'CS_Diff@14', 'xp_diff':'XP_Diff@14', 'gold_diff':'Gold_Diff@14', 'dmg_diff':'DMG_Diff@14'}
    x=mid_df_10.groupby('championName_x', as_index=False,).agg(agg_types).rename(columns=rename_dict).sort_values(by='Games', ascending=False)
    format_dict={'Game': "{:.0f}",
                    'Winrate': "{:.0%}",
                    'CS_Diff@14': "{:.0f}",
                    'XP_Diff@14': "{:.0f}",
                    'Gold_Diff@14': "{:.0f}",
                    'DMG_Diff@14': "{:.0f}"}
    return x.style.format(format_dict)

def stats_at_min_with_teammates(main_summoner_name, role, db_engine, teammates_dict, minutes_to_analyse =[14],
                 beg_timestamp= pd.Timestamp('2018-01-01 15:48:49'), end_timestamp= pd.Timestamp('2025-12-31 15:48:49') ):
    df = prep_comparison_df(main_summoner_name, db_engine, minutes_to_analyse = minutes_to_analyse) 
    cols=['role','championName_x','championName_y','winFlag_x','minute','cs_diff','xp_diff','gold_diff','dmg_diff']

    # Let's say we're looking for 3 summoners. There will be 1 match_id  for each  correct summoner_name / role combo.
    # So then we need to select only the match_ids with 3 instances.
    match_id_mask = [ m_id for m_id, summ_name, role in zip(df.matchId, df.summonerName_x, df.role) 
                        if summ_name in teammates_dict.keys() and (role == teammates_dict[summ_name] or teammates_dict[summ_name] == 'ALL ROLES') ]

    val_counts = Counter(match_id_mask)
    valid_matchlist = [m_id for m_id in val_counts if val_counts[m_id]==len(teammates_dict.keys())]
    df = df[df['matchId'].isin(valid_matchlist)]

    processed_df = df[(df.summonerName_x == main_summoner_name) &
                (df.gameCreationDate > beg_timestamp) &
                (df.gameCreationDate < end_timestamp)][cols]

    agg_types={'championName_y':'count','winFlag_x':'mean', 'cs_diff':'mean','xp_diff':'mean', 'gold_diff':'mean', 'dmg_diff':'mean' }
    rename_dict = {'championName_y':'Games', 'winFlag_x':'Winrate', 'cs_diff':'CS_Diff@14', 'xp_diff':'XP_Diff@14', 'gold_diff':'Gold_Diff@14', 'dmg_diff':'DMG_Diff@14'}
    aggregated_df=processed_df.groupby('championName_x', as_index=False,).agg(agg_types).rename(columns=rename_dict).sort_values(by='Games', ascending=False)
    format_dict={'Game': "{:.0f}",
                    'Winrate': "{:.0%}",
                    'CS_Diff@14': "{:.0f}",
                    'XP_Diff@14': "{:.0f}",
                    'Gold_Diff@14': "{:.0f}",
                    'DMG_Diff@14': "{:.0f}"}
    return aggregated_df.style.format(format_dict)
