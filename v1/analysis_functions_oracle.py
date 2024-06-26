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
        case when summoner_basis.teamName = det.teamName then 1 else 0 end as teamMate,
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
    FROM lol_match_timeline time
    INNER JOIN lol_match_details det on time.matchId=det.matchId and time.participantId=det.participantId
    INNER JOIN lol_match_roles rol on rol.matchId=det.matchId and rol.participantId=det.participantId
    INNER JOIN (select distinct matchid, teamName, LISTAGG(summonerName, '') as summonerNameList FROM lol_match_details group by matchid, teamName) summoner_basis
                on det.matchid = summoner_basis.matchid

    where rol.validRole = 1 and minute in ({minutes_where_sql}) and lower(summonerNameList) LIKE '%{summoner_name.lower()}%' 
    FETCH NEXT 30 ROWS ONLY
    """ # {teammates_where_sql}
    df = pd.read_sql(query, con=db_engine)
    df_home = df[df['TEAMMATE']==1]
    df_away = df[df['TEAMMATE']==0]

    df_compare = df_home.merge(df_away, how='inner', left_on = ['MATCHID','ROLE','MINUTE','QUEUENAME','GAMECREATIONDATE'],
                                                    right_on = ['MATCHID','ROLE','MINUTE','QUEUENAME','GAMECREATIONDATE'])
    
    df_compare['CS_DIFF'] = df_compare['TOTALCS_x'] - df_compare['TOTALCS_y']
    df_compare['XP_DIFF'] = df_compare['XP_x'] - df_compare['XP_y']
    df_compare['GOLD_DIFF'] = df_compare['TOTALGOLD_x'] - df_compare['TOTALGOLD_y']
    df_compare['DMG_DIFF'] = df_compare['TOTALDAMAGEDONETOCHAMPIONS_x'] - df_compare['TOTALDAMAGEDONETOCHAMPIONS_y']

    return df_compare

def stats_at_min(summoner_name, role, db_engine, minutes_to_analyse =[14], teammates_names=[],
                 beg_timestamp= pd.Timestamp('2018-01-01 15:48:49'), end_timestamp= pd.Timestamp('2025-12-31 15:48:49'), return_type = 'styled' ):
    summoner_name = summoner_name.lower()
    df = prep_comparison_df(summoner_name, db_engine, minutes_to_analyse = minutes_to_analyse, teammates_names = teammates_names)
    cols=['ROLE','CHAMPIONNAME_x','CHAMPIONNAME_y','WINFLAG_x','MINUTE','CS_DIFF','XP_DIFF','GOLD_DIFF','DMG_DIFF']
    if role == 'ALL ROLES':
        mid_df_10 = df[(df.SUMMONERNAME_x==summoner_name) &
                    (df.GAMECREATIONDATE > beg_timestamp) &
                    (df.GAMECREATIONDATE < end_timestamp)][cols]
    else:
        mid_df_10 = df[(df.SUMMONERNAME_x==summoner_name) & (df.ROLE==role) &
                    (df.GAMECREATIONDATE > beg_timestamp) &
                    (df.GAMECREATIONDATE < end_timestamp)][cols]
    agg_types={'CHAMPIONNAME_y':'count','WINFLAG_x':'mean', 'CS_DIFF':'mean','XP_DIFF':'mean', 'GOLD_DIFF':'mean', 'DMG_DIFF':'mean' }
    rename_dict = {'CHAMPIONNAME_y':'Games', 'CHAMPIONNAME_x':'Champion_Name', 'WINFLAG_x':'Winrate', 'CS_DIFF':'CS_Diff@14', 'XP_DIFF':'XP_Diff@14', 'GOLD_DIFF':'Gold_Diff@14', 'DMG_DIFF':'DMG_Diff@14'}
    x=mid_df_10.groupby('CHAMPIONNAME_X', as_index=False,).agg(agg_types).rename(columns=rename_dict)
    n_games = x.Games.sum()
    total_row = ['Total', n_games,  x.Games.dot(x.Winrate)/n_games,
                                    x.Games.dot(x['CS_Diff@14'])/n_games,
                                    x.Games.dot(x['XP_Diff@14'])/n_games,
                                    x.Games.dot(x['Gold_Diff@14'])/n_games,
                                    x.Games.dot(x['DMG_Diff@14'])/n_games
                ]
    x.loc[len(x)] = total_row
    x = x.sort_values(by='Games', ascending=False).reset_index(drop=True)
    format_dict={'Game': "{:.0f}",
                    'Winrate': "{:.0%}",
                    'CS_Diff@14': "{:.0f}",
                    'XP_Diff@14': "{:.0f}",
                    'Gold_Diff@14': "{:.0f}",
                    'DMG_Diff@14': "{:.0f}"}
    if return_type == 'styled':
        return x.style.format(format_dict)
    else:
        return x

def stats_at_min_with_teammates(main_summoner_name, role, db_engine, teammates_dict, minutes_to_analyse =[14],
                 beg_timestamp= pd.Timestamp('2018-01-01 15:48:49'), end_timestamp= pd.Timestamp('2025-12-31 15:48:49'), 
                 agg_type='agg', return_type='styled' ):
    df = prep_comparison_df(main_summoner_name, db_engine, minutes_to_analyse = minutes_to_analyse) 
    cols=['GAMECREATIONDATE', 'ROLE','CHAMPIONNAME_x','CHAMPIONNAME_y','WINFLAG_x','MINUTE','CS_DIFF','XP_DIFF','GOLD_DIFF','DMG_DIFF']


    # Let's say we're looking for 3 summoners. There will be 1 match_id  for each  correct summoner_name / role combo.
    # So then we need to select only the match_ids with 3 instances.
    match_id_mask = [ m_id for m_id, summ_name, role in zip(df.MATCHID, df.SUMMONERNAME_x, df.ROLE) 
                        if summ_name in teammates_dict.keys() and (role == teammates_dict[summ_name] or teammates_dict[summ_name] == 'ALL ROLES') ]

    val_counts = Counter(match_id_mask)
    valid_matchlist = [m_id for m_id in val_counts if val_counts[m_id]==len(teammates_dict.keys())]
    df = df[df['MATCHID'].isin(valid_matchlist)]

    if role == 'ALL ROLES':
        processed_df = df[(df.SUMMONERNAME_x==main_summoner_name) &
                    (df.GAMECREATIONDATE > beg_timestamp) &
                    (df.GAMECREATIONDATE < end_timestamp)][cols]
    else:
        processed_df = df[(df.SUMMONERNAME_x==main_summoner_name) & (df.ROLE==role) &
                    (df.GAMECREATIONDATE > beg_timestamp) &
                    (df.GAMECREATIONDATE < end_timestamp)][cols]    

    if agg_type != 'agg':
        rename_dict_2 = {'CHAMPIONNAME_y':'Enemy_Champion', 'CHAMPIONNAME_X':'Champion', 'WINFLAG_x':'Win'}
        return processed_df.rename(columns=rename_dict_2).sort_values(by='GAMECREATIONDATE', ascending = False).reset_index(drop=True)
    else:
        processed_df.drop(['GAMECREATIONDATE'], axis=1, inplace=True)
        agg_types={'CHAMPIONNAME_y':'count','WINFLAG_x':'mean', 'CS_DIFF':'mean','XP_DIFF':'mean', 'GOLD_DIFF':'mean', 'DMG_DIFF':'mean' }
        rename_dict = {'CHAMPIONNAME_y':'Games', 'CHAMPIONNAME_x':'Champion_Name', 'WINFLAG_x':'Winrate', 'CS_DIFF':'CS_Diff@14', 'XP_DIFF':'XP_Diff@14', 'GOLD_DIFF':'Gold_Diff@14', 'DMG_DIFF':'DMG_Diff@14'}
        aggregated_df=processed_df.groupby('CHAMPIONNAME_x', as_index=False,).agg(agg_types).rename(columns=rename_dict)
        n_games = aggregated_df.Games.sum()
        total_row = ['Total', n_games,  aggregated_df.Games.dot(aggregated_df.Winrate)/n_games,
                                        aggregated_df.Games.dot(aggregated_df['CS_Diff@14'])/n_games,
                                        aggregated_df.Games.dot(aggregated_df['XP_Diff@14'])/n_games,
                                        aggregated_df.Games.dot(aggregated_df['Gold_Diff@14'])/n_games,
                                        aggregated_df.Games.dot(aggregated_df['DMG_Diff@14'])/n_games
        ]
        aggregated_df.loc[len(aggregated_df)] = total_row
        aggregated_df = aggregated_df.sort_values(by='Games', ascending=False).reset_index(drop=True)
        format_dict={'Game': "{:.0f}",
                        'Winrate': "{:.0%}",
                        'CS_Diff@14': "{:.0f}",
                        'XP_Diff@14': "{:.0f}",
                        'Gold_Diff@14': "{:.0f}",
                        'DMG_Diff@14': "{:.0f}"}
        if return_type == 'styled':
            return aggregated_df.style.format(format_dict)
        else:
            return aggregated_df
            

