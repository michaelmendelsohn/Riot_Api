import streamlit as st
import os
import mysql

import analysis_functions as af 
import pandas as pd
import helper_functions as help
import role_classification as rc
import oracledb
from riotwatcher import LolWatcher

_RIOT_API_KEY='RGAPI-530dc2f5-6f18-414d-b0ac-0037d535622c'
lol_watcher = LolWatcher(_RIOT_API_KEY)
engine = help.create_mysql_engine()

st.set_page_config(page_title='Best Damn League App', page_icon = ':tada:', layout = "wide")
st.write(os.getcwd())

# oracle_con = oracledb.connect(user = "admin", password = "Iamnotgroot123", dsn = "riotapidb_medium",
#                         config_dir = "Riot_Api/v1/oracle_wallet",
#                         wallet_location = "Riot_Api/v1/oracle_wallet",
#                         #config_dir = r"C:\Users\mmend\OneDrive\riot_api_git_clone_folder\Riot_Api\v1\oracle_wallet",
#                         #wallet_location = r"C:\Users\mmend\OneDrive\riot_api_git_clone_folder\Riot_Api\v1\oracle_wallet",
#                         wallet_password = "Iamnotgroot123")
# print(oracle_con)
# read_df = pd.read_sql('SELECT * FROM test_upload_table', oracle_con)
# st.write(read_df)


# -- Header Section ----
with st.container(): # this is optional
    st.title ("Welcome to my page! We're gonna show you some cool stats about your League of Legends gameplay!" )
    st.subheader("Page created by me - Michael Mendelsohn - a Senior Data Analyst looking for my next role.")
    st.write("To demo this page, try entering the Summoner Name 'SpicedCider', and clicking Go!.")
# def local_css(filename):
#     with open(filename) as f:
#         st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
# local_css("../style/style.css")
def display_stats(main_summoner_name, role, teammates_dict, return_type='agg'):
    engine = help.create_mysql_engine()
    #stats_df = af.stats_at_min(summoner_name, role, engine)
    stats_df = af.stats_at_min_with_teammates(main_summoner_name, role, engine, teammates_dict, return_type=return_type)
    st.write(stats_df)
    return

# ---- What I do

with st.container():
    
    st.write("---")
    left_col, right_col = st.columns((1,1))


    
    with left_col:

        summ_name_col, role_col = st.columns((1,1))
        with summ_name_col:
            summoner_name_0 = st.text_input("Enter Summoner Name", key="summoner_name_input_0")
            summoner_name_1 = st.text_input("Enter 1st Teammate Name", key="summoner_name_input_1")
            summoner_name_2 = st.text_input("Enter 2nd Teammate Name", key="summoner_name_input_2")
            summoner_name_3 = st.text_input("Enter 3rd Teammate Name", key="summoner_name_input_3")
            summoner_name_4 = st.text_input("Enter 4th Teammate Name", key="summoner_name_input_4")
        
        with role_col:
            role_0 = st.selectbox('Choose your Role.',('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))
            role_1 = st.selectbox("Choose 1st Teammate's Role.",('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))
            role_2 = st.selectbox("Choose 2nd Teammate's Role.",('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))
            role_3 = st.selectbox("Choose 3rd Teammate's Role.",('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))
            role_4 = st.selectbox("Choose 4th Teammate's Role.",('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))

        teammates_dict = {summoner_name_0:role_0,
                          summoner_name_1:role_1,
                          summoner_name_2:role_2,
                          summoner_name_3:role_3,
                          summoner_name_4:role_4}
        if "" in teammates_dict.keys():
            teammates_dict.pop("")
        teammates_dict_without_main = teammates_dict.copy()
        if not summoner_name_0 == "":
            teammates_dict_without_main.pop(summoner_name_0)

        if st.button(f"Pull New Data for {summoner_name_0}"):
            num_threads = 4
            matches_to_slurp = help.find_to_slurp(summoner_name_0, lol_watcher, 'lol_match_details', engine, region = 'na1')
            st.write(f"Pulling and analyzing data for {min(len(matches_to_slurp),100)} games. {num_threads} Concurrent processes. Estimated runtime is {min(2+len(matches_to_slurp)/2,52)} seconds. Max of 100 games.")
            help.collect_riot_api_data(summoner_name_0, lol_watcher, 'lol_match_timeline',
                        engine, match_upload_limit=100, num_worker_threads=num_threads)
            help.collect_riot_api_data(summoner_name_0, lol_watcher, 'lol_match_details',
                                    engine, match_upload_limit=100, num_worker_threads=num_threads)
            rc.determine_roles(engine)
    
    with right_col:
        if st.button("Go!", key='stats_go_button'):
            
            teammates_string = " and ".join([f" {name} as {teammates_dict_without_main[name]}" for name in teammates_dict_without_main.keys()])
            if len(teammates_dict_without_main)>0:
                conditional_with = ' with '
            else:
                conditional_with = ''
            st.write(f"Pulling {summoner_name_0}'s {role_0} games{conditional_with}{teammates_string}.")
            display_stats(summoner_name_0, role_0, teammates_dict)

# Lower Section Stats
with st.container():
    st.write("---")
    st.subheader("Game by Game data for the above aggregated stats")
    display_stats(summoner_name_0, role_0, teammates_dict, return_type='underlying')