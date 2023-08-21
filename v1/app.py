import streamlit as st
import os
from riotwatcher import LolWatcher
import analysis_functions as af 
import helper_functions as help
import role_classification as rc

_RIOT_API_KEY='RGAPI-df355d03-837c-43cc-a40a-b70885e04bd3'
lol_watcher = LolWatcher(_RIOT_API_KEY)
engine = help.create_mysql_engine()

st.set_page_config(page_title='Best Damn League App', page_icon = ':tada:', layout = "wide")

# -- Header Section ----
with st.container(): # this is optional
    st.title ("Welcome to my page! We're gonna show you some cool stats about your League of Legends gameplay!" )
    st.subheader("Page created by me - Michael Mendelsohn - a Senior Data Analyst looking for my next role.")
    st.write("To demo this page, try entering the Summoner Name 'SpicedCider', and clicking Go!.")
# def local_css(filename):
#     with open(filename) as f:
#         st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
# local_css("../style/style.css")
def display_stats(summoner_name, role):
    engine = help.create_mysql_engine()
    stats_df = af.stats_at_min(summoner_name, role, engine)
    st.write(stats_df)
    return

# ---- What I do
with st.container():
    st.write("---")
    left_col, right_col = st.columns((1,1))
    with left_col:
        summoner_name = st.text_input("Enter Summoner Name",key="summoner_name_inpute")
        role = st.selectbox('Choose your Role.',('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))
        if st.button(f"Pull New Data for {summoner_name}"):
            num_threads = 4
            matches_to_slurp = help.find_to_slurp(summoner_name, lol_watcher, 'lol_match_details', engine, region = 'na1')
            st.write(f"Pulling and analyzing data for {min(len(matches_to_slurp),100)} games. {num_threads} Concurrent processes. Estimated runtime is {min(2+len(matches_to_slurp)/2,52)} seconds. Max of 100 games.")
            help.collect_riot_api_data(summoner_name, lol_watcher, 'lol_match_timeline',
                        engine, match_upload_limit=100, num_worker_threads=num_threads)
            help.collect_riot_api_data(summoner_name, lol_watcher, 'lol_match_details',
                                    engine, match_upload_limit=100, num_worker_threads=num_threads)
            rc.determine_roles(engine)
    with right_col:
        if st.button("Go!"):
            display_stats(summoner_name, role)
# Lower Section Stats
with st.container():
    st.write("---")