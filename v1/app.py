import streamlit as st
import os

import analysis_functions as af 
import helper_functions as help


st.set_page_config(page_title='Best Damn League App', page_icon = ':tada:', layout = "wide")

# -- Header Section ----
with st.container(): # this is optional
    st.title ("Welcome to my page! We're gonna show you some cool stats about your League of Legends gameplay!" )
# def local_css(filename):
#     with open(filename) as f:
#         st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
# local_css("../style/style.css")
def display_stats(summoner_name, role):
    engine = help.create_mysql_engine()
    stats_df = af.stats_at_min(summoner_name, role, engine)
    st.write(stats_df)

# ---- What I do
with st.container():
    st.write("---")
    left_col, right_col = st.columns((1,2))
    with left_col:
        st.header("What Stats?")
        summoner_name = st.text_input("Enter Summoner Name",key="summoner_name_inpute")
        role = st.selectbox('Choose your Role.',('ALL ROLES', 'TOP', 'JUNGLE', 'MIDDLE','BOTTOM', 'SUPPORT'))
        
    with right_col:
        st.button('gogogogogo', on_click=display_stats(summoner_name, role))