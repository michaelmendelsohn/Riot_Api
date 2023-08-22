# Riot_Api
Pulls and stores League of Legends data. Displays performance stats filterable by teammates & roles seen nowhere else. Soon to be a public streamlit app! [mmendelsohn.streamlit.app](mmendelsohn.streamlit.app)
Screenshots  of streamlit app:
------------------------
 
![image](https://github.com/michaelmendelsohn/Riot_Api/assets/12532898/8cd56f0b-ccfb-4116-91be-15ce5d6a70ff)
![image](https://github.com/michaelmendelsohn/Riot_Api/assets/12532898/869045c0-4403-44e3-b756-27d771d8420c)



Database Hierarchy


- match_participant_view
    - 1 Row Per Partcipant in each match. Each participant will have same value for match date, game duration, queue type
    - Contains all raw data on participant as is available
- match_timeline_view
    - 1 row per minute per participant in each match

- summoner_performance_view
    - derived table created from above information
    - 1 row per match per summoner with aggregated stats (e.g. GD 15, Win/Loss, CSD 15, others?)
- 
