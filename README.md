# Riot_Api
Pulls and stores League of Legends data. displays sick stats seen nowhere else(filtersble by teammates and roles). Now a streamlit app! [mmendelsohn.streamlit.app]
------------------------
 



Database HierArchy


- match_participant_view
    - 1 Row Per Partcipant in each match. Each participant will have same value for match date, game duration, queue type
    - Contains all raw data on participant as is available
- match_timeline_view
    - 1 row per minute per participant in each match

- summoner_performance_view
    - derived table created from above information
    - 1 row per match per summoner with aggregated stats (e.g. GD 15, Win/Loss, CSD 15, others?)
- 
