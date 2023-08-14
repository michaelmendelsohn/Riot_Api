# Riot_Api
Connecting to Riot's API for League of Legends data.

1. Pull data using riot_data_pull
2. Classify laners into proper positions using the role_classification_functions module. can use score_error to see how accuracte we are.
3. Use data_analysis to calculate some statistics
4. Experiment! Woo!

------------------------

to do for v1:


1. get puuid for a summoner name - Summonerv4/by-name/summonername
2. get list of match ids by puuid - Matchv5/ids
3. Check database for which match ids i don't have - find_to_slurp
4. Create a function that does the following
- get match by match id - Matchv5/matchids
    - Store in a DB
- get match timeline by match id
    - store in a DB
5. Create concurrent processing that will speed this up




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