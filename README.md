# Capstone NBA Spacing
## Spacing Effects on Offensive Efficiency in the NBA Using Player Tracking Data

## Project Background
Allowing players to operate with the most space between them and a defender is a general idea that has shaped a majority of offensive schemes from past to future. Coaches and players understand the value in getting players open in spots where they are offensively efficient. And thanks to the advent of modern cameras and algorithms, capturing data on player locations has been made available. Player tracking data offers the ability to look at the game from the lowest level. By tracking the 10 players and the ball at 25 frames/second, we have the precision to be able to measure spacing and evaluate which teams are utilizing it the best. In this project I seek to further understand the correlation between spacing and offensive efficiency. By the end I would like to determine the significance spacing has on the outcomes of games, as well as the value it has for teams and individual players.

## Data
Data is broken up into csv and feather files
- final_df: final dataset used for Tableau Dashboad visualizations
- outcomes_final: event outcomes for possession joined onto movement datasets
- player_ortg15-16: offensive rating for players in 2015-2016 NBA season from basketball-reference
- shot_distance_percentage: shooting data containing the percentage and volume each player shot at various distances on the court
- sportsref_downoad15-16: offensive ratings for teams in the 2015-2016 NBA season from basketball-reference
- temp_data: This contains a few feather files that are the results of running the feature engineering script on the raw game files. Just if anyone wants to get an idea of what the inital data looked like
- players: dataset containing all the player names and id that appeared in a game from the movement data 
- teams: dataset contained the team name and id for all teams in the movement data

## Deliverables
- capstone_presentation: 6 minute video walking through the powerpoint presentation
- NBA Spacing Evaluations Using Player Tacking Data: powerpoint presenting findings in the data
- Tableau workbook containing for dashboards relating to game, team, and player level information

## Noteboks
I ran jupyter notebook locally on only one game files to ensure that everything worked properly before running the scripts on ACCRE
- concatenate: concatenates all game files after the feature engineering script has run.
- feature_engineering_final: contains the logical for the feature engineering I did on each game file that I would scale up to run on ACCRE
- feature_engineering_raw: contains logic for other features (offense/defense shapes, animations) that I did not scale mainly due to time constraints
- final_df: final dataset used for Tableau Dashboad visualizations, merges offensive ratings andincludes the correlation test performed on spacing and offensive rating
- json_to_feather: contains the logic for converting the original json game files into feather files
- shots: contains shooting information that is aggregated to produce shot_distance_percentage.csv
- team_player_data: logic for creating the players and teams datasets

## Scripts
The scripts are essentially the same as the notebooks but instead are scaled so that they'll loop through multiple game files. Ill only higlight scripts that I do not have as a notebook
- agg.py: Aggregated the concatenated dataframes with groupby statements so that we were looking at spacing by event/possession and not a each millisecond. 
