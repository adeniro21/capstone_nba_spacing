#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import pyarrow.feather as feather
import json
import os
import sys

data_path = '/home/deniroar/capstone/movement_unzipped'
feather_path = '/home/deniroar/capstone/team_player_data'
files = os.listdir(data_path)

if not os.path.exists(feather_path):
    os.makedirs(feather_path)

count = 0

team_header = ['team_name', 'team_id']
teams = []

players_header = ['player_name', 'playerid', 'position']
players = []

for file in files:
    if '.json' not in file:
        continue
    try:
        count = count + 1
        file_data = open('%s/%s' % (data_path, file))
        data = json.load(file_data)
        events = data['events']

        home_players = events[0]['home']['players']
        away_players = events[0]['visitor']['players']

        for i in range(len(home_players)):
            name = home_players[i]['firstname'] + " " + home_players[i]['lastname']
            playerid = home_players[i]['playerid']
            position = home_players[i]['position']
            players.append([name, playerid, position])

        for i in range(len(away_players)):
            name = away_players[i]['firstname'] + " " + away_players[i]['lastname']
            playerid = away_players[i]['playerid']
            position = away_players[i]['position']
            players.append([name, playerid, position])

        teams.append([events[0]['home']['name'], events[0]['home']['teamid']])
        teams.append([events[0]['visitor']['name'], events[0]['visitor']['teamid']])
        
        print('\n')
        print('Finished appending palyers and teams for Game ID: ' + game_id)
        print('Completed : ' + str(count) + ' games.')
    except Exception as e:
        print('Error in loading: ' + str(file) + ' file, Error: ' + str(e))

print('\n')
print('Finished appending palyers and teams for all games.')
print(str(count) + ' games counted')

players = pd.DataFrame(players, columns=players_header).drop_duplicates()
teams = pd.DataFrame(teams, columns=team_header).drop_duplicates()

feather.write_feather(players, '%s/%s.file' % (feather_path, 'players'))
feather.write_feather(teams, '%s/%s.file' % (feather_path, 'teams'))


