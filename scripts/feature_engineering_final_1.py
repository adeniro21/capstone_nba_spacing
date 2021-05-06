#!/usr/bin/env python
# coding: utf-8

# In[270]:


import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import math
import pyarrow.feather as feather
import json
import os
import sys





players = feather.read_feather('/data/p_dsi/nba_player_movement/team_player_data/players.file')
teams = feather.read_feather('/data/p_dsi/nba_player_movement/team_player_data/teams.file')
outcomes = feather.read_feather('/data/p_dsi/nba_player_movement/outcome_shots_data/outcomes.file')
shot_distance_percentage = pd.read_csv('/data/p_dsi/nba_player_movement/outcome_shots_data/shot_distance_percentage.csv')


data_path = '/data/p_dsi/nba_player_movement/data_feather_file/group1'
feather_path = '/data/p_dsi/nba_player_movement/data_merged_files'
files = os.listdir(data_path)

if not os.path.exists(feather_path):
    os.makedirs(feather_path)

count = 0

for file in files:
    if '.file' not in file:
        continue
    try:
        count = count + 1
        movement = feather.read_feather('%s/%s' % (data_path, file))
        movement['event_id'] = movement.event_id.astype('int')
        movement['game_id'] = movement.game_id.astype('string')


        # filtering of movement data
        def convert_game_clock(seconds): 
            seconds = round(seconds)
            actual_seconds = seconds % 60 
            minutes = seconds // 60
              
            return "%02d:%02d" % (minutes, actual_seconds)

        game_clock_new = pd.Series(movement.game_clock).map(convert_game_clock)

        movement['game_clock_new'] = game_clock_new
        movement.game_clock_new = movement.game_clock_new.astype('string')

        movement['game_clock_total'] = movement.game_clock * movement.quarter

        movement = movement.sort_values(['quarter','game_clock','event_id'], ascending = (True, False, True)).reset_index()
        movement.drop_duplicates(subset = ['player_id', 'x_loc', 'y_loc', 'game_clock', 'shot_clock', 'quarter'],inplace = True)

        game_clock_filter = movement.groupby(['game_clock_total']).count()['game_id'].loc[lambda x: x == 11].sort_values().index.get_level_values(0).tolist()
        movement = movement[movement.game_clock_total.isin(game_clock_filter)]

        movmement = movement[movement.shot_clock != 24]


        #merging movement and outcomes data
        merge_df = movement.merge(outcomes, left_on=['game_id' ,'quarter', 'game_clock_new'], right_on=['GAME_ID', 'PERIOD', 'PCTIMESTRING'], how='left')

        merge_df.drop_duplicates(subset = ['player_id', 'x_loc', 'y_loc', 'game_clock', 'shot_clock', 'quarter'], inplace=True)

        merge_df.reset_index(inplace=True)

        merge_df = merge_df.drop(['index','level_0', 'event_id'], axis=1)

        #Fill missing values
        merge_df = merge_df.sort_values(['quarter','game_clock','EVENTNUM'], ascending = (True, False, True)).reset_index()

        merge_df.EVENTNUM.fillna(method='bfill', inplace=True)
        merge_df.EVENTMSGTYPE.fillna(method='bfill', inplace=True)
        merge_df.drop(['index'], axis=1, inplace=True)

        game_id = merge_df.game_id[0]

        # distance from the ball
        dist_from_ball = []

        for i in range(merge_df.shape[0]):
            if i % 11 == 0:
                dist_from_ball.append(np.nan)
                for j in range(1,11):
                    dist = math.sqrt(((merge_df.x_loc[i] - merge_df.x_loc[i+j])**2) + ((merge_df.y_loc[i] - merge_df.y_loc[i+j])**2))
                    dist_from_ball.append(dist)

        merge_df['dist_from_ball'] = dist_from_ball


        # creating possession variable
        loc_feature = pd.DataFrame(merge_df.groupby('EVENTNUM').mean()['x_loc']).reset_index().rename(columns = {'x_loc':'avg_loc'})
        merge_df = merge_df.merge(loc_feature, on='EVENTNUM', how='inner')

        first_made_shot_event = merge_df[(merge_df.EVENTMSGTYPE == 1) | (merge_df.EVENTMSGTYPE == 2)].loc[:,'EVENTNUM'].iloc[0]

        poss_loc = merge_df[merge_df.EVENTNUM == first_made_shot_event].loc[:,'x_loc'].mean()
        poss_indic = pd.DataFrame(merge_df[merge_df.EVENTNUM == first_made_shot_event].loc[:,['HOMEDESCRIPTION','VISITORDESCRIPTION']].iloc[-1].tolist())
        side = poss_indic[poss_indic[0].notnull()].index.tolist()[0]

        side_loc = 0
        if poss_loc > 50:
            side_loc == 1
            
        home_id = merge_df.loc[:,'team_id'].iloc[1]
        away_id = merge_df.loc[:,'team_id'].iloc[6]

        possession = []
        for i in range(merge_df.shape[0]): 
            if i % 11 == 0:
                possession.append(np.nan)
            else:
                # away, right
                if side == 1:  
                    if side_loc == 1:
                        if merge_df.quarter[i] < 3:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                        else:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                    # away left
                    else:
                        if merge_df.quarter[i] < 3:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                        else:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1) 
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                # home, right
                else:
                    if side_loc == 1:
                        if merge_df.quarter[i] < 3:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                        else:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                    # home left
                    else:
                        if merge_df.quarter[i] < 3:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)
                        else:
                            if merge_df.avg_loc[i] > 50:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(1)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(0)  
                            else:
                                if merge_df.team_id[i] == home_id:
                                    possession.append(0)
                                elif merge_df.team_id[i] == away_id:
                                    possession.append(1)
                                    
        merge_df['possession'] = possession


        # distance of the closest defender to any offensive player
        closest_defender_dist = []
        for i in range(merge_df.shape[0]):
            if i % 11 == 0:  
                closest_defender_dist.append(np.nan)
                offense_ind = []
                defense_ind = []
                for j in range(1,11):
                    if merge_df.possession[i+j] == 1:
                        offense_ind.append(i+j)
                    else:
                        defense_ind.append(i+j)
                if offense_ind[0] > defense_ind[0]:
                    closest_defender_dist.extend([np.nan,np.nan,np.nan,np.nan,np.nan]) 
                for k in range(len(offense_ind)):
                    dist = []
                    for l in range(11):
                        if merge_df.possession[i+l] == 0:
                            dist.append(math.sqrt(((merge_df.x_loc[offense_ind[k]] - merge_df.x_loc[i+l])**2) + ((merge_df.y_loc[offense_ind[k]] - merge_df.y_loc[i+l])**2)))
                    closest_defender_dist.append(min(dist))
                if offense_ind[0] < defense_ind[0]:
                    closest_defender_dist.extend([np.nan,np.nan,np.nan,np.nan,np.nan])
                        
        merge_df['closest_defender_dist'] = closest_defender_dist


        # distance from the basket
        left_basket_coordinates = [5.25, 25]
        right_basket_coordinates = [88.65, 25]

        dist_from_basket = []
        for i in range(merge_df.shape[0]):
            left_basket_dist = math.sqrt(((merge_df.x_loc[i] - left_basket_coordinates[0])**2) + ((merge_df.y_loc[i] - left_basket_coordinates[1])**2))
            right_basket_dist = math.sqrt(((merge_df.x_loc[i] - right_basket_coordinates[0])**2) + ((merge_df.y_loc[i] - right_basket_coordinates[1])**2))
            dist_from_basket.append(min(left_basket_dist, right_basket_dist))  
            
        merge_df['dist_from_basket'] = dist_from_basket

        # binning player distance so that it matches the shooting bins
        player_dist_bin = []
        for i in range(merge_df.shape[0]):
            if merge_df.player_id[i] != -1:
                if merge_df.dist_from_basket[i] <= 8:
                    player_dist_bin.append(0)
                elif (merge_df.dist_from_basket[i] > 8) & (merge_df.dist_from_basket[i] <= 16):
                    player_dist_bin.append(8)
                elif (merge_df.dist_from_basket[i] > 16) & (merge_df.dist_from_basket[i] <= 24):
                    player_dist_bin.append(16)
                elif (merge_df.dist_from_basket[i] > 24) & (merge_df.dist_from_basket[i] <= 50):
                    player_dist_bin.append(24)
                else:
                    player_dist_bin.append(50)
            else:
                player_dist_bin.append(np.nan)
                
        merge_df['player_dist_bin'] = player_dist_bin

        # mergeing shooting data to movement and outcomes
        merge_df = merge_df.merge(shot_distance_percentage, left_on = ['player_id', 'player_dist_bin'], right_on = ['PLAYER_ID', 'SHOT_RANGE_BIN'], how='left')


        # merging player and team names
        merge_df = merge_df.merge(players, left_on = 'player_id', right_on = 'playerid', how='left')
        merge_df = merge_df.merge(teams, left_on = 'team_id', right_on = 'team_id', how='left')


        eventnum_list = merge_df[(merge_df.EVENTMSGTYPE == 1) | (merge_df.EVENTMSGTYPE == 2)].groupby('EVENTNUM').count().reset_index()['EVENTNUM'].tolist()

        shooter_df = outcomes[(outcomes.EVENTNUM.isin(eventnum_list)) & (outcomes.GAME_ID == game_id)][['EVENTNUM','shooter']]

        merge_df = merge_df.merge(shooter_df, on='EVENTNUM', how='left')


        shooter_ind = []
        for i in range(merge_df.shape[0]):
            if str(merge_df.shooter_y[i]) in str(merge_df.player_name[i]):
                shooter_ind.append(1)
            else:
                shooter_ind.append(0)
                
        merge_df['shooter_ind'] = shooter_ind


        shooting_averages = merge_df.groupby('player_dist_bin').agg({'SHOT_PERCENTAGE':['mean','std'],'SHOT_COUNT_MADE':['mean','std']})


        #impute missing shooting data with mean
        for i in range(merge_df.shape[0]):
            if i % 11 != 0:
                if math.isnan(merge_df.SHOT_PERCENTAGE[i]):
                    if merge_df.player_dist_bin[i] == 0:
                        merge_df.SHOT_PERCENTAGE[i] = shooting_averages.iloc[0,0]
                        merge_df.SHOT_COUNT_MADE[i] = shooting_averages.iloc[0,2]
                    elif merge_df.player_dist_bin[i] == 8:
                        merge_df.SHOT_PERCENTAGE[i] = shooting_averages.iloc[1,0]
                        merge_df.SHOT_COUNT_MADE[i] = shooting_averages.iloc[1,2]
                    elif merge_df.player_dist_bin[i] == 16:
                        merge_df.SHOT_PERCENTAGE[i] = shooting_averages.iloc[2,0]
                        merge_df.SHOT_COUNT_MADE[i] = shooting_averages.iloc[2,2]
                    elif merge_df.player_dist_bin[i] >= 24:
                        merge_df.SHOT_PERCENTAGE[i] = shooting_averages.iloc[3,0]
                        merge_df.SHOT_COUNT_MADE[i] = shooting_averages.iloc[3,2]


        #impute missing closest defender data with the mean
        cdd_mean = merge_df.closest_defender_dist.mean()
        for i in range(merge_df.shape[0]):
            if i % 11 != 0:
                if merge_df.possession[i] == 1:
                    if math.isnan(merge_df.closest_defender_dist[i]):
                        merge_df.closest_defender_dist[i] = cdd_mean


        #standardizing data scources shooting percentages and shooting volume
        player_dist_0 = []
        player_volume_0 = []
        player_dist_8 = []
        player_volume_8 = []
        player_dist_16 = []
        player_volume_16 = []
        player_dist_24 = []
        player_volume_24 = []

        for i in range(merge_df.shape[0]):
            if merge_df.player_dist_bin[i] == 0:
                player_dist_0.append([i,merge_df.SHOT_PERCENTAGE[i]])
                player_volume_0.append([i,merge_df.SHOT_COUNT_MADE[i]])
            elif merge_df.player_dist_bin[i] == 8:
                player_dist_8.append([i,merge_df.SHOT_PERCENTAGE[i]])
                player_volume_8.append([i,merge_df.SHOT_COUNT_MADE[i]])
            elif merge_df.player_dist_bin[i] == 16:
                player_dist_16.append([i,merge_df.SHOT_PERCENTAGE[i]])
                player_volume_16.append([i,merge_df.SHOT_COUNT_MADE[i]])
            elif merge_df.player_dist_bin[i] >= 24:
                player_dist_24.append([i,merge_df.SHOT_PERCENTAGE[i]])
                player_volume_24.append([i,merge_df.SHOT_COUNT_MADE[i]]) 
                
        player_dist_0 = pd.DataFrame(player_dist_0)
        player_volume_0 = pd.DataFrame(player_volume_0)
        player_dist_8 = pd.DataFrame(player_dist_8)
        player_volume_8 = pd.DataFrame(player_volume_8)
        player_dist_16 = pd.DataFrame(player_dist_16)
        player_volume_16 = pd.DataFrame(player_volume_16)
        player_dist_24 = pd.DataFrame(player_dist_24)
        player_volume_24 = pd.DataFrame(player_volume_24)



        player_dist_0['shooting_standard'] = (player_dist_0.iloc[:,1]-shooting_averages.iloc[0,0])/shooting_averages.iloc[0,1]
        player_volume_0['volume_standard'] = (player_volume_0.iloc[:,1]-shooting_averages.iloc[0,2])/shooting_averages.iloc[0,3]
        player_dist_8['shooting_standard'] = (player_dist_8.iloc[:,1]-shooting_averages.iloc[1,0])/shooting_averages.iloc[1,1]
        player_volume_8['volume_standard'] = (player_volume_8.iloc[:,1]-shooting_averages.iloc[1,2])/shooting_averages.iloc[1,3]
        player_dist_16['shooting_standard'] = (player_dist_16.iloc[:,1]-shooting_averages.iloc[2,0])/shooting_averages.iloc[2,1]
        player_volume_16['volume_standard'] = (player_volume_16.iloc[:,1]-shooting_averages.iloc[2,2])/shooting_averages.iloc[2,3]
        player_dist_24['shooting_standard'] = (player_dist_24.iloc[:,1]-shooting_averages.iloc[3,0])/shooting_averages.iloc[3,1]
        player_volume_24['volume_standard'] = (player_volume_24.iloc[:,1]-shooting_averages.iloc[3,2])/shooting_averages.iloc[3,3]




        shooting_frames = pd.concat([player_dist_0, player_dist_8, player_dist_16, player_dist_24]).rename(columns = {0:'index',1:'shooting'})
        volume_frames = pd.concat([player_volume_0, player_volume_8, player_volume_16, player_volume_24]).rename(columns = {0:'index',1:'volume'})




        merge_df['index_merge'] = [i for i in range(merge_df.shape[0])]
        merge_df = merge_df.merge(shooting_frames, left_on='index_merge',right_on='index',how='left')
        merge_df = merge_df.merge(volume_frames, left_on='index_merge',right_on='index',how='left')



        merge_df['dist_from_ball_standard'] = (merge_df.dist_from_ball-merge_df.dist_from_ball.mean())/merge_df.dist_from_ball.std()
        merge_df['closest_defender_standard'] = (merge_df.closest_defender_dist-merge_df.closest_defender_dist.mean())/merge_df.closest_defender_dist.std()



        #create spacing measure
        shooting_measure = merge_df.shooting_standard + merge_df.volume_standard
        #normalize shooting measure
        merge_df['shooting_normalize'] = (shooting_measure-shooting_measure.min())/(shooting_measure.max()-shooting_measure.min())



        merge_df['spacing_1'] = merge_df.closest_defender_standard*merge_df.shooting_normalize
        merge_df['spacing_2'] = abs(merge_df.dist_from_ball_standard-merge_df.closest_defender_standard)*merge_df.shooting_normalize



        merge_df = merge_df.loc[:,['game_id','team_id','team_name','player_id','player_name','position','x_loc','y_loc','game_clock',
                        'game_clock_new','shot_clock', 'quarter', 'EVENTNUM','EVENTMSGTYPE','possession','dist_from_ball',
                        'closest_defender_dist','dist_from_basket','player_dist_bin','SHOT_PERCENTAGE','SHOT_COUNT_MADE','shooter_ind',
                        'shooting_standard','volume_standard','shooting_normalize','dist_from_ball_standard','closest_defender_standard',
                        'spacing_1','spacing_2']]

        feather.write_feather(merge_df,'%s/%s.file' % (feather_path, game_id+'_merged'), compression='lz4')


        print('\n')
        print('\n')
        print('Finished collecting dataframe for Game ID: ' + game_id)
        print('Completed : ' + str(count) + ' games.')
    except Exception as e:
        print('Error in loading: ' + str(file) + ' file, Error: ' + str(e))

print('\n')
print('\n')
print('Finished collecting dataframes for all games.')
print(str(count) + ' games counted')




