import pandas as pd
import os
import sys
import json
import pyarrow.feather as feather

#import movement.config as CONFIG

data_path = '/data/p_dsi/nba_player_movement/concat_dataframe'
feather_path = '/data/p_dsi/nba_player_movement/concat_dataframe/agg_files'
files = os.listdir(data_path)

if not os.path.exists(feather_path):
    os.makedirs(feather_path)

count = 0

for file in files:
    if '.file' not in file:
        continue
    try:
        df = feather.read_feather('%s/%s' % (data_path, file))
        df_agg = df[(df.EVENTMSGTYPE == 1) | (df.EVENTMSGTYPE == 2)].groupby(['game_id','position','player_name','team_name','EVENTMSGTYPE','player_dist_bin']).agg({'spacing_1':'mean', 'spacing_2':'mean'}).reset_index()
        
        count += 1

        feather.write_feather(df_agg, '%s/%s.file' % (feather_path, 'agg_file'+str(count)))

        print('finished agg_file' + str(count))

    except Exception as e:
        print('Error in loading: ' + str(file) + ' file, Error: ' + str(e))

print('\n')
print('\n')
print('Finished concatenating dataframes for all games.')
print(str(count) + ' games counted')
