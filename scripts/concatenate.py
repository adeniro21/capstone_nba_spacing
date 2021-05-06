import pandas
import os
import sys
import json
import pyarrow.feather as feather

#import movement.config as CONFIG

data_path = '/data/p_dsi/nba_player_movement/data_merged_files'
feather_path = '/data/p_dsi/nba_player_movement/concat_dataframe'
files = os.listdir(data_path)

if not os.path.exists(feather_path):
    os.makedirs(feather_path)


data = pd.DataFrame()

count = 0
for file in files:
    if '.file' not in file:
        continue
    try:
    	df = feather.read_feather('%s/%s' % (data_path, file))
    	if count == 0:
    		df = data
    	else:
    		data = pd.concat([data, df], axis=0)

    	count += 1

    	print('\n')
        print('\n')
        print('Finished concatenating dataframe for Game ID: ' + game_id)
        print('Completed : ' + str(count) + ' games.')
    except Exception as e:
        print('Error in loading: ' + str(file) + ' file, Error: ' + str(e))

print('\n')
print('\n')
print('Finished concatenating dataframes for all games.')
print(str(count) + ' games counted')

feather.write_feather(data,'%s/%s.file' % (feather_path, "concat_df"), compression='lz4')