dirName = 'FTP_DATA/2020'

import os
from tqdm import tqdm

for subdir in os.listdir(dirName):
	for subsubdir in tqdm(os.listdir(os.path.join(dirName,subdir))):
		_path = os.path.join(dirName,subdir,subsubdir).replace('\\','/')
		for roots, dirs, files in os.walk(_path):
			for file in files:
				if file[-4:] == '.csv':
					os.remove(os.path.join(_path, file))
					# print(os.path.join(_path, file),' ... removed!')

print('All CSVs deleted successfully!')
