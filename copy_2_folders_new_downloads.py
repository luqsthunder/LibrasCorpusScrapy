# %%
import os
from tqdm import tqdm
import shutil
import pandas as pd

#%%
bad_videos = pd.read_csv('bad_videos.csv')
db_path = '/media/usuario/Others/gdrive/LibrasCorpus/Santa Catarina/Inventario Libras/'
base_download_folder = '/home/usuario/Downloads/'
downloads_files = list(filter(lambda x: '.mp4' in x ,
                              [os.path.join(base_download_folder, x) for x in os.listdir(base_download_folder)]))

all_folders_path = sorted([os.path.join(db_path, x) for x in os.listdir(db_path)],
                          key=lambda x: int(x.split(' v')[-1]), reverse=True)


# %%
for row in tqdm(bad_videos.iterrows(), total=bad_videos.shape[0]):
    row = row[1]
    only_name = row.video_name.replace('\\', '/').split('/')[-1]
    vpart_folder = row.video_name.replace('\\', '/').split('/')[-2]

    download_file_path = os.path.join(base_download_folder, only_name)

    dst_file_path = os.path.join(db_path, vpart_folder, only_name)

    if os.path.exists(download_file_path) and os.path.exists(dst_file_path):
        shutil.copy(download_file_path, dst_file_path)
