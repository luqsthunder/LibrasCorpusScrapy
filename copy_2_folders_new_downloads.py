# %%
import os
from tqdm import tqdm
import shutil
from urllib.parse import unquote
import pandas as pd

#%%
bad_videos = pd.read_csv('need_2_download.csv')
db_path = '/media/usuario/Others/gdrive/LibrasCorpus/Santa Catarina/Inventario Libras/'
base_download_folder = '/home/usuario/Downloads/libras_videos/'
downloads_files = list(filter(lambda x: '.mp4' in x ,
                              [os.path.join(base_download_folder, x) for x in os.listdir(base_download_folder)]))

all_folders_path = sorted([os.path.join(db_path, x) for x in os.listdir(db_path)],
                          key=lambda x: int(x.split(' v')[-1]), reverse=True)
bad_videos['vid_name'] = bad_videos.name


# %%
for row in tqdm(bad_videos.iterrows(), total=bad_videos.shape[0]):
    row = row[1]

    only_name = row.vid_name.replace('\\', '/').split('/')[-1]
    vpart_folder = row.vid_name.replace('\\', '/').split('/')[-2]

    download_file_path = os.path.join(base_download_folder, only_name)

    download_file_path = unquote(download_file_path)

    is_eaf = 'EAF' in download_file_path
    if is_eaf:
        download_file_path = download_file_path[:-3] + "txt"

    dst_file_path = os.path.join(db_path, vpart_folder, only_name)
    dst_file_path = unquote(dst_file_path)

    if os.path.exists(download_file_path):
        shutil.copy(download_file_path, dst_file_path)
