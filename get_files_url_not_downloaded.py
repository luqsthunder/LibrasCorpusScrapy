import os
import pandas as pd
from tqdm import tqdm

# %%
proj_path = '/media/usuario/Others/gdrive/LibrasCorpus/Santa Catarina/Inventario Libras/'
items = os.listdir(proj_path)
items = list(map(lambda x: os.path.join(proj_path, x), items))
to_download = pd.DataFrame()
for k, it in tqdm(enumerate(items)):
    vpart = it.split(' v')[-1]
    csv_path = list(filter(lambda x: 'download' in x, os.listdir(it)))
    if len(csv_path) == 0:
        continue

    csv_path = csv_path[0]
    csv_path = os.path.join(it, csv_path)
    csv_file = pd.read_csv(csv_path, index_col=0)
    for row in csv_file.iterrows():
        row = row[1]
        name = row.files.split('/')[-1] if 'mp4' in row.files else row.files.split('?')[0].split('/')[-1] + '.EAF'
        if name.find('mp4') != -1:
            idx = name.find('mp4')
            name = name[:idx + 3]

        file_url = row.files if row.video == 1 else 'http://' + row.files
        file_name = os.path.join(it, name)
        if not os.path.exists(file_name):
            to_download = to_download.append(pd.DataFrame(dict(
                url=[file_url], name=[file_name], vpart=[vpart]
            )), ignore_index=True)
to_download.to_csv('need_2_download.csv', index=False)
# %%
for a in to_download.iterrows():
    a = a[1]
    print(a.url, file=open('to_download.txt', mode='a'))

# %%
data_df_2_download = pd.read_csv('bad_itens.csv')
for a in data_df_2_download.iterrows():
    a = a[1]
    print(a.url, file=open('to_download.txt', mode='a'))
