import pandas as pd
import os
import pget
import datetime

if __name__ == '__main__':
    proj_path = './db/Santa Catarina/Inventario Libras'
    items = os.listdir(proj_path)
    items = list(map(lambda x: os.path.join(proj_path, x), items))
    amount_folder = len(items)
    cant_download = ['https://repositorio.ufsc.br/bitstream/handle/123456789/153268/FLN_G2_D1_1entrevista_VIDEO1.mp4?sequence=1&isAllowed=y',
                     'https://repositorio.ufsc.br/bitstream/handle/123456789/153337/FLN_G2_D1_1entrevista_VIDEO2.mp4?sequence=1&isAllowed=y',
                     'https://repositorio.ufsc.br/bitstream/handle/123456789/153547/FLN_G2_D1_1entrevista_VIDEO3.mp4?sequence=1&isAllowed=y',
                     'https://repositorio.ufsc.br/bitstream/handle/123456789/153575/FLN_G2_D1_1entrevista_VIDEO4.mp4?sequence=1&isAllowed=y']
    for k, it in enumerate(items):
        csv_path = list(filter(lambda x: 'csv' in x, os.listdir(it)))[0]
        csv_path = os.path.join(it, csv_path)
        csv_file = pd.read_csv(csv_path, index_col=0)
        curr_time = datetime.datetime.now().time()
        print(f'begin download at time : {curr_time} \n '
              f'folder {k} / {amount_folder}\n')
        for row in csv_file.iterrows():
            row = row[1]
            name = row.files.split('/')[-1] if 'mp4' in row.files \
                else row.files.split('?')[0].split('/')[-1] + '.EAF'

            if name.find('mp4') != -1:
                idx = name.find('mp4')
                name = name[:idx+3]

            file_url = row.files if row.video == 1 else 'http://' + row.files
            file_name = os.path.join(it, name)

            if os.path.exists(file_name) or file_url in cant_download:
                continue

            print(f'\nDownloading {name} in path:\n{file_name}')

            try:
                downloader = pget.Downloader(file_url, file_name,
                                             16 if row.video == 1 else 1)

                if row.video == 1:
                    downloader.subscribe(pget.pget_download_callback, 256)

                    downloader.start()

                    downloader.wait_for_finish()
                else:
                    downloader.start_sync()
                    # downloader.wait_for_finish()
            except BaseException as e:
                print(f'Erro downloading file {name} \n in path: {file_name}\n'
                      f'Exception \n{e}', file=open('error.log', mode='a'))
