import os
import cv2 as cv
import pandas as pd
from tqdm import tqdm
import xml.etree.ElementTree as et


class CheckDBBaseItemsIntegrity:

    def __init__(self, db_path: str, all_videos: pd.DataFrame or str):
        """

        Parameters
        ----------
        db_path :  str
        all_videos : pd.DataFrame or str

        """
        # gerenciando tipos dos parâmetros.
        if not (isinstance(all_videos, pd.DataFrame) or isinstance(all_videos, str)):
            raise TypeError(f'All videos must be str or pd.dataframe all videos has type {type(all_videos)}')

        self.db_path = db_path
        self.all_videos = all_videos if isinstance(all_videos, pd.DataFrame) else pd.read_csv(all_videos)
        self.folders = self.all_videos.folder.unique().tolist()
        self.bad_itens_df = pd.DataFrame()

    def process(self):
        for f in tqdm(self.folders):
            ret = self._read_single_folder(f)
            url_video_group = self._group_video_name_2_his_own_url(ret['videos'], ret['download_csv'])
            url_eaf_group = self._group_eaf_name_2_his_own_url(ret['eaf'], ret['download_csv'])
            curr_end = self.all_videos[self.all_videos.folder == f].end.max()

            v_part = f.split(' v')[-1]

            for url_n_eaf in url_eaf_group:
                curr_eaf_integrity = self._check_single_eaf_integrity(url_n_eaf['eaf'])
                if not curr_eaf_integrity:
                    self.bad_itens_df = self.bad_itens_df.append(pd.DataFrame(
                        dict(v_part=[v_part], video_name=[url_n_eaf['eaf']], url=[url_n_eaf['url']])
                    ), ignore_index=True)

            for url_n_video in url_video_group:
                curr_vid_integrity = self._check_single_video_integrity(url_n_video['video'], curr_end)
                if not curr_vid_integrity:
                    self.bad_itens_df = self.bad_itens_df.append(pd.DataFrame(
                        dict(v_part=[v_part], video_name=[url_n_video['video']], url=[url_n_video['url']])
                    ), ignore_index=True)

        self.bad_itens_df.to_csv('bad_itens.csv')

    def _read_single_folder(self, folder):
        """

        Parameters
        ----------
        folder : str

        Returns
        -------
        dict
          É retornado um dicionario com as chaves download_csv e videos.

        """
        folder_path = os.path.join(self.db_path, folder).replace('\\', '/')
        files = os.listdir(folder_path)

        # procurando CSV de downloads
        csv_download = list(filter(lambda x: 'download.csv' in x, files))
        video_files = list(filter(lambda x: '.mp4' in x, files))
        eaf_files = list(filter(lambda x: '.EAF' in x or '.eaf' in x, files))

        if len(csv_download) == 0 or len(video_files) == 0:
            return {}

        video_files = list(map(lambda x: os.path.join(folder_path, x), video_files))
        eaf_files = list(map(lambda x: os.path.join(folder_path, x), eaf_files))
        # sempre tem apenas 1 csv de download por pasta.
        csv_download = pd.read_csv(os.path.join(folder_path, csv_download[0]))
        return dict(download_csv=csv_download, videos=video_files, eaf=eaf_files)

    @staticmethod
    def _group_video_name_2_his_own_url(videos_list: list, download_df: pd.DataFrame):
        """

        Parameters
        ----------

        videos_list : List

        download_df : pd.DataFrame

        Returns:
        """
        videos_url = download_df[download_df.video == 1]
        videos_url = videos_url.files.unique().tolist()
        url_video_group = []
        for url in videos_url:
            file_name = url.split('/')[-1].split('.mp4')[0]
            for video_path in videos_list:
                if file_name in video_path:
                    url_video_group.append(dict(video=video_path, url=url))

        return url_video_group

    @staticmethod
    def _group_eaf_name_2_his_own_url(eaf_list: list, download_df: pd.DataFrame):
        """

        Parameters
        ----------

        videos_list : List

        download_df : pd.DataFrame

        Returns:
        """
        eaf_url = download_df[download_df.video == 0]
        eaf_url = eaf_url.files.unique().tolist()
        url_video_group = []
        for url in eaf_url:
            file_name = url.split('/')[-1].split('?')[0]
            eaf_path_bound = -4 if '.eaf' in file_name or '.EAF' in file_name else len(file_name)
            file_name = file_name[:eaf_path_bound]

            for eaf_path in eaf_list:
                eaf_path_bound = -4 if '.eaf' in eaf_path or '.EAF' in eaf_path else len(eaf_path)
                eaf_path = eaf_path[:eaf_path_bound]
                if file_name in eaf_path:
                    url_video_group.append(dict(eaf=eaf_path, url=url))

        return url_video_group

    @staticmethod
    def _check_single_eaf_integrity(eaf_path):
        try:
            et.parse(eaf_path + '.EAF')
            return True
        except et.ParseError:
            return False

    @staticmethod
    def _check_single_video_integrity(vid_path: str, end_msec: int, pbar: tqdm=None):
        """

        Parameters
        ----------
        vid_path : str
        end_msec : int
        pbar : tqdm

        Returns
        -------

        """
        vid = cv.VideoCapture(vid_path)
        ret, frame = vid.read()

        if not ret:
            return False

        last_msec = vid.get(cv.CAP_PROP_POS_MSEC)
        if pbar is not None:
            pbar.reset(total=end_msec)
            pbar.update(last_msec)
            pbar.refresh()

        while vid.get(cv.CAP_PROP_POS_MSEC) <= end_msec:
            ret, frame = vid.read()
            if not ret:
                return False

            curr_msec = vid.get(cv.CAP_PROP_POS_MSEC)

            if pbar is not None:
                pbar.update(int(curr_msec - last_msec))
                pbar.refresh()

            last_msec = curr_msec

        return True

if __name__ == '__main__':
    a = CheckDBBaseItemsIntegrity('/media/usuario/Others/gdrive/', '../LibrasDB/all_videos.csv')
    a.process()

# sanskara é uma conexão mental que n foi realizada ainda. Conjunto de cicatrizes e marcas por habitos, Marcas Karmicas. Avidhja.
# Os cincos skandas funciam por sanskara.
