# -*- coding: utf-8 -*-
import scrapy
import os
import urllib3
import sys
import time
import unicodedata
import json
import pandas as pd
from tqdm import tqdm
from pget.down import Downloader


def decorate(func, param):
    def wrapper(p):
        return func(p, **param) if isinstance(param, dict) else func(p, param)

    return wrapper


class LibrasCorpusSpider(scrapy.Spider):
    name = 'LibrasCorpus'
    allowed_domains = ['http://corpuslibras.ufsc.br/']
    http = urllib3.PoolManager()
    all_estates = []

    retry_files = []
    download_dataframe = pd.DataFrame(columns=['estate', 'project',
                                               'subs', 'video'])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.curr_url = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
                        '/{}?page=1'

        self.url_page = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
                        '/{}?page=1'
        self.db = '/media/lucas/Others/LibrasCorpus/{}'
        # '/run/media/lucas/04B40D7AB40D700A/Users/lucas/Documents/Projects/
        # LibrasCorpusScrapy/db/{}'
        self.all_pages_name = []

    def start_requests(self):
        # Aki começa a pegar nome de todos os estados disponiveis para crawl
        # as paginas disponiveis.
        while self.curr_url is not None:
            if len(self.all_estates) == 0:
                all_estates_page_url = 'http://corpuslibras.ufsc.br/dados'
                yield scrapy.Request(all_estates_page_url,
                                     self.parse_all_estates_name)
            else:
                estate_url = 'http://corpuslibras.ufsc.br/dados/projeto/' \
                             'porestado?term={}'

                for estate in self.all_estates:
                    yield scrapy.Request(estate_url.format(estate),
                                         decorate(self.parse_each_estate_page,
                                                  estate))

                for page_name in self.all_pages_name:
                    for pages in page_name['urls']:
                        fn_cb = decorate(self.parse_video_page,
                                         dict(page_name=page_name['name'],
                                              project_name=pages))
                        yield scrapy.Request(self.url_page.format(pages), fn_cb)
                        while self.curr_url is not None:
                            # aqui não precisa atualizar com .format page_name
                            # pois ja é atualizado com url next
                            fn_cb = decorate(self.parse_video_page,
                                             dict(page_name=page_name['name'],
                                                  project_name=pages))
                            yield scrapy.Request(self.curr_url, fn_cb)

    def parse_all_estates_name(self, response):
        estates_xpath = '//map[@id="mapBrasil"]/area/@alt'
        self.all_estates = response.xpath(estates_xpath).getall()

    def parse_each_estate_page(self, response, page_name):
        pages = json.loads(response.text)
        if len(pages['data']) > 0:
            urls_names = []
            for d in pages['data']:
                urls_names.append(d['label'])
            self.all_pages_name.append({'name': page_name,
                                        'urls': urls_names})

    def parse_video_page(self, response, page_name, project_name):
        """
        Aqui a pagina principal de cada projeto por estado é crawlada.
        Encontrando cada video e legendas respectivamentes é passado url e nome
        das legendas e videos para ser feito o download. Ao acabar a pagina
        é verificado se há mais paginas dentro do projeto a serem crawladas mais
        uma requisição é feita. A requisição é feita alterando o estado atual
        da variavel self.curr_url
        
        :param response
        :param page_name
        :param project_name
        :returns
        """
        print('downloading page {} project {}'.format(page_name, project_name))
        data_keys = [int(dk) for dk in
                     response.xpath('//div[@data-key]/@data-key').getall()]

        base_xpath = '//div[@data-key="{}"]/div[@class="view data-view"]/' \
                     'div[@id="metadados"]/'

        for key, dk in enumerate(data_keys):
            curr_xpath = base_xpath.format(dk)
            name_xpath = curr_xpath + 'h3/span/text()'
            video_xpath = curr_xpath + 'div[@id="video-tab"]/' \
                                       'div[@class="tab-content"]/' \
                                       'div[@id="w{}-tab{}"]/video/source/@src'

            name = unicodedata.normalize('NFC',
                                         response.xpath(name_xpath).get())

            subtitles_path = base_xpath.format(dk) + 'span/p/span/a/@href'
            subtitles = response.xpath(subtitles_path).getall()

            curr_dir = os.path.join(self.db.format(page_name), project_name,
                                    name + 'v' + str(dk))

            tab_xpath = curr_xpath + 'div[@id="video-tab"]/' \
                                     'div[@class="tab-content"]'

            amount_video_tab = len(response.xpath(tab_xpath).getall())
            downloads_queue = []

            video_mp4_path = None
            subtitles_urls = []
            video = False
            if subtitles:
                for k, s in enumerate(subtitles):
                    all_url_sub = self.allowed_domains[0] + s[1:]
                    subtitle_file_path = os.path.join(curr_dir, 'sub' + str(k))
                    downloads_queue.append({'url': all_url_sub,
                                            'file': subtitle_file_path + '.xml'})
                    subtitles_urls.append(all_url_sub)

                subtitles_urls = ''.join(x + '||' for x in subtitles_urls)

                for tab in range(amount_video_tab):
                    video_xpath = video_xpath.format(key + 1, tab)
                    video_url = response.xpath(video_xpath).get()
                    if video_url:
                        video = True
                    else:
                        continue

                    cut = video_url.find('mp4')
                    video_mp4_path = os.path.join(curr_dir,
                                                  'v' + str(tab) + '.mp4')
                    downloads_queue.append({'url': video_url[:cut + 3],
                                            'file': video_mp4_path})

            if subtitles and video:
                if not os.path.exists(curr_dir):
                    os.makedirs(curr_dir)
                self.download_queued_files(downloads_queue)
                single_line_df = pd.DataFrame(data=dict(estate=[page_name],
                                                        project=[project_name],
                                                        video=[video_mp4_path],
                                                        subs=[subtitles_urls]))
                self.download_dataframe = \
                    self.download_dataframe.append(single_line_df,
                                                   ignore_index=True)
                print(self.download_dataframe)
            else:
                print('sub {}, video {}, tabs {}'.format(subtitles, video,
                                                         amount_video_tab))

        if not response.xpath('//li[@class="next"]/a/@href'):
            self.curr_url = None
        else:
            next_xpath = '//li[@class="next"]/a/@href'
            self.curr_url = response.urljoin(response.xpath(next_xpath).get())

    def download_queued_files(self, files_queue_dict):
        """
        Faz os downloads dentro do files_queue_dict, checa se o arquivo ja
        existe antes de fazer o download para não baixar nada desnecessario.

        :param files_queue_dict fila contendo um dicionario com url e nome dos
                                arquivos a serem baixados.
        """
        for dt in files_queue_dict:
            if os.path.exists(dt['file']):
                continue

            try:
                r = self.http.request('GET', dt['url'], preload_content=False)
                with open(dt['file'], 'wb') as file:
                    content_bytes = r.headers.get('Content-Length')
                    if content_bytes == 0:
                        continue

                    loops = (int(content_bytes) // 128) \
                        if content_bytes is not None else 1000

                    for _ in tqdm(range(loops)):
                        data = r.read(128)
                        if not data:
                            break
                        file.write(data)
                    r.release_conn()
            except:
                print('error', dt['file'], dt['url'])
                self.retry_files.append(dt)

    @staticmethod
    def download_queued_files_pget(files_queue_dict):
        """
        Baixa os arquivos emfileirados checando se ja não foi baixado
        anteriormente. Utiliza o pget para fazer os downloads. Pget é uma
        biblioteca semelhante ao wget (linha de comando do linux) entretanto
        baixa os arquivos em lotes, podendo ter um numero enorme de threads
        baixando o arquivo e podendo funcionar asincronamente.

        :param files_queue_dict fila contendo um dicionario com url e nome dos
                                arquivos a serem baixados.
        """
        for dt in files_queue_dict:
            if os.path.exists(dt['file']):
                continue

            print('downloading {}'.format(dt['file']))
            downloader = Downloader(dt['url'], dt['file'], 8)
            downloader.start()
            downloader.wait_for_finish()

