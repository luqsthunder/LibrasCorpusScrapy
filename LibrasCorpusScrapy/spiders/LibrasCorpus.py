# -*- coding: utf-8 -*-
import scrapy
import os
import urllib3
import sys
import time
import unicodedata
from tqdm import tqdm
import json


def decorate(func, param):
    def wrapper(p):
        return func(p, param)

    return wrapper

class LibrasCorpusSpider(scrapy.Spider):
    name = 'LibrasCorpus'
    allowed_domains = ['http://corpuslibras.ufsc.br/']
    http = urllib3.PoolManager()
    all_estates = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.curr_url = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
                        '/{}?page=1'
        self.db = 'db/{}'
        self.all_pages_name = []

    def start_requests(self):
        # Começamos a pegar nome de todos os estados disponiveis para crawl
        # as paginas disponiveis.
        while self.curr_url is not None:
            if len(self.all_estates) == 0:
                all_estates_page_url = 'http://corpuslibras.ufsc.br/dados'
                yield scrapy.Request(all_estates_page_url,
                                     self.parse_all_estates_name)
            else:
                estate_url = \
                    'http://corpuslibras.ufsc.br/dados/projeto/porestado?term={}'
                for estate in self.all_estates:
                    yield scrapy.Request(estate_url.format(estate),
                                         decorate(self.parse_each_estate_page,
                                                  estate))

                for page_name in self.all_pages_name:
                    for pages in page_name['urls']:
                        yield scrapy.Request(self.curr_url.format(pages),
                                             decorate(self. parse_video_page,
                                                      page_name['name']))
                    while self.curr_url is not None:
                        # aqui n precisa atualizar com .format page_name pois
                        # ja é atualizado com url next
                        yield scrapy.Request(self.curr_url,
                                             decorate(self.parse_video_page,
                                                      page_name['name']))

    def parse_all_estates_name(self, response):
        estates_xpath = '//map[@id="mapBrasil"]/area/@alt'
        self.all_estates = response.xpath(estates_xpath).getall()

    def parse_each_estate_page(self, response, page_name):
        print(page_name)
        pages = json.loads(response.text)
        if len(pages['data']) > 0:
            urls_names = []
            for d in pages['data']:
                urls_names.append(d['label'])
            self.all_pages_name.append({'name': page_name, 'urls': urls_names})

    def parse_video_page(self, response, page_name):
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

            curr_dir = os.path.join(self.db.format(page_name),
                                    name + 'v' + str(dk))

            tab_xpath = curr_xpath + 'div[@id="video-tab"]/' \
                                     'div[@class="tab-content"]'

            amount_video_tab = len(response.xpath(tab_xpath).getall())
            downloads_queue = []

            video = False
            if subtitles:
                for k, s in enumerate(subtitles):
                    all_url_sub = self.allowed_domains[0] + s[1:]
                    subtitle_file_path = os.path.join(curr_dir, 'sub' + str(k))
                    downloads_queue.append({'url': all_url_sub,
                                            'file': subtitle_file_path})

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

        if not response.xpath('//li[@class="next"]/a/@href'):
            self.curr_url = None
        else:
            next_xpath = '//li[@class="next"]/a/@href'
            self.curr_url = response.urljoin(response.xpath(next_xpath).get())

    def download_queued_files(self, files_queue_dict):
        """
        Faz os downloads dentro do files_queue_dict, checa se o arquivo ja
        existe antes de fazer o download para não baixar nada desnecessario.
        """
        for dt in files_queue_dict:
            if os.path.exists(dt['file']):
                continue

            r = self.http.request('GET', dt['url'], preload_content=False)

            with open(dt['file'], 'wb') as file:
                content_bytes = r.headers.get('Content-Length')
                if content_bytes == 0:
                    continue

                loops = int(content_bytes) \
                    if content_bytes is not None else 100000

                for _ in tqdm(range(loops)):
                    data = r.read(128)
                    if not data:
                        break
                    file.write(data)
            r.release_conn()


