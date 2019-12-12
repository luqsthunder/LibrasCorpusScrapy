# -*- coding: utf-8 -*-
import scrapy
import os
import urllib3
import sys
import time
import unicodedata
from tqdm import tqdm


class LibrasCorpusSpider(scrapy.Spider):
    name = 'LibrasCorpus'
    allowed_domains = ['http://corpuslibras.ufsc.br/']
    http = urllib3.PoolManager()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.curr_url = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto/{}?page=1'.format(self.namep)
        self.db = 'db/{}'.format(self.namep)

        if not os.path.exists(self.db):
            os.makedirs(self.db)

    def start_requests(self):
        while self.curr_url is not None:
            yield scrapy.Request(self.curr_url, self.parse)

    def parse(self, response):
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

            name = unicodedata.normalize('NFC', response.xpath(name_xpath).get())

            subtitles_path = base_xpath.format(dk) + 'span/p/span/a/@href'
            subtitles = response.xpath(subtitles_path).getall()

            curr_dir = os.path.join(self.db, name + 'v' + str(dk))

            os.makedirs(curr_dir)

            tab_xpath = curr_xpath + 'div[@id="video-tab"]/' \
                                     'div[@class="tab-content"]'

            amount_video_tab = len(response.xpath(tab_xpath).getall())
            downloads_queue = []

            if subtitles:
                for k, s in enumerate(subtitles):
                    all_url_sub = self.allowed_domains[0] + s[1:]
                    subtitle_file_path = os.path.join(curr_dir, 'sub' + str(k))
                    downloads_queue.append({'url': all_url_sub,
                                            'file': subtitle_file_path})

                for tab in range(amount_video_tab):
                    video_xpath = video_xpath.format(key + 1, tab)
                    video_url = response.xpath(video_xpath).get()
                    cut = video_url.find('mp4')
                    video_mp4_path = os.path.join(curr_dir,
                                                  'v' + str(tab) + '.mp4')
                    downloads_queue.append({'url': video_url[:cut + 3],
                                            'file': video_mp4_path})
            self.download_queued_files(downloads_queue)

        if not response.xpath('//li[@class="next"]/a/@href'):
            self.curr_url = None
        else:
            self.curr_url = response.urljoin(response.xpath('//li[@class="next"]/a/@href').get())

    def download_queued_files(self, files_queue_dict):
        for dt in files_queue_dict:
            r = self.http.request('GET', dt['url'], preload_content=False)

            with open(dt['file'], 'wb') as file:
                content_bytes = r.headers.get('Content-Length')
                loops = int(content_bytes) if content_bytes is not None else 100000
                for it in tqdm(range(loops)):
                    data = r.read(32)
                    if not data:
                        break
                    file.write(data)
            r.release_conn()


