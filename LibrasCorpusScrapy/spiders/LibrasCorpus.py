# -*- coding: utf-8 -*-
import scrapy
import os
import pandas as pd
import unicodedata
from copy import copy

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def decorate(func, param):
    def wrapper(p):
        return func(p, **param) if isinstance(param, dict) else func(p, param)

    return wrapper


class LibrasCorpusSpider(scrapy.Spider):
    name = 'LibrasCorpus'
    allowed_domains = ['corpuslibras.ufsc.br']

    download_dataframe = pd.DataFrame(columns=['estate', 'project', 'item_name',
                                               'subs', 'video'])
    url_base = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto/{}?page={}'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.curr_url = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
                        '/{}?page=1'

        self.db = './db/{}'
        all_projects = pd.read_csv('../../all_projects.csv').drop(columns=['Unnamed: 0'])
        self.estates_project = {
            proj: [] for proj in all_projects.estate.unique().tolist()
        }

        for idx, proj in all_projects.iterrows():
            if not any(proj.isna()):
                self.estates_project[proj.estate].append(proj.project)

        self.estates = all_projects.estate.unique().tolist()

        for k in self.estates_project.keys():
            self.estates_project[k] = list(set(self.estates_project[k]))

    def start_requests(self):
        """

        :return:
        """

        for estate in self.estates:
            if len(self.estates_project[estate]) == 0:
                continue

            proj_name = self.estates_project[estate][0]
            url = copy(self.url_base).format(proj_name, 1)
            fn_cb = decorate(self.parse, dict(estate_name=estate,
                                              project_name=proj_name,
                                              base_page_url=copy(self.url_base),
                                              cur_page_pos=1))
            yield scrapy.Request(url, fn_cb)

    def make_login(self, response):
        token_xpath = '//input[@name="_csrf"]/@value'
        token = response.xpath(token_xpath).getfirst()
        login_form_data = {
            '_csrf': token,
            'LoginForm[usuario]': 'lafa@ic.ufal.br',
            'LoginForm[senha]': 'K!ll3rinstinct'
        }
        yield scrapy.FormRequest.from_response(response,
                                               formdata=login_form_data)

    def parse(self, response, estate_name=None, project_name=None,
              base_page_url=None, cur_page_pos=None):
        """
        Aqui a pagina principal de cada projeto por estado é crawlada.
        Encontrando cada video e legendas respectivamentes é passado url e nome
        das legendas e videos para ser feito o download. Ao acabar a pagina
        é verificado se há mais paginas dentro do projeto a serem crawladas mais
        uma requisição é feita. A requisição é feita alterando o estado atual
        da variavel self.curr_url

        :param response:
        :param estate_name:
        :param project_name:
        :param base_page_url:
        :param cur_page_pos:
        :return:
        """
        data_keys = [int(dk) for dk in
                     response.xpath('//div[@data-key]/@data-key').getall()]

        base_xpath = '//div[@data-key="{}"]/div[@class="view data-view"]/' \
                     'div[@id="metadados"]/'

        for key, dk in enumerate(data_keys):
            curr_xpath = copy(base_xpath).format(dk)
            name_xpath = curr_xpath  + 'h3/span/text()'

            video_xpath = curr_xpath + 'div[@id="video-tab"]/' \
                                       'div[@class="tab-content"]/' \
                                       'div[@id="{}"]/video/source/@src'

            name = unicodedata.normalize('NFC',
                                         response.xpath(name_xpath).get())

            subtitles_path = base_xpath.format(dk) + 'span/p/span/a/@href'
            subtitles = response.xpath(subtitles_path).getall()

            curr_dir = os.path.join(self.db.format(estate_name), project_name,
                                    name + 'v' + str(dk))

            tab_xpath = curr_xpath + 'div[@id="video-tab"]/' \
                                     'div[@class="tab-content"]/' \
                                     'div[contains(@class, "tab-pane")]/@id'
            video_tab_content = response.xpath(tab_xpath).getall()

            subtitles_urls = []
            videos_urls = []

            for s in subtitles:
                sub_url = self.allowed_domains[0] + '/' + s[1:]
                subtitles_urls.append(sub_url)

            for tab_href in video_tab_content:
                curr_video_xpath = copy(video_xpath).format(tab_href)
                video_url = response.xpath(curr_video_xpath).get()
                if not video_url:
                    continue
                videos_urls.append(video_url)

            if len(subtitles) > 0 and len(videos_urls) > 0:
                if not os.path.exists(curr_dir):
                    os.makedirs(curr_dir, exist_ok=True)
                files_url = []
                files_url.extend(videos_urls)

                files_url.extend(subtitles_urls)
                estate_list = [estate_name] * len(files_url)
                project_list = [project_name] * len(files_url)

                is_video = [1 if 'mp4' in x else 0 for x in files_url]
                is_sub = [0 if 'mp4' in x else 1 for x in files_url]

                single_line_df = pd.DataFrame(data=dict(estate=estate_list,
                                                        project=project_list,
                                                        files=files_url,
                                                        sub=is_sub,
                                                        video=is_video))
                single_line_df.to_csv(os.path.join(curr_dir,
                                                   'files_download.csv'))

        if response.xpath('//li[@class="next"]/a/@href'):
            url = copy(base_page_url).format(project_name, cur_page_pos + 1)
            fn_cb = decorate(self.parse, dict(estate_name=estate_name,
                                              project_name=project_name,
                                              base_page_url=base_page_url,
                                              cur_page_pos=cur_page_pos + 1))
        else:
            next_proj_index = self.estates_project[estate_name].index(project_name)
            next_proj_index = next_proj_index + 1 if len(self.estates_project[estate_name]) > next_proj_index + 1\
                                                  else None

            if next_proj_index is None:
                return

            # setando novo projeto, setando o URL para ele.
            new_proj_name = self.estates_project[estate_name][next_proj_index]
            url = copy(base_page_url).format(new_proj_name, 1)

            fn_cb = decorate(self.parse, dict(estate_name=estate_name,
                                              project_name=new_proj_name,
                                              base_page_url=base_page_url,
                                              cur_page_pos=1))
        yield scrapy.Request(url, fn_cb)


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl('LibrasCorpus')
    process.start()
