# -*- coding: utf-8 -*-
import scrapy
import os
import pandas as pd
import unicodedata
from copy import copy


def decorate(func, param):
    def wrapper(p):
        return func(p, **param) if isinstance(param, dict) else func(p, param)

    return wrapper


class LibrasCorpusSpider(scrapy.Spider):
    name = 'LibrasCorpus'
    allowed_domains = ['corpuslibras.ufsc.br']

    download_dataframe = pd.DataFrame(columns=['estate', 'project', 'item_name',
                                               'subs', 'video'])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.curr_url = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
                        '/{}?page=1'

        self.db = './db/{}'

    def start_requests(self):
        # Aki começa a pegar nome de todos os estados disponiveis para crawl
        # as paginas disponiveis.

        # yield scrapy.Request('', self.make_login)
        # projs_df = pd.read_csv('all_projects.csv', index_col=0)
        # projs_df = projs_df.drop_duplicates()
        # for row in projs_df.iterrows():
        #     row = row[1]
        #     if row.project.isna():
        #         continue

            # url = copy(self.curr_url).format(row.project)
        url_base = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
                   '/Invent%C3%A1rio+Libras?page={}'

        url = 'http://corpuslibras.ufsc.br/dados/dado/porprojeto' \
              '/Invent%C3%A1rio+Libras?page=1'
        fn_cb = decorate(self.parse, dict(estate_name='Santa Catarina',
                                          project_name='Inventario Libras',
                                          base_page_url=url_base,
                                          cur_page_pos=1))
        yield scrapy.Request(url, fn_cb)

    def make_login(self, response):
        token_xpath = '//input[@name="_csrf"]/@value'
        token = response.xpath(token_xpath).getfirst()
        login_form_data = {
            '_csrf': token,
            'LoginForm[usuario]': 'lafa@ic.ufal.br',
            'LoginForm[senha]': '12345'
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
            url = base_page_url.format(cur_page_pos + 1)
            fn_cb = decorate(self.parse, dict(estate_name='Santa Catarina',
                                              project_name='Inventario Libras',
                                              base_page_url=base_page_url,
                                              cur_page_pos=cur_page_pos + 1))
            yield scrapy.Request(url, fn_cb)


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

