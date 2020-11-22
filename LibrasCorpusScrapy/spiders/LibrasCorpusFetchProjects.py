import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from copy import copy
import json
import pandas as pd


def decorate(func, param):
    def wrapper(p):
        return func(p, **param) if isinstance(param, dict) else func(p, param)

    return wrapper


class LibrasCorpusFetchProjects(scrapy.Spider):
    name = 'LibrasCorpusFetchProjects'

    allowed_domains = ['corpuslibras.ufsc.br']
    all_estates_page_url = 'http://corpuslibras.ufsc.br/dados'
    base_estate_url = 'http://corpuslibras.ufsc.br/dados/projeto/' \
                      'porestado?term={}'
    all_estates = []
    start_urls = ['http://corpuslibras.ufsc.br/dados']
    all_estates_projects = {}

    def parse(self, response, **kwargs):
        estates_xpath = '//map[@id="mapBrasil"]/area/@alt'
        self.all_estates = response.xpath(estates_xpath).getall()
        for estate in self.all_estates:
            cur_estate_url = copy(self.base_estate_url).format(estate)
            cb_fn = decorate(self.parse_projects, estate)
            yield scrapy.Request(cur_estate_url,
                                 callback=cb_fn)

    def parse_projects(self, response, estate_name):
        pages = json.loads(response.text)
        estate_projs_url = []
        estate_projs_ids = []
        if len(pages['data']) > 0:
            for d in pages['data']:
                estate_projs_url.append(d['label'])
                estate_projs_ids.append(d['id'])

        curr_estate_projects = {estate_name: (estate_projs_url,
                                              estate_projs_ids)}
        self.all_estates_projects.update(curr_estate_projects)
        if len(self.all_estates_projects) == len(self.all_estates):
            estates_projects_df = pd.DataFrame(columns=['estate', 'project',
                                                        'id'])
            for estate, proj_id in self.all_estates_projects.items():
                projs = proj_id[0]
                ids = proj_id[1]

                estate_name_list = [estate] * (len(projs)
                                               if len(projs) > 0 else 1)
                projs = projs if len(projs) > 0 else [None]
                ids = ids if len(ids) > 0 else [None]

                estates_projects_df = estates_projects_df.append(
                    pd.DataFrame(data=dict(estate=estate_name_list,
                                           project=projs,
                                           id=ids)),
                    ignore_index=True)

            estates_projects_df.to_csv('all_projects.csv')


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl('LibrasCorpusFetchProjects')
    process.start()
