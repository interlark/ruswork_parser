#!/usr/bin/env python3

# Usage:
# parser.py город Пенза results_penza_city.csv
# parser.py регион Архангельск results_arckhangelsk_dist.csv

import csv
import json
import os
import re
import sys
from argparse import ArgumentParser
from time import sleep
from urllib.parse import urlparse

OUTPUT_FIELDS = ['Ссылка', 'Регион', 'Город', 'Компания', 'Вакансия', 'Телефон', 'E-mail', 'Контактное лицо', 'Вакансия размещена']
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
REGIONS_PATH = os.path.join(SCRIPT_DIR, 'regions.json')

from bs4 import BeautifulSoup
from tqdm import tqdm
import asyncio
import aiohttp
import requests
from pyfiglet import Figlet

def download_pages(urls, n_parallel):
    sema = asyncio.BoundedSemaphore(n_parallel)

    async def fetch_page(url):
        data = None
        while data is None:
            try:
                async with sema, aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        assert resp.status == 200
                        data = await resp.text()
            except aiohttp.ClientError:
                # retry download
                await asyncio.sleep(1)
        
        return url, data

    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(fetch_page(url)) for url in urls]
    pages = loop.run_until_complete(asyncio.gather(*tasks))
    return pages

def page_iterator(base_url):
    page = requests.get(base_url + '/vakansii/p1.html')
    page.encoding = '1251'  # cyrillic quickfix
    soup = BeautifulSoup(page.text, 'html.parser')
    filter_counter = soup.select_one('.filter .v_cnt')
    counter_match = re.search('[\s\d]+$', filter_counter.text)
    
    if counter_match:
        counter_text = re.search('[\s\d]+$', filter_counter.text).group(0)
        counter_text = re.sub('\s', '', counter_text)
        max_page = int(int(counter_text) / 10 + 1)
    
    yield 1, max_page, page
    for n_page in range(2, max_page + 1):
        page = None
        while page is None:
            try:
                page = requests.get(base_url + f'/vakansii/p{n_page}.html')
            except requests.ConnectionError:
                # retry download
                sleep(1)

        page.encoding = '1251'  # cyrillic quickfix
        yield n_page, max_page, page

def save_data(f_writer, data):
    output = []
    for f in OUTPUT_FIELDS:
        if f in data:
            output.append(data[f])
        else:
            output.append('NULL')

    f_writer.writerow(output)

def extract_data(ad_page):
    soup = BeautifulSoup(ad_page, 'html.parser')
    param_keys = [
        'Пол', 'Возраст', 'Образование', 'Опыт работы', 
        'Компания', 'График работы', 'Зарплата', 'Телефон',
        'Контактное лицо', 'E-mail', 'Вакансия размещена',
    ]
    top_params = [re.sub(r'[\n\t\r\xa0]+', '', x.text) for x in soup.select('.param_top > .v_param')]
    bottom_params = [re.sub(r'[\n\t\r\xa0]+', '', x.text) for x in soup.select('.param_box > .v_param')]
    params = top_params + bottom_params
    data = {}
    for param in params:
        for key in param_keys:
            if param.startswith(key + ':'):
                data[key] = param[len(key) + 1:].strip()
                break

    data['Вакансия'] = soup.select_one('.box.page > h1').text
    return data

def site_parse(base_url, output_path, n_parallel=5):
    # check for trailing slash
    base_url = base_url.rstrip('/')

    # get region and the city
    region, city = get_region_and_city(base_url)

    # write header
    if not os.path.isfile(output_path):
        with open(output_path, 'w', encoding=args.output_encoding) as f_out:
            f_writer = csv.writer(f_out)
            f_writer.writerow(OUTPUT_FIELDS)

    # open output path for appending data
    with open(output_path, 'a', encoding=args.output_encoding) as f_out:
        f_writer = csv.writer(f_out)
        with tqdm(total=0, unit='стр.', desc='Парсинг') as pbar:
            for n_page, max_page, page in page_iterator(base_url):
                if n_page == 1:
                    pbar.total = max_page
                soup = BeautifulSoup(page.text, 'html.parser')
                pages_links = [base_url + x.attrs['href'] for x in soup.select('.v_box > a')]
                
                ads_data = download_pages(pages_links, n_parallel)
                for url, ad_page in ads_data:
                    data = extract_data(ad_page)
                    data = {
                        **data, 
                        **{
                            'Город': city,
                            'Регион': region,
                            'Ссылка': url,
                        }
                    }
                    save_data(f_writer, data)
                    f_out.flush()
                
                pbar.update()
                sleep(0.1)

def get_region_and_city(base_url):
    with open(REGIONS_PATH, 'r', encoding='utf-8') as f_regions:
        regions_data = json.load(f_regions)
        for region_data in regions_data:
            for city_data in region_data['cities']:
                if city_data['url'] == base_url:
                    return region_data['name'], city_data['name']
    
    return 'NULL', 'NULL'

def get_url_by_region_or_city(name, type):
    urls = []
    with open(REGIONS_PATH, 'r', encoding='utf-8') as f_regions:
        regions_data = json.load(f_regions)
        if type.upper() == 'РЕГИОН':
            regions = [x for x in regions_data if x['name'].upper() == name.upper()]
            if len(regions) > 0:
                region = regions[0]
                urls = [x['url'] for x in region['cities']]
        else:
            for region_data in regions_data:
                for city_data in region_data['cities']:
                    if city_data['name'].upper() == name.upper():
                        urls = [city_data['url'], ]
    
    return urls

def get_all_cities():
    urls = []
    with open(REGIONS_PATH, 'r', encoding='utf-8') as f_regions:
        regions_data = json.load(f_regions)
        for x in regions_data:
            for y in x['cities']:
                urls.append(y['url'])
            urls.append(x['url'])
    return list(set(urls))

if __name__ == '__main__':
    argparser = ArgumentParser(description='Ruswork Parser', epilog='Пример запуска: parser.py город ижевск out_izhevsk.csv')
    argparser.add_argument('target_type', type=str, choices=['регион', 'город', 'all'], help='Тип места парсинга "регион" или "город"')
    argparser.add_argument('target_place', type=str, help='Место парсинга, например "Ижевск" или "Москва"')
    argparser.add_argument('output_path', type=str, default='parser_result.csv', help='Путь до выходной таблицы резултьтатов парсера')
    argparser.add_argument('--n-parallel', type=int, default=10, help='Количество одновременных загрузок объявлений со страницы')
    argparser.add_argument('--output-encoding', type=str, default='utf-8', help='Кодировка результирующей таблицы')
    args = argparser.parse_args()
    if args.n_parallel <= 0:
        print('Параметр --n-parallel должен быть неотрицательным', file=sys.stderr)
        exit(-1)

    figlet = Figlet(font='slant')
    print(figlet.renderText('RUSWORK parser'), file=sys.stderr)

    print('target_type =', args.target_type, file=sys.stderr)
    print('target_place =', args.target_place, file=sys.stderr)
    print('output_path =', args.output_path, file=sys.stderr)
    print('output_encoding =', args.output_encoding, file=sys.stderr)
    print('n_parallel =', args.n_parallel, file=sys.stderr)

    # Парсинг всех городов
    if args.target_type == 'all' and args.target_place == 'all':
        urls = get_all_cities()
        print(f'Запуск парсера по всем городам (%d URL-адресов)' % (len(urls),), file=sys.stderr)
        for url in urls:
            site_parse(url, output_path=args.output_path, n_parallel=args.n_parallel)
        exit(0)

    # Парсинг города или целого региона
    urls = get_url_by_region_or_city(args.target_place, args.target_type)
    if len(urls) > 0:
        print(f'Запуск парсера по %s %s (%d URL-адресов)' % \
            ('региону' if args.target_type.upper() == 'РЕГИОН' else 'городу', args.target_place, len(urls)), file=sys.stderr)
        for url in urls:
            site_parse(url, output_path=args.output_path, n_parallel=args.n_parallel)
        exit(0)
    else:
        print('Город или регион не найден!', file=sys.stderr)
        exit(-1)
