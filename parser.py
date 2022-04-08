#!/usr/bin/env python3

# Author: interlark@gmail.com
# Description: Parser for advertisments on https://rus-work.com
# Disclaimer: For education purpose only
# Version: 1.0.5

# Usage:
# parser.py Пенза results_penza.csv
# parser.py Архангельск results_arckhangelsk.csv

import csv
import json
import os
import re
import sys
from argparse import ArgumentParser
from time import sleep
from urllib.parse import urlparse, urljoin

ADV_FIELDS = ['Пол', 'Возраст', 'Образование', 'Опыт работы',  'Компания', 'График работы', 'Зарплата', 'Телефон', 'Контактное лицо', 'E-mail', 'Вакансия размещена', 'Адрес', 'Занятость']
OUTPUT_FIELDS = ['Вакансия', 'Компания', 'Опыт работы', 'График работы', 'Занятость', 'Адрес', 'Город', 'Регион', 'E-mail', 'Телефон', 'Ссылка']
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CITIES_PATH = os.path.join(SCRIPT_DIR, 'cities.json')

ADV_PER_PAGE = 10  # Number of advertisments per page

from bs4 import BeautifulSoup
from tqdm import tqdm
import asyncio
import aiohttp
import requests
from pyfiglet import Figlet

def download_pages(urls, n_parallel):
    if not urls:
        return []
    
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
    page = requests.get(base_url + '/vakansii/?p=1')
    soup = BeautifulSoup(page.text, 'html.parser')
    filter_counter = soup.select_one('.cnt_line .tit')
    counter_match = re.search('[\s\d]+', filter_counter.text)
    
    if counter_match:
        counter_text = counter_match.group(0)
        counter_text = re.sub('\s', '', counter_text)
        if counter_text:
            max_page = int(int(counter_text) / ADV_PER_PAGE + 1)
        else:
            max_page = 0
    
    yield 1, max_page, page
    
    for n_page in range(2, max_page + 1):
        page = None
        while page is None:
            try:
                page = requests.get(base_url + f'/vakansii/?p={n_page}')
            except requests.ConnectionError:
                # retry download
                sleep(1)

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
    top_params, addr_params, contact_params = [], [], []

    # Company block
    top_node = soup.select('.card_ogz > div')
    if top_node:
        top_params = [re.sub(r'[\n\t\r\xa0]+', '', x.text) for x in top_node]
    
    # Address block
    addr_node = soup.select('.card_adr')
    if addr_node:
        addr_params = [re.sub(r'[\n\t\r\xa0]+', '', x.text) for x in addr_node]
    
    # Contacts block
    contact_block = soup.select('.card_contact')
    if contact_block:
        contact_params = [re.sub(r'[\n\t\r\xa0]+', '', x.text) for x in contact_block]
        # contact parameters are combined into one big text splitted by <br/>,
        # we need to split them first
        contact_params = [BeautifulSoup(x, 'html.parser').text.strip() for x in str(contact_block).strip('][').split('<br/>')]
    
    params = top_params + addr_params + contact_params
    data = {}
    for param in params:
        for key in ADV_FIELDS:
            if param.startswith(key + ':'):
                data[key] = param[len(key) + 1:].strip()
                break

    data['Вакансия'] = soup.select_one('.vid_tit').text

    resp_url_path = soup.select_one('.otklik > a')
    if resp_url_path:
        data['Отклик URL_Path'] = resp_url_path.attrs['href']
    
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
                data_list = []
                pages_links = [urljoin(base_url, x.attrs['href']) for x in soup.select('.v_box > .v_name > a')]
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
                    data_list.append(data)

                # parallel retrieving contact pages (the rest info we need)
                pages_links = [urljoin(x['Ссылка'], x['Отклик URL_Path']) for x in data_list]
                ads_data = download_pages(pages_links, n_parallel)
                for (_, ad_page), data in zip(ads_data, data_list):
                    data_contacts = extract_data(ad_page)
                    data = {**data, **data_contacts}
                    del data['Отклик URL_Path']
                    save_data(f_writer, data)
                    f_out.flush()
                
                pbar.update()
                sleep(0.1)

def is_url(path):
    return urlparse(path).scheme != ''

def get_city_url(name):
    with open(CITIES_PATH, 'r', encoding='utf-8') as f_cities:
        cities_data = json.load(f_cities)
        if name in cities_data:
            return cities_data[name]['url']
        else:
            for k, v in cities_data.items():
                if k.lower() == name:
                    return v['url']
    
    return None

def get_all_urls():
    with open(CITIES_PATH, 'r', encoding='utf-8') as f_cities:
        cities_data = json.load(f_cities)
        return [x['url'] for x in cities_data.values()]  

def get_region_and_city(url):
    with open(CITIES_PATH, 'r', encoding='utf-8') as f_cities:
        cities_data = json.load(f_cities)
        for city_name, city_data in cities_data.items():
            region, city_url = city_data['region'], city_data['url']
            if url == city_url:
                return region, city_name
    return None, None

if __name__ == '__main__':
    argparser = ArgumentParser(description='Ruswork Parser', epilog='Пример запуска: parser.py Пермь out_perm.csv')
    argparser.add_argument('path', type=str, help='URL или Название города')
    argparser.add_argument('output_path', type=str, default='parser_result.csv', help='Путь до выходной таблицы резултьтатов парсера')
    argparser.add_argument('--n-parallel', type=int, default=10, help='Количество одновременных загрузок объявлений со страницы')
    argparser.add_argument('--output-encoding', type=str, default='utf8', help='Кодировка результирующей таблицы')
    
    args = argparser.parse_args()
    if args.n_parallel <= 0:
        print('Параметр --n-parallel должен быть неотрицательным', file=sys.stderr)
        exit(-1)

    figlet = Figlet(font='slant')
    print(figlet.renderText('RUSWORK parser'), file=sys.stderr)

    # class Dummy:
    #     pass
    # args = Dummy()
    # args.path = 'https://mirniy-arhangelsk.rus-work.com'
    # args.path = 'пермь'
    # args.n_parallel = 5
    # args.output_path = 'parser_result.csv'
    # args.output_encoding = 'utf-8'

    print('path =', args.path, file=sys.stderr)
    print('output_path =', args.output_path, file=sys.stderr)
    print('output_encoding =', args.output_encoding, file=sys.stderr)
    print('n_parallel =', args.n_parallel, file=sys.stderr)

    if args.path.lower() == 'all':
        urls = get_all_urls()
        print(f'Запуск парсера по {len(urls)} URL-адресам', file=sys.stderr)
        for url in urls:
            site_parse(url, output_path=args.output_path, n_parallel=args.n_parallel)
    elif is_url(args.path):
        print('Запуск парсера по URL-адресу:', args.path, file=sys.stderr)
        site_parse(args.path, output_path=args.output_path, n_parallel=args.n_parallel)
    elif os.path.isfile(args.path):
        print('Запуск парсера по спику URL-адресов:', args.path, file=sys.stderr)
        with open(args.path, 'r') as f_urls:
            urls = [x.rstrip() for x in f_urls.readlines()]
            print(f'Запуск парсера по {len(urls)} URL-адресам', file=sys.stderr)
            for url in urls:
                site_parse(url, output_path=args.output_path, n_parallel=args.n_parallel)
    else:
        print('Запуск парсера по названию города:', args.path, file=sys.stderr)
        url = get_city_url(args.path)
        if url is None:
            print(f'URL города {args.path} не найден!', file=sys.stderr)
            exit(1)
        site_parse(url, output_path=args.output_path, n_parallel=args.n_parallel)
        
