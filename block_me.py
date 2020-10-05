"""
The purpose of this file is to block our crawler!
We need Info like 1. Internet time 2. #connections 3. Type of blocking
"""
import re
import multiprocessing
import zlib

from crawler import WebPage, pretty_url, is_good_link, get_all_links_from_content
import requests

"""
This is initial seed; its mean we start crawling from this url.
This is URL (str) value.
"""
INITIAL_SEED = WebPage(url=pretty_url('https://namnak.com'))

"""
Time-out is number od seconds that we wait for respond.
This is an int.
"""
TIME_OUT = 30

# This is main function by running this function you will get the output.
"""
seeds is list of links that will be crawl.
The initial seed is the first element of the list
"""
seeds = [INITIAL_SEED]

file = open('webpage.csv', 'a')


def crawler():
    file.write('url,out-degree,size,compressed-sized,status_code,crawled\n')
    # For loop on seeds to crawl
    # This is a queue, We are using BFS
    for seed in seeds:
        try:
            s = seed
            del seed
            # check if this is a good link to crawl
            if is_good_link(s.url, INITIAL_SEED.url):
                # get page content of a page
                res = requests.get(s.url, timeout=TIME_OUT)
                # set s status_code
                s.status_code = res.status_code
                # check if respond is 200
                print(res.status_code)
                if res.status_code == 200:
                    # set size of page content
                    s.size = len(res.text.encode('utf-8')) / 1000
                    # compress context by removing white space
                    compressed = re.sub(r'\w+', '', res.text)
                    # convert str to byte because compressing works with bytes
                    compressed = str.encode(compressed)
                    # compressing
                    compressed = zlib.compress(compressed, level=9)
                    # saving compressed sized in s
                    s.compressed_sized = len(compressed) / 1000
                    # get all links from this res
                    discovered_links = get_all_links_from_content(res.text, domain=INITIAL_SEED.url)
                    # out_degree for this page
                    out_degree = 0
                    # loop throw discovered_links
                    for d_link in discovered_links:
                        # use pretty url
                        d_link = pretty_url(d_link)
                        # check if link is not in seeds
                        if WebPage(d_link) not in seeds:
                            # add web page to ss
                            seeds.append(WebPage(url=d_link))
                            # add discovered_links_counter
                            out_degree += 1
                    # set out-degree for s
                    s.out_degree = out_degree
            file.write(str(s) + '\n')
        except Exception as e:
            print(str(e))


def main():
    start_url_index = 0
    while len(seeds) < 100:
        try:
            respond = requests.get(seeds[start_url_index].url)
            if respond.status_code == 200:
                links = get_all_links_from_content(respond.text, INITIAL_SEED.url)
                for link in links:
                    if WebPage(url=link) not in seeds:
                        seeds.append(WebPage(url=link))
            print(len(seeds))
        except Exception as e:
            print(str(e))
        start_url_index += 1

    list_thread = []
    for _ in range(100):
        list_thread.append(multiprocessing.Process(target=crawler))

    for t in list_thread:
        t.start()

    for t in list_thread:
        t.join()


if __name__ == '__main__':
    main()
