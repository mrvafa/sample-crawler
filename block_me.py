"""
The purpose of this file is to block our crawler!
We need Info like 1. Internet time 2. #connections 3. Type of blocking
"""
import re
import threading
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


# Create file
def crawler():
    file = open('webpage.csv', 'a')
    file.write('url,out-degree,size,compressed-sized,status_code,crawled\n')
    # For loop on seeds to crawl
    # This is a queue, We are using BFS
    for seed in seeds:
        try:
            # check if this is a good link to crawl
            if is_good_link(seed.url, INITIAL_SEED.url):
                # get page content of a page
                res = requests.get(seed.url, timeout=TIME_OUT)
                # set seed status_code
                seed.status_code = res.status_code
                # check if respond is 200
                if res.status_code == 200:
                    # set size of page content
                    seed.size = len(res.text.encode('utf-8')) / 1000
                    # compress context by removing white space
                    compressed = re.sub(r'\w+', '', res.text)
                    # convert str to byte because compressing works with bytes
                    compressed = str.encode(compressed)
                    # compressing
                    compressed = zlib.compress(compressed, level=9)
                    # saving compressed sized in seed
                    seed.compressed_sized = len(compressed) / 1000
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
                            # add web page to seeds
                            seeds.append(WebPage(url=d_link))
                            # add discovered_links_counter
                            out_degree += 1
                    # set out-degree for seed
                    seed.out_degree = out_degree
        except Exception as e:
            print(str(e))
            file.write(str(seed) + '\n')
        del seed


list_thread = []
for _ in range(100):
    list_thread.append(threading.Thread(target=crawler))

for t in list_thread:
    t.start()

for t in list_thread:
    t.join()
