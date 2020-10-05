"""
This code is a sample crawler without saving pages in DB.
This crawler 1. Get n page, 2. Find number of links, 3. Finds Out-degree,
4. Max, MIN, AVG Pages with/without COMPRESSION and 5. MAX, MIN , AVG url chars
"""
import requests
from bs4 import BeautifulSoup
import re
import zlib


# pretty_url is function that get site url and remove parameters.
def pretty_url(url):
    url = re.sub(r'([#?]).*?(/|$)', '/', url)
    if url.startswith('https://'):
        url = (url[:9], url[9:])
    elif url.startswith('http://'):
        url = (url[:8], url[8:])
    else:
        url = ('', url)

    url = url[0] + re.sub(f'/+', f'/', url[1])
    url = re.sub(r'/*$', '', url)
    return url


# is_good_link is function that returns True/False if the site_key in url and
# link is a HTML or web app not an image or etc
def is_good_link(link, domain):
    try:
        domain = domain[:-1] if domain.startswith('/') else domain
        if not link or 'http' not in link or not link.startswith(domain):
            return False
        res = requests.head(link, timeout=TIME_OUT)
        if res.status_code == 200:
            if 'html' in res.headers['Content-Type']:
                return True
    except Exception as e:
        print(str(e))
        return False
    return False
    # return True


# This function get page content and returns list of links.
# WARNING: There is NO CONFIDENCE that all links are valid
def get_all_links_from_content(text, domain):
    # check if domain ends with / remove it
    domain = domain[:-1] if domain.endswith('/') else domain
    # create soup obj with html.parser
    soup = BeautifulSoup(text, 'html.parser')
    # create empty links list
    links = []
    # get all <a> tags in html
    a_tags = soup.find_all('a')
    # loop throw a_tags
    for a_tag in a_tags:
        # if a tag has href attr
        if 'href' in str(a_tag):
            # check if url starts with /
            if a_tag['href'].startswith('/'):
                # append domain + link
                links.append(domain + a_tag['href'])
            else:
                # appending link to list
                links.append(a_tag['href'])
    return links


# This is class That contains a url and some more info about page.
class WebPage:

    # out-degree is number of link in the page.
    # Size of page content in KB
    # Size of page content with compressing algorithms
    # time spent to download page
    # title of page
    # Does the has timeout or any other errors
    # status code is an int that page return
    def __init__(self, url, out_degree=0, size=0, compressed_sized=0, status_code=0, crawled=False):
        self.url = url
        self.out_degree = out_degree
        self.size = size
        self.compressed_sized = compressed_sized
        self.status_code = status_code
        self.crawled = crawled

    def __str__(self):
        return f'{self.url[-20:]},{self.out_degree},{self.size},{self.compressed_sized},{self.status_code},{self.crawled}'

    def __eq__(self, other):
        return self.url == other.url


"""
This is initial seed; its mean we start crawling from this url.
This is URL (str) value.
"""
INITIAL_SEED = WebPage(url=pretty_url('https://namnak.com'))

"""
This is an int value stands for the limit of discovering pages.
The crawler will stop after NUMBER_OF_PAGES_TO_DISCOVER discover page.
This is an int value.
"""
NUMBER_OF_PAGES_TO_CRAWL = 300

"""
Time-out is number od seconds that we wait for respond.
This is an int.
"""
TIME_OUT = 30


# This is main function by running this function you will get the output.
def main():
    """
    seeds is list of links that will be crawl.
    The initial seed is the first element of the list
    """
    seeds = [INITIAL_SEED]

    """
    This is a counter to counter the number of links.
    This is an int value.
    """
    discovered_links_counter = 0

    """
    This is a counter to count number of crawled web pages
    This is an int value.
    """
    page_crawled_counter = 0

    # For loop on seeds to crawl
    # This is a queue, We are using BFS
    for seed in seeds:
        # check limit page crawl
        if page_crawled_counter < NUMBER_OF_PAGES_TO_CRAWL:
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
                        # plus previous discovered_links_counter to current out-degree
                        discovered_links_counter += out_degree
                        # crawled to True
                        seed.crawled = True
                        # add counter; crawling this page is finished
                        page_crawled_counter += 1
            except Exception as e:
                print(str(e))
    # Write info in file.
    file = open('webpage.csv', 'w')
    file.write('url,out-degree,size,compressed-sized,status_code,crawled\n')
    sum_size = 0
    sum_out_degree = 0
    sum_link_len = 0
    sum_compressed_size = 0
    for seed in seeds:
        if seed.crawled:
            file.write(str(seed) + '\n')
            sum_size += seed.size
            sum_out_degree += seed.out_degree
            sum_link_len += len(seed.url)
            sum_compressed_size += seed.compressed_sized
    file.close()
    file = open('result.txt', 'w')
    file.write(f'discovered_links_counter = {discovered_links_counter}\n')
    file.write(f'avg_size = {sum_size / NUMBER_OF_PAGES_TO_CRAWL}\n')
    file.write(f'avg_out_degree = {sum_out_degree / NUMBER_OF_PAGES_TO_CRAWL}\n')
    file.write(f'avg_link_len = {sum_link_len / NUMBER_OF_PAGES_TO_CRAWL}\n')
    file.write(f'avg_compressed_size = {sum_compressed_size / NUMBER_OF_PAGES_TO_CRAWL}\n')
    file.close()
    # Get robots.txt and save it.
    res = requests.get(INITIAL_SEED.url + '/robots.txt')
    if res.status_code == 200:
        file = open('robots.txt', 'w')
        file.write(res.text)


main()
