"""
This code is a sample crawler without saving pages in DB.
This crawler 1. Get n page, 2. Find number of links, 3. Finds Out-degree,
4. Max, MIN, AVG Pages with/without COMPRESSION and 5. MAX, MIN , AVG url size
"""
import requests
from bs4 import BeautifulSoup
import re
import zlib
from pymongo import MongoClient


# pretty_url is function that get site url and remove parameters.
def pretty_url(url):
    # remove parameters
    url = re.sub(r'([#?]).*?(/|$)', '/', url)
    # split http?:// and others
    if url.startswith('https://'):
        url = (url[:9], url[9:])
    elif url.startswith('http://'):
        url = (url[:8], url[8:])
    else:
        url = ('', url)
    # remove /
    url = url[0] + re.sub(f'/+', f'/', url[1])
    url = re.sub(r'/*$', '', url)
    return url


# is_good_link is function that returns True/False if the site_key in url and
# link is a HTML or web app not an image or etc
def is_good_link(link, domain):
    try:
        # remove /
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
    def __init__(self, url, out_degree=0, size=0, compressed_size=0, status_code=0, crawled=False,
                 compressed_content=''):
        self.url = url
        self.out_degree = out_degree
        self.size = size
        self.compressed_size = compressed_size
        self.status_code = status_code
        self.crawled = crawled
        self.compressed_content = compressed_content

    def __str__(self):
        return f'{self.url[-20:]},{self.out_degree},{self.size},{self.compressed_size},{self.status_code},{self.crawled}'

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


# This is function for getting crawler index
# Crawler index is an INT value that represent of current url that will be crawled
def get_crawler_index():
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_page_index = db['web_page_index']
    # If there is no index in DB
    if not web_page_index.find_one({'index': re.compile('.*')}):
        # insert 0 value for index in DB
        db['web_page_index'].insert_one({'index': '0'})
    # Read index from DB
    index = int(web_page_index.find_one({'index': re.compile('.*')})['index'])
    # Return index
    return index


# This function is going to put 0 in crawler index.
def clear_crawler_index():
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page_index']
    # If there is index in DB
    if web_crawler_web_page.find_one({'index': re.compile('.*')}):
        # Update index with index + 1
        web_crawler_web_page.update(
            {'index': re.compile(r'.*', re.IGNORECASE)},
            {'$set': {'index': str(0)}}
        )
    else:
        # insert 0 value for index in DB
        db['web_crawler_web_page'].insert_one({'index': '0'})


# This function is going to add 1 to crawler index and return new value.
def add_one_value_to_crawler_index():
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page_index']
    # If there is index in DB
    if not web_crawler_web_page.find_one({'index': re.compile('.*')}):
        # set 0 for index
        clear_crawler_index()
    # Read index from DB and PLUS by 1
    index = int(web_crawler_web_page.find_one({'index': re.compile('.*')})['index']) + 1
    # Update index with new index
    web_crawler_web_page.update_one(
        {'index': re.compile(r'.*', re.IGNORECASE)},
        {'$set': {'index': str(index)}}
    )
    # return index
    return index


# This function gets web object and write it in DB
def save_web_page_in_db(web_page):
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page']
    # get last item index of web page in DB.
    index = get_number_of_web_page_in_db()
    # create data collection
    data = {
        'index': index,
        'url': web_page.url,
        'out_degree': web_page.out_degree,
        'size': web_page.size,
        'compressed_size': web_page.compressed_size,
        'status_code': web_page.status_code,
        'crawled': web_page.crawled,
        'compressed_content': web_page.compressed_content,
    }
    # write data in collection
    web_crawler_web_page.insert_one(data)


# Update web_page with new values
def update_web_page_in_db(web_page):
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page']
    if web_crawler_web_page.find_one({'url': web_page.url}):
        web_crawler_web_page.update_one(
            {'url': web_page.url},
            {'$set': {'out_degree': web_page.out_degree,
                      'size': web_page.size,
                      'compressed_size': web_page.compressed_size,
                      'status_code': web_page.status_code,
                      'crawled': web_page.crawled,
                      'compressed_content': web_page.compressed_content, }})


# get url and check if there is web page in bd with this url
def is_url_in_web_page_db(url):
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page']
    if web_crawler_web_page.find_one({'url': url}):
        return True
    return False


# This function will delete web_crawler DB
def delete_db():
    # create mongo client
    client = MongoClient()
    # Drop database
    client.drop_database('web_crawler')


# This function is getting web_page by index.
def get_web_page_from_db_by_index(index):
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page']
    # Find web page by index
    web_page = web_crawler_web_page.find_one({'index': index})
    # Create web page Object
    web_page = WebPage(
        url=web_page['url'],
        size=web_page['size'],
        out_degree=web_page['out_degree'],
        compressed_size=web_page['compressed_size'],
        status_code=web_page['status_code'],
        crawled=web_page['crawled'],
        compressed_content=web_page['compressed_content'],
    )
    # Return web_page
    return web_page


# get len of web page in DB.
def get_number_of_web_page_in_db():
    # create mongo client
    client = MongoClient()
    # create/get database
    db = client.web_crawler
    # create/get collection
    web_crawler_web_page = db['web_page']
    # get length of web pages in DB
    length = web_crawler_web_page.estimated_document_count()
    # Return length
    return length


# This is main function by running this function you will get the output.
def main():
    # This value is representing for allowing crawling.
    # For example if the counter >= NUMBER_OF_PAGES_TO_CRAWL this value will set to False
    continue_crawling = True

    # delete db
    delete_db()

    # clear index. It will set index=0 to start from first link
    clear_crawler_index()

    # write INITIAL_SEED in DB
    save_web_page_in_db(INITIAL_SEED)

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

    # get len of web pages in db
    end_of_db_index = get_number_of_web_page_in_db()

    # get crawl index
    web_page_index = get_crawler_index()

    # While on seeds to crawl
    # This is a queue, We are using BFS
    while web_page_index < end_of_db_index and continue_crawling:
        # check limit page crawl
        if page_crawled_counter < NUMBER_OF_PAGES_TO_CRAWL:
            try:
                # get web page object from index
                web_page = get_web_page_from_db_by_index(web_page_index)
                # check if this is a good link to crawl
                if is_good_link(web_page.url, INITIAL_SEED.url):
                    # get page content of a page
                    res = requests.get(web_page.url, timeout=TIME_OUT)
                    # set web_page status_code
                    web_page.status_code = res.status_code
                    # check if respond is 200
                    if res.status_code == 200:
                        # set size of page content
                        web_page.size = len(res.text.encode('utf-8')) / 1000
                        # compress context by removing white space
                        compressed = re.sub(r'\w+', '', res.text)
                        # convert str to byte because compressing works with bytes
                        compressed = str.encode(compressed)
                        # compressing
                        compressed = zlib.compress(compressed, level=9)
                        # saving compressed sized in web_page
                        web_page.compressed_size = len(compressed) / 1000
                        # set web_page compressed content
                        web_page.compressed_content = compressed
                        # get all links from this res
                        discovered_links = get_all_links_from_content(res.text, domain=INITIAL_SEED.url)
                        # out_degree for this page
                        out_degree = 0
                        # loop throw discovered_links
                        for d_link in discovered_links:
                            # use pretty url
                            d_link = pretty_url(d_link)
                            # check if link is not in seeds
                            if not is_url_in_web_page_db(d_link):
                                # add web page to seeds
                                save_web_page_in_db(WebPage(url=d_link))
                            # add discovered_links_counter
                            out_degree += 1
                        # set out-degree for web_page
                        web_page.out_degree = out_degree
                        # plus previous discovered_links_counter to current out-degree
                        discovered_links_counter += out_degree
                        # crawled to True
                        web_page.crawled = True
                        # add counter; crawling this page is finished
                        page_crawled_counter += 1
                        # save web_page in DB
                        update_web_page_in_db(web_page)
                end_of_db_index = get_number_of_web_page_in_db()
                # Update index
                add_one_value_to_crawler_index()
                # Update web_page_index value
                web_page_index = get_crawler_index()
            except Exception as e:
                print(str(e))
        # if page_crawled_counter >= NUMBER_OF_PAGES_TO_CRAWL
        else:
            # break crawling
            continue_crawling = False
    # Write info in file.
    file = open('webpage.csv', 'w')
    # Write header for csv
    file.write('url,out-degree,size,compressed-sized,status_code,crawled\n')
    # Initial values
    sum_size = 0
    sum_out_degree = 0
    sum_link_size = 0
    sum_compressed_size = 0
    # Update len DB
    end_of_db_index = get_number_of_web_page_in_db()
    # loop throw DB
    for web_page_index in range(end_of_db_index):
        web_page = get_web_page_from_db_by_index(web_page_index)
        if web_page.crawled:
            file.write(str(web_page) + '\n')
            sum_size += web_page.size
            sum_out_degree += web_page.out_degree
            sum_link_size += len(str.encode(web_page.url)) / 1000
            sum_compressed_size += web_page.compressed_size
    file.close()
    file = open('result.txt', 'w')
    file.write(f'discovered_links_counter = {discovered_links_counter}\n')
    file.write(f'avg_size = {sum_size / NUMBER_OF_PAGES_TO_CRAWL} KB\n')
    file.write(f'avg_out_degree = {sum_out_degree / NUMBER_OF_PAGES_TO_CRAWL}\n')
    file.write(f'avg_link_size = {sum_link_size / NUMBER_OF_PAGES_TO_CRAWL} KB\n')
    file.write(f'avg_compressed_size = {sum_compressed_size / NUMBER_OF_PAGES_TO_CRAWL} KB\n')
    file.close()
    # Get robots.txt and save it.
    res = requests.get(INITIAL_SEED.url + '/robots.txt')
    if res.status_code == 200:
        file = open('robots.txt', 'w')
        file.write(res.text)


if __name__ == '__main__':
    main()
