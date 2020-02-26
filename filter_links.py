#!/usr/bin/env python
#
# a client to scrape the KTEK email inbox and compile a list of promo tracks to fetch
import sys
import os

from collections import namedtuple

import csv

import time
import datetime
import re
from pprint import pprint

import urllib.parse
from urllib.parse import urlparse
import hashlib

import yaml

import pycurl
from io import BytesIO

import random

import itertools

import progressbar

from typing import List, Dict, Set

import requests
from bs4 import BeautifulSoup

LINKS_CSV_FILE = "email_links.csv"
CURL_LINKS_CSV_FILE = "curl_email_links.csv"
NETLOC_FILE = "netlocs.csv"
HOSTNAME_FILE = "hostnames.csv"
LINK_DUP_FILE = "link_duplicate.csv"
LINK_DEDUP_FILE = "email_links_useful_dedup.csv"
DEDUP_CURL_LINK_FILE = "dudup_links_with_cURL.csv"
LINK_DEDUP_CURL_FILE = "email_links_useful_curl.csv"

# FILTER_PATTERNS_FILE = "filter_patterns.txt"
FILTER_PATTERNS_FILE = "filter_patterns.yml"

LINKS_PER_BATCH = 50

CURL_TIMEOUT =  60 #in seconds

ETA_SAMPLE_DELTA = 300

RegexPatterns = []

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36"
]

PROXY_FILE = "proxies.txt"
PROXY_SHCHEME = ""
PROXIES = []
PROXYPOOL = itertools.cycle(PROXIES)
USE_PROXIES = False

def get_proxies():
    global PROXIES
    global PROXYPOOL
    res = requests.get('https://free-proxy-list.net/', headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(res.text,"lxml")
    for items in soup.select("#proxylisttable tbody tr"):
        proxy = ':'.join([item.text for item in items.select("td")[:2]])
        PROXIES.append(proxy)
    random.shuffle(PROXIES)
    PROXYPOOL = itertools.cycle(PROXIES)

def remove_proxy(proxy : str):
    global PROXIES
    global PROXYPOOL
    if proxy in PROXIES:
        print("Removing Proxy [{}]".format(proxy))
        PROXIES.remove(proxy)
        random.shuffle(PROXIES)
        PROXYPOOL = itertools.cycle(PROXIES)

widgets = [
            ' [', progressbar.Timer(), '] ',
            progressbar.Bar(), progressbar.Percentage(),
            ' (', progressbar.ETA(), ') ',
            '[',  progressbar.Counter(), ']'
        ]

widgets_unknown = [
            ' [', progressbar.Timer(), '] ',
            progressbar.RotatingMarker(),
            ' (', progressbar.Counter(), ') ',
        ]

# print("Reading Filters from [{}] ...".format(FILTER_PATTERNS_FILE ))
# with open(FILTER_PATTERNS_FILE, "r",) as pattern_file:
#     for line in pattern_file:
#         if s := line.strip():
#             if  not s.startswith("//#"):
#                 FILTERS.append(s)

def yes_or_no(question):
    reply = str(input('{} (y/n): '.format(question))).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
    else:
        return yes_or_no("invalid responce [{}], please enter (y/n) \n {}".format(reply, question))

def is_useful(url : urllib.parse.ParseResult, s : str):
        if not url.path or url.path == "/":
            return False
        netloc = url.netloc.lower()
        for pattern in GlobalSubstrings:
            if pattern in netloc:
                return False
            if s and pattern in s.lower():
                return False
        if RegexFilter.search(netloc):
            return False
        if s:
            if RegexFilter.search(s.lower()) or TextRegexFilter.search(s.lower()):
                return False
        for pattern in URLSubstrings:
            if pattern in url.geturl().lower():
                return False
        return True

def check_usefull_and_dup_links(links : list):
    link_hashes = set()
    dup_links = set()
    netlocs = set()
    hostnames = set()
    link_counts = {}
    
    with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as pbar:  
        pbar.max_value = len(links)
        for link in pbar(links):
            try:
                url = urlparse(link['href'])
            except ValueError:
                print("\ninvalid link:  {}".format(link['href']))
                continue

            link['http'] = url.scheme in ('http', 'https')
            if link['http'] and hasattr(url, 'hostname') and url.hostname and len(url.hostname.split('.')) > 1:
                sha256hex = hashlib.sha256(url.geturl().encode()).hexdigest()
                link['sha256_link'] = sha256hex
                link['dup'] = sha256hex in link_hashes
                if link['str'] is None or link['str'] == "None":
                    link['str'] = ""
                else:
                    link['str'] = link['str'].strip()
                link['useful'] = is_useful(url, link['str'])
                if link['useful']:
                    netlocs.add(url.netloc)
                    hostnames.add(url.hostname)
                if link['dup']:
                    dup_links.add(sha256hex)
                    link_counts[sha256hex]['count'] += 1
                else:
                    link_counts[sha256hex] = {'href': url.geturl(), 'count': 1}
                    link_hashes.add(sha256hex)
    
    return (links, link_hashes, dup_links, netlocs, hostnames, link_counts)


LinkCurl = namedtuple('LinkCurl', ['link', 'res', 'hndl'])
def build_curl(link : dict, use_proxy = False):
    global PROXIES
    global PROXYPOOL
    res = BytesIO()
    hndl = pycurl.Curl()
    hndl.setopt(pycurl.URL, link['href'].encode('iso-8859-1'))
    hndl.setopt(pycurl.USERAGENT, random.choice(USER_AGENTS))
    proxy = ""
    if use_proxy:
        try:
            proxy = next(PROXYPOOL)
        except StopIteration:
            print("Reloading proxylist from https://free-proxy-list.net/ ...")
            get_proxies()
            print("Loaded [{}] proxies.".format(len(PROXIES)))
            try:
                proxy = next(PROXYPOOL)
            except StopIteration:
                pass
    link['proxy'] = proxy
    hndl.setopt(pycurl.PROXY, proxy)
    hndl.setopt(pycurl.WRITEDATA, res)
    hndl.setopt(pycurl.TIMEOUT, CURL_TIMEOUT)
    headers = {}
    link['headers'] = headers
    def header_function(header_line):
        # HTTP standard specifies that headers are encoded in iso-8859-1.
        # On Python 2, decoding step can be skipped.
        # On Python 3, decoding step is required.
        header_line = header_line.decode('iso-8859-1')

        # Header lines include the first status line (HTTP/1.x ...).
        # We are going to ignore all lines that don't have a colon in them.
        # This will botch headers that are split on multiple lines...
        if ':' not in header_line:
            return

        # Break the header line into header name and value.
        name, value = header_line.split(':', 1)

        # Remove whitespace that may be present.
        # Header lines include the trailing newline, and there may be whitespace
        # around the colon.
        name = name.strip()
        value = value.strip()

        # Header names are case insensitive.
        # Lowercase name here.
        name = name.lower()

        # Now we can actually record the header name and value.
        # Note: this only works when headers are not duplicated, see below.
        headers[name] = value
    
    hndl.setopt(pycurl.HEADERFUNCTION, header_function)
    hndl.setopt(pycurl.FOLLOWLOCATION, True)
    hndl.setopt(pycurl.NOBODY, True)
    req = LinkCurl(link, res, hndl)
    return req

CurlBatch = namedtuple('CurlBatch', ['curlmlti', 'curls'])
def build_link_batch(batch_links, use_proxy=False):
    multi = pycurl.CurlMulti()
    curls = []
    for link in batch_links:
        c = build_curl(link, use_proxy)
        curls.append(c)
        multi.add_handle(c[2])
    return CurlBatch(curlmlti=multi, curls=curls)

def clean_curl_batch(curl_batch):
    multi, curls = curl_batch
    for clink in curls:
        multi.remove_handle(clink[2])
        clink[2].close
    multi.close()


def process_curl_multi(multi, curls, pbar : progressbar.ProgressBar):
    timeouts = 0
    SELECT_TIMEOUT = 1.0
    num_handles = len(curls)
    total_curls = num_handles
    while num_handles:
        pbar.update(pbar.value, links_left=num_handles, links_todo=total_curls)
        ret = multi.select(SELECT_TIMEOUT)
        if ret == -1:
            print("TIMEOUT")
            timeouts += 1
            continue
        while 1:
            ret, num_handles = multi.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM: 
                break
    return timeouts

ProxyFailureCounts = {}

def process_curl_batch(curl_batch, pbar : progressbar.ProgressBar, use_proxy=False):
    global ProxyFailureCounts
    global PROXIES
    timeouts = 0
    multi, curls = curl_batch
    print("Hitting [{}] links with HTTP HEAD [{} proxies loaded]...".format(len(curls), len(PROXIES)))
    timeouts = process_curl_multi(multi, curls, pbar)
    print("\nProcessing [{}] link results...".format(len(curls)))
    retry_noproxy = []
    for curl in curls:
        link, res, hndl = curl # type: dict, StringIO, pycurl.Curl
        link['status'] = hndl.getinfo(pycurl.RESPONSE_CODE)
        if use_proxy and link['status'] == 0 :
            if link['proxy'] in ProxyFailureCounts:
                if ProxyFailureCounts[link['proxy']] > 2:
                    remove_proxy(link['proxy'])
            else:
                ProxyFailureCounts[link['proxy']] = 0
            ProxyFailureCounts[link['proxy']] += 1
            retry_noproxy.append(link)
            continue
        headers = link['headers']
        encoding = None
        if 'content-type' in headers:
            content_type = headers['content-type'].lower()
            match = re.search(r'charset=(\S+)', content_type)
            if match:
                encoding = match.group(1)
        if encoding is None:
            # Default encoding for HTML is iso-8859-1.
            # Other content types may have different default encoding,
            # or in case of binary data, may have no encoding at all.
            encoding = 'iso-8859-1'
        body = res.getvalue()
        link['body'] = body.decode(encoding)
        try:
            link['effective-url'] = hndl.getinfo_raw(pycurl.EFFECTIVE_URL).decode('iso-8859-1')
        except UnicodeDecodeError:
            try: 
                link['effective-url'] = hndl.getinfo_raw(pycurl.EFFECTIVE_URL).decode(encoding)
            except UnicodeDecodeError:
                link['effective-url'] = link['href'] # fallback
        
        link['redirect-count'] = hndl.getinfo(pycurl.REDIRECT_COUNT)
        sha256hex = hashlib.sha256(link['effective-url'].encode()).hexdigest()
        link['sha256_effective-url'] = sha256hex
        print("STATUS [{}]: {} --> {} (redirects: {})".format(
            link['status'], link['href'], 
            link['effective-url'], link['redirect-count']
        ))

    clean_curl_batch(curl_batch)

    if retry_noproxy:
        print("\nReprocessing [{}] proxy failures without a proxy".format(len(retry_noproxy)))
        timeout_cbatch = build_link_batch(retry_noproxy, use_proxy=False)
        timeouts += process_curl_batch(timeout_cbatch, pbar, use_proxy=False)

    return timeouts

def cURL_links(links : list, use_proxy=False):
    n = LINKS_PER_BATCH
    link_batches = [links[i * n:(i + 1) * n] for i in range((len(links) + n - 1) // n )]
    num_batches = len(link_batches)
    print("\nProcessing [{}] links in [{}] batches of [{}] links each...\n".format(
        len(links), num_batches, n
    ))

    widgets_curl = [
                ' [', progressbar.Timer(), '] ',
                progressbar.RotatingMarker(), progressbar.Bar(), progressbar.Percentage(),
                ' (', progressbar.AdaptiveETA(samples=datetime.timedelta(seconds=ETA_SAMPLE_DELTA)), ') ',
                '[',  progressbar.SimpleProgress(), ']',
                '[', progressbar.Variable('links_left', format='Links left in batch: {formatted_value}', width=2),
                 '/', progressbar.Variable('links_todo',  format='{formatted_value}', width=2), ']'
            ]

    timeouts = 0
    with progressbar.bar.ProgressBar(widgets=widgets_curl, redirect_stdout=True) as pbar:
        pbar.max_value = num_batches
        for b_num, batch in pbar(zip(range(num_batches), link_batches)):
            print('\nProcessing batch {} ...'.format(b_num + 1))
            cbatch = build_link_batch(batch, use_proxy)
            timeouts += process_curl_batch(cbatch, pbar, use_proxy)

    print("\nProcceed links with [{}] timeout events".format(timeouts))
    
    print("\nWriting cURL results file....")
    with progressbar.bar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets, redirect_stdout=True) as pbar:
        pbar.max_value = len(links)
        fields = ('msgid', 'href', 'str', 'status', 'effective-url', 'redirect-count')
        with open(DEDUP_CURL_LINK_FILE, 'w', newline='',  encoding='utf-8') as curl_csv:
            writer = csv.DictWriter(curl_csv, fieldnames=fields)
            writer.writeheader()
            for row in pbar(links):
                writer.writerow( {k:row[k] for k in row if k in fields} )
    
    return links

def filter_post_curl_link(link: dict):
    if link['status'] < 400 and link['status'] >= 200:
        try:
            url = urlparse(link['effective-url'])
        except ValueError:
            print("\ninvalid link:  {}".format(link['effective-url']))
            link['useful'] = False
            return
        
        link['useful'] = is_useful(url, "")
    else:
        link['useful'] = False

def proces_post_curl_links(links: list):
    with progressbar.bar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets, redirect_stdout=True) as pbar:
        pbar.max_value = len(links)
        for link in pbar(links):
            filter_post_curl_link(link)
    return [l for l in links if l['useful']]


if __name__ == "__main__":

    print("Reading Filters from [{}] ...".format(FILTER_PATTERNS_FILE ))
    with open(FILTER_PATTERNS_FILE, "r",) as pattern_file:
        try:
            filters = yaml.load(pattern_file, Loader=yaml.FullLoader)
        except yaml.YAMLError as err:
            print("Error loading filter file: ", err)
            sys.exit(1)

    try:
        RegexPatterns = tuple(map(lambda s: s.strip().lower(), filters['regex']))
        TextRegexPatterns = tuple(map(lambda s: s.strip().lower(), filters['text_regex']))
        GlobalSubstrings = tuple(map(lambda s: s.strip().lower(), filters['substring']))
        URLSubstrings = tuple(map(lambda s: s.strip().lower(), filters['url_substring']))
    except KeyError as err:
        print("Invalid filter file: ", err)

    # print("Loading Proxies from file [{}]".format(PROXY_FILE))
    # with open(PROXY_FILE, 'r') as proxy_file:
    #     for line in proxy_file:
    #         PROXIES.append("{}{}".format(PROXY_SHCHEME, line))
    # PROXYPOOL = itertools.cycle(PROXIES)
    print("fetching proxylist from https://free-proxy-list.net/ ...")
    get_proxies()
    print("Loaded [{}] proxies.".format(len(PROXIES)))

    # pprint("GlobalSubStrings: {}".format(GlobalSubstrings))
    # pprint("RegexPatterns: {}".format(RegexPatterns))

    #     for line in pattern_file:
    #         if s := line.strip():
    #             if  not s.startswith("//#"):
    #                 FILTERS.append(s)

    print("Compiling REGEX from Filters....")


    try:
        RegexPattern = "".join(("(", ")|(".join(RegexPatterns), ")"))
        RegexFilter = re.compile(RegexPattern)

        TextRegexPattern = "".join(("(", ")|(".join(TextRegexPatterns), ")"))
        TextRegexFilter = re.compile(RegexPattern)
    except re.error as err:
        print("Error compileing regex: ", err)
        sys.exit(1)

    # pprint(RegexFilter.pattern)

    

    Links = []

    print("\nReading Links from CSV...")
    with progressbar.bar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets_unknown, redirect_stdout=True) as pbar:
        with open(LINKS_CSV_FILE, 'r', newline='',  encoding='utf-8') as links_csv:
            reader = csv.DictReader(links_csv, fieldnames=['msgid', 'href', 'str', 'http', 'dup', 'useful'])
            for row in pbar(reader):
                Links.append(row)

    print("\nChecking for duplicate or useless links...")
    Links, Link_Hashes, Dup_Links, Netlocs, Hostnames, Link_Counts = check_usefull_and_dup_links(Links)
                

    print("\nWriting extended file....")
    with progressbar.bar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets, redirect_stdout=True) as pbar:
        pbar.max_value = len(Links)
        with open(CURL_LINKS_CSV_FILE, 'w', newline='',  encoding='utf-8') as links_csv:
            writer = csv.DictWriter(links_csv, fieldnames=['msgid', 'href', 'str', 'http', 'dup', 'useful'])
            writer.writeheader()
            for row in pbar(Links):
                writer.writerow( {k:row[k] for k in row if k != 'sha256_link'} )

    print("\nWriting location list...")
    with open(NETLOC_FILE, 'w', newline='', encoding='utf-8') as netloc_csv:
        writer = csv.writer(netloc_csv)
        writer.writerow(('netloc',))
        for row in Netlocs:
            writer.writerow((row,))

    print("Writing hostname list...")
    with open(HOSTNAME_FILE, 'w', newline='', encoding='utf-8') as netloc_csv:
        writer = csv.writer(netloc_csv)
        writer.writerow(('hostname',))
        for row in Hostnames:
            writer.writerow((row,))

    print("Writing duplicate link report...")
    with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as  pbar:  
        pbar.max_value = len(Dup_Links)
        with open(LINK_DUP_FILE, 'w', newline='',  encoding='utf-8') as dups_csv:
            writer = csv.DictWriter(dups_csv, fieldnames=['href', 'count'])
            writer.writeheader()
            for h_key in pbar(Dup_Links):
                writer.writerow(Link_Counts[h_key])

    print("\nWriting de-duplicated useful links file...")
    write_unique = set()
    msgids_unique = set()
    non_dup_links = []
    with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as  pbar:  
        pbar.max_value = len(Links)
        with open(LINK_DEDUP_FILE, 'w', newline='',  encoding='utf-8') as dedups_csv:
            writer = csv.DictWriter(dedups_csv, fieldnames=['msgid', 'href', 'str'])
            writer.writeheader()
            for link in pbar(Links):
                if link['http'] and link['useful'] and link['sha256_link'] not in write_unique:
                    writer.writerow({k:link[k] for k in link if k in ('msgid', 'href', 'str') })
                    write_unique.add(link['sha256_link'])
                    msgids_unique.add(link['msgid'])
                    non_dup_links.append(link)

    print("\nFound [{}] potentially useful non duplicate links in [{}] Emails.".format(len(write_unique), len(msgids_unique)))

    
    if yes_or_no("Hit found links with cURL and then use the effective_URL and HTTP status to filter further?"):
        gen_cURL = True
        if os.path.exists(DEDUP_CURL_LINK_FILE):
            timestamp = os.path.getmtime(DEDUP_CURL_LINK_FILE)
            localmtime = datetime.datetime.fromtimestamp(timestamp).strftime("%H:%M:%S on %a %b %d, %Y")
            localtime = datetime.datetime.now().strftime("%H:%M:%S on %a %b %d, %Y")
            question = "Found previous results of cURL\nResults file created at [{}]\nCurrent local time [{}]\nRun cURL and replace results?".format(localmtime, localtime)
            if not yes_or_no(question):
                gen_cURL = False

        if gen_cURL:
            if len(PROXIES) > 0 and yes_or_no("Use Proxies?"):
                USE_PROXIES = True
            post_curl_links = cURL_links(non_dup_links, USE_PROXIES)
        else:
            post_curl_links = []
            print("\nReading Links from CSV...")
            with progressbar.bar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets_unknown, redirect_stdout=True) as  pbar:
                with open(DEDUP_CURL_LINK_FILE, 'r', newline='',  encoding='utf-8') as curl_csv:
                    reader = csv.DictReader(curl_csv, fieldnames=['msgid', 'href', 'str', 'http', 'dup', 'useful'])
                    for row in pbar(reader):
                        post_curl_links.append(row)
        
        print("\nFiltering links after cURL results...")
        filtered_links = proces_post_curl_links(post_curl_links)

        print("Writing links with unique effective-urls...")
        filtered_write_unique = set()
        filtered_msgids_unique = set()
        with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as  pbar:  
            pbar.max_value = len(filtered_links)
            fields = ('msgid', 'href', 'str', 'effective-url')
            with open(LINK_DEDUP_CURL_FILE, 'w', newline='',  encoding='utf-8') as dedups_curl_csv:
                writer = csv.DictWriter(dedups_curl_csv, fieldnames=fields)
                writer.writeheader()
                for link in pbar(filtered_links):
                    if link['http'] and link['useful'] and link['sha256_effective-url'] not in filtered_write_unique:
                        writer.writerow({k:link[k] for k in link if k in fields })
                        filtered_write_unique.add(link['sha256_effective-url'])
                        filtered_msgids_unique.add(link['msgid'])
                        non_dup_links.append(link)
        
        print("\nFound [{}] potentially useful non duplicate links in [{}] Emails after cURL.".format(len(filtered_write_unique), len(filtered_msgids_unique)))

    print("\nDone.")
