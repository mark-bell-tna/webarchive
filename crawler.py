#!/home/ec2-user/WEBARCH/env/bin/python3

import sys
import time
import re
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from http.client import IncompleteRead

from socket import error as SocketError
import bs4
from bs4 import BeautifulSoup
import grequests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests_futures.sessions import FuturesSession
import pickle
from operator import itemgetter
from os.path import isdir
from os import mkdir
from networkx.algorithms.dag import topological_sort
import networkx as nx
from os.path import isfile

class Crawl:

    def __init__(self, page_list, shortcode):
        self.ukgwa_protocol = "https://"
        self.ukgwa_prefix = "webarchive.nationalarchives.gov.uk"
        self.snap_format = "99999999999999"
        self.snap_re = re.compile("[0-9]{14}")
        self.data_folder = "./"
        self.executor = ThreadPoolExecutor(max_workers=50)
        #self.session = FuturesSession(executor=ThreadPoolExecutor(max_workers=100))
        self.url_list = []
        self.new_urls = []
        self.rejects = set()
        self.crawled = set()
        self.site = ''
        self.snapshot = ''
        self.protocol = 'http:'
        self.viewed = 0
        self.added = 0
        self.link_counts = {}
        self.earliest_links = {}
        self.links_filter = set()
        self.save_root = "/data/"
        self.fixed_snapshot = False

    # Increment counter dictionary
    def add_to_dict(self, D, k, v = 1):
        if k in D:
            D[k] += v
        else:
            D[k] = v

    def set_protocol(self, protocol):
        self.protocol = protocol + ":"

    def set_snapshot(self, snapshot):
        self.snapshot = snapshot
        self.mk_snap_dir()

    # mk_site_dir, mk_snap_dir, set_batchsize, set_size all set up the
    # directory structure for writing data to
    def mk_site_dir(self):
        if len(self.site) == 0:
            return
        site_dir = self.save_root + self.site
        #if not isdir(site_dir):
        #    mkdir(site_dir)
        
    def set_root(self, folder):
        self.save_root = folder
        if folder[-1] != "/":
            self.save_root += "/"

    def mk_snap_dir(self):
        if len(self.snapshot) == 0:
            return

        self.mk_site_dir()
        snap_dir = self.save_root + self.site + "/" + self.snapshot
        #if not isdir(snap_dir):
        #    mkdir(snap_dir)

    # Allows partitioning of files into sub folders to reduce size of any one folder
    def set_batchsize(self, batchsize):
        self.batchsize = batchsize
        if self.snapshot == '':
            return
        batch_root = self.save_root + self.site + "/" + self.snapshot + "/"
        #for i in range(batchsize):
        #    if not isdir(batch_root + str(i)):
        #        mkdir(batch_root + str(i))

    def set_site(self, site):
        self.site = site
        if site[0:4] == "www.":
            self.site_suffix = site[4:]
        else:
            self.site_suffix = site
        self.mk_site_dir()

    def set_data_folder(self, folder):
        self.data_folder = folder
        if folder[-1] != "/":
            self.data_folder += "/"
    
    # Uses urlparse to split url into constituent parts
    # Pre-processes url to remove ukgwa address and snaphot if present
    # Return value is a list containing the ParseResult object (see urllib) and the snapshot value
    def get_url_parts(self, url):
        prefix_pos = url.find(self.ukgwa_prefix)
        if prefix_pos > 0:
            this_url = url[prefix_pos+len(self.ukgwa_prefix):]
        else:
            this_url = url
        snap_match = re.search(self.snap_re, this_url)
        if snap_match is None:
            snapshot = ''
        else:
            snapshot = this_url[snap_match.start():snap_match.end()]
            this_url = this_url[snap_match.end()+1:]
        try:
            url_parts = urlparse(this_url)
        except Exception as e:
            self.rejects.add(this_url)
            print("Error:",e,this_url)
            return [urlparse("https://www.dummyurl.com/abcdef"), self.snapshot]

        return [url_parts, snapshot]

    def rebuild(self, url, snapshot):
        if isinstance(url, str):
            this_url = url
        else:
            this_url = urlunparse(url)
        
        if this_url[0:4] != "http":
            return("https://" + self.ukgwa_prefix + "/" + self.snapshot + "/" + self.protocol + "//" + this_url)
        else:
            return("https://" + self.ukgwa_prefix + "/" + self.snapshot + "/" + this_url)

    def load_url(self, url, timeout):
        page =  urlopen(Request(url, headers={'User-Agent': 'Turing Challenge Crawl'}))
        return page

    def get_page_list(self, url_list):

        futures = {self.executor.submit(self.load_url, u, 20): u for u in url_list}  #{self.session.get(u) for u in url_list}
        results = []
        self.viewed += len(url_list)
        has_errors = False
        #dg = [x for x in url_list if "DG_183733" in x]
        #if len(dg) > 0:
        #    print("DG_183733", dg)
        for future in as_completed(futures):
            try:
                resp = future.result()
                results.append(resp)
            except Exception as  e:
                has_errors = True
                print("Error fetching:",e, futures[future])
                self.rejects.add(futures[future])
                continue
        #if has_errors:
        #    print(url_list)
        return results

    def add_url(self, url):
        self.url_list.append(url)

    def process_urls(self, more_links = True):
        
        while len(self.url_list) > 0:
            these_urls = self.url_list[0:self.batchsize]
            self.url_list = self.url_list[self.batchsize:]
            responses = self.get_page_list(these_urls)
            #print("Time start:",time.time())
            for r in responses:
                #if 'content-type' not in r.headers:
                #    print("No content type",r)
                #    continue
                content_type = r.info().get_content_maintype() #r.headers['Content-Type'].split(";")[0].strip()
                filter = self.filter_pages(r.getcode(), content_type)
                if filter == 1:
                    #print("\tLink:",r.url)
                    parts = self.get_url_parts(r.url)
                    new_url = parts[0].netloc + "/" + parts[0].path
                    new_url = parts[0].scheme + "://" + new_url.replace("//","/")
                    if new_url not in self.crawled:
                        #print("*********Adding",new_url)
                        self.added += 1
                        if self.added % 100 == 0:
                            print("Viewed:",self.viewed,"Added:",self.added,"Time:",time.time())
                        self.sitemap.add_phrase(new_url)
                        self.crawled.add(new_url)
                        try:
                            html = r.read()
                        except (IncompleteRead) as e:
                            continue
                        soup = self.get_soup(html)
                        if more_links:
                            links = self.get_html_links(soup, new_url)
                            for l in links:
                                if l not in self.rejects:
                                    #if "DG_183733" in l:
                                    #    print("Add DG_183733:", l)
                                    self.new_urls.append(l)
                else:
                    print("**********Out:",r.geturl(), r.getcode(), content_type)
            #print("Time end:",time.time())

    def crawl_versions(self,url,url_file="",skip_list = set()):

        version_list = []
        try:
            html = urlopen(url)
        except Exception as e:
            print("Error with URL:",url)
            print(e)
            return
        soup = BeautifulSoup(html, 'html.parser')
        #print(soup)

        if url[len(prefix)-1:len(prefix)+2] != "/*/":
            print("Different format:",url,url[len(prefix)-1:len(prefix)+2])
            return
        domain = url[len(prefix)+2:]

        #out_file = open(url_file,"a")
        accordions = soup.findAll("div", {"class": "accordion"})
        print("Dom:",domain)
        print("Url:",url,"Accordions:",len(accordions))
        for acc in accordions:
            year = acc.find("span", {"class" : "year"})
            #print("Acc:",acc)
            print("\tYear", year, year.text,domain)
            versions = acc.findAll("a", href=re.compile(".[1-2]*" + domain, re.IGNORECASE))
            for v in versions:
                print("\t\t",v['href'])
                version_list.append(v['href'])
                #out_file.write(domain + "|" + year.text +  "|" + v['href'] + "\n")
        #out_file.close()
        return version_list

    def get_page(self, url):
        try:
            html = urlopen(Request(url, headers={'User-Agent': 'Turing Challenge Crawl'}))
        except HTTPError as e:
            print("HTTP Error:",e.code,url)
            return [e.code, None]
        except URLError as e:
            print("URL:",e.code)
            print(url)
            return [e.code, None]
        else:
            return [html.status, html]
            
        #message = html.info()
        #got_url = html.geturl()
        #print(got_url)
        #print(html)
#
        #print(message.get_content_type())      # -> text/html
        #print(message.get_content_maintype())  # -> text
#        content_type = message.get_content_maintype()  # -> text
#        if content_type in ["application","video","audio","image"]:
#            return
#
#        subtype = message.get_content_subtype()
#        if subtype in ["mpeg","jpeg","octet-stream"]:
#            return
#
#        if write_file:
#            page_file = open(out_file, "w")
#

    def filter_pages(self, page_status, page_type, return_types = [200], page_types = ['text/html','text']):
        if page_status not in return_types:
            return -1
        if page_type not in page_types:
            return -1
        return 1

    def get_soup(self, html):

        try:
            soup = BeautifulSoup(html, 'html.parser')
        except SocketError as e:
            if e.errno != errno.ECONNRESET:
                raise # Not error we are looking for
            return # Handle error here.

        #print(soup.find_all('title'))
        #links = [l for l in soup.find_all('a', href=True)]
        #for l in links:
        #    if l['href'][0] == '#':
        #        continue
        #    print(l['href'])

        return soup
 
    def get_html_links(self, soup, parent, filter = set(), return_orig = False):
        page_links = []
        links = [l for l in soup.find_all('a', href=True)]
        parent_parts = self.get_url_parts(parent)
        #print("Parent:",parent_parts)
        for ln in links:
            if len(ln['href']) == 0:
                continue
            if '#' in ln['href'][0]:
                continue
            if ln['href'].find("javascript") >= 0:
                continue
            if ln['href'].find("mailto") >= 0:
                continue
            parts = self.get_url_parts(ln['href'].strip())
            link_url = parts[0]
            #print("\tLink:",ln)
            #print("\t\tPath:",link_url.netloc,link_url.path)
            snapshot = parent_parts[1]
            if_path = 0
            if link_url.netloc == '':
                this_url = parent_parts[0].netloc + "/" + link_url.path
            else:
                if link_url.netloc[-len(self.site_suffix):] != self.site_suffix:
                    continue
                if_path += 1
                this_url = link_url.netloc + link_url.path
            #if "DG_183713" in this_url:
            #    print("P:", parent_parts[0].netloc, "U:",link_url.path)
            #    print("Gen URL:", this_url, "If:",if_path)
            self.add_to_dict(self.link_counts, this_url)
            parent_page = parent_parts[0].netloc
            if this_url in self.earliest_links:
                earliest_url = self.earliest_links[this_url]
                earliest_split = earliest_url.split("/")
                parent_split = parent_page.split("/")
                if len(parent_split) < len(earliest_split):
                    self.earliest_links[this_url] = parent_page
            else:
                self.earliest_links[this_url] = parent_page
            this_url = self.rebuild(this_url, snapshot) # TODO: change to link_url
            base_url = this_url[8+len(self.ukgwa_prefix)+16:]
            if base_url not in filter:
                if return_orig:
                    page_links.append([this_url, ln])
                else:
                    page_links.append(this_url)

        return page_links

    def url_to_filename(self, url):
        parts = self.get_url_parts(url)
        new_url = parts[0].netloc + "/" + parts[0].path
        new_url = parts[0].scheme + "://" + new_url.replace("//","/")
        snapshot = parts[1]
        filename = new_url.replace(":","").replace("/","_")
        filename = snapshot + "_" + filename
        return filename[:245] + ".txt"

    def write_soup_to_file(self, soup, folder, filename, url):

        html_links = self.get_html_links(soup, parent = url, filter = self.links_filter, return_orig = True)   
        print("Writing to:", folder + filename)
        page_file = open(folder + filename,"w")
        to_keep = dict([(l[1],l[0][8+len(self.ukgwa_prefix)+16:]) for l in html_links])
        links = [l for l in soup.find_all('a', href=True)]
        for l in links:
            try:
                if l not in to_keep:
                    l.decompose()
            except:
                print("Error decomposing link")
            #else:
            #    print("Keeping:",to_keep[l])
        S = [[soup,'abc']]
        #print(S)
        soup_text = soup.find_all(text=True)
        for t in soup_text:
            if len(t.strip()) > 0:
                page_file.write(t + "\n")
        #while len(S) > 0:
        #   ch = [c for c in S[0][0].children]
        #   parent = S[0][0]
        #   S = S[1:]
        #   #print("Children:",len(ch))
        #   for c in ch:
        #       #print("\nName:",c.name,"Type:",type(c))
        #       if type(c) is bs4.element.NavigableString:
        #           text = c.string.replace("\n","")
        #           if len(text) > 0:
        #               #print("\t\t","Nav",str(c),parent.name)
        #               if parent.name != "script":
        #                   page_file.write(text + "\n")
        #       elif type(c) is bs4.element.Tag:
        #           if c.name != "a":
        #               S.append([c,parent])
        #           #if c.name == "a":
        #           #    #print(c.get('href'),"is a",c.name)
        page_file.close()
                
    def snap_filter(self, snapshot):
        this_year = int(snapshot[0:4])
        snap_year = int(self.snapshot[0:4])
        print("Filtering:",this_year,snap_year)
        if self.fixed_snapshot:
            if snapshot[0:6] != self.snapshot[0:6]:
                return False
        if this_year == snap_year:
            return True
        if snap_year - this_year == 1:
            return True
        return False

    def urls_to_files(self, folder):

        print("URLS to write:",len(self.url_list))
        while len(self.url_list) > 0:
            these_urls = self.url_list[0:self.batchsize]
            self.url_list = self.url_list[self.batchsize:]
            responses = self.get_page_list(these_urls)
            #print("Time start:",time.time())
            for i,r in enumerate(responses):
                url = r.geturl()
                parts = self.get_url_parts(url)
                this_snap = parts[1]
                if not self.snap_filter(this_snap):
                    continue
                html = r.read()
                soup = self.get_soup(html)
                filename = self.url_to_filename(r.geturl())
                self.write_soup_to_file(soup, self.data_folder + str(i) + "/", filename, r.geturl())
        
    def load_pickles(self):
        try:
            self.sitemap.ngram_paths = pickle.load(open('crawler_ngram_' + self.site + "." + self.snapshot + '.pck','rb'))
        except:
            print("Not loaded")
        try:
            self.new_urls = pickle.load(open('crawler_new_urls_' + self.site + "." + self.snapshot + '.pck','rb'))
        except:
            print("Not loaded")
        try:
            self.link_counts = pickle.load(open('crawler_link_counts_' + self.site + "." + self.snapshot + '.pck','rb'))
        except:
            print("Not loaded")
        try:
            self.earliest_links = pickle.load(open('crawler_earliest_links_' + self.site + "." + self.snapshot + '.pck','rb'))
        except:
            print("Not loaded")
        
    def save_pickles(self):
        pickle.dump(self.sitemap.ngram_paths,open('crawler_ngram_' + self.site + "." + self.snapshot + '.pck','wb'))
        pickle.dump(self.new_urls, open('crawler_new_urls_' + self.site + "." + self.snapshot + '.pck','wb'))
        pickle.dump(self.link_counts, open('crawler_link_counts_' + self.site + "." + self.snapshot + '.pck','wb'))
        pickle.dump(self.earliest_links, open('crawler_earliest_links_' + self.site + "." + self.snapshot + '.pck','wb'))

    def bf_all_paths(self):
        BF = self.sitemap.breadth_first([protocol + ":",self.site])
        paths = set()
        path = protocol + "://" + self.site
        paths.add(path)
        for pages in BF:
            path = protocol + "://" + self.site
            for p in pages:
                path += "/" + p
                paths.add(path)
        return(list(paths))
    
    def graph_from_soup(self, soup):

        G = nx.DiGraph()

        node_dict = {}
        all_nodes = []
        all_nodes.append(soup.html)
        node_dict[soup.html] = 0
        parent_count = {}
        counter = 0
        while len(all_nodes) > 0:
            N = all_nodes.pop()
            #if N.name != "script":
            #    print("Node:",counter,N,"Name:",N.name,"String:",N.string)
            counter += 1
            if N.parent in node_dict:
                p_id = node_dict[N.parent]
            else:
                p_id = -1
            if p_id not in parent_count:
                parent_count[p_id] = 1
            else:
                parent_count[p_id] += 1
            if N.name == "script":
                content = ""
                node_label = ""
                node_type = "script"
            elif N.name == "p":
                content = N.get_text()
                node_label = content[0:20]
                node_type = "p"
            elif N.name == "a":
                href = N.get('href')
                node_type = "link"
                if N.string is None:
                    content = "LINK:blank:"+href
                    node_label = ""
                else:
                    content = "LINK:"+N.string+":"+href
                    node_label = N.string
            else:
                node_type = N.name
                if len(node_type) == 0:
                    node_type = "blank"
                content = N.string
                node_label = ""
                #if content == None:
                #    node_label = ""
                #else:
                #    node_label = content[0:20]
            #print(node_dict[N], N.name, p_id,  N.parent.name, content)
            #if "clientSide" in node_label: #Filter these out for now
            #    continue
 
            node_label = node_label.replace("\n"," ").strip()
            if node_label not in self.site_labels:
                self.site_labels[node_label] = len(self.site_labels)
            standardised_label = self.site_labels[node_label]

            if p_id not in G:
                G.add_node(p_id, name="root", sitenameid=standardised_label,  tag=N.parent.name, nodetype = node_type)
            if node_dict[N] not in G:
                G.add_node(node_dict[N], name=node_label, sitenameid=standardised_label, tag=N.name, nodetype = node_type)
            if p_id != node_dict[N]:
                G.add_edge(p_id, node_dict[N])
            C = N.children
            for i,c in enumerate(C):
                if c.name is not None:
                    all_nodes.append(c)
                    node_dict[c] = len(node_dict)

        print(self.site_labels)
        return G

    def collapse(self, G):
        T = topological_sort(G)

        prev = None
        to_collapse = []
        for t in T:
            if len(G[t]) == 1:
                parents = [p for p in G.predecessors(t)]
                if len(parents) == 0:
                    continue
                if "name" in G.nodes[t]:
                    if len(G.node[t]["name"]) > 0:
                        continue
                to_collapse.append([[parents[0],t,k] for k in G[t]])
            if len(G[t]) == 0:
                if "name" in G.nodes[t]:
                    if len(G.node[t]["name"]) == 0:
                        G.remove_node(t)
                else:
                    G.remove_node(t)

        renamed = {}
        for TC in [x[0] for x in to_collapse]:
            for i in range(len(TC)):
                if TC[i] in renamed:
                    TC[i] = renamed[TC[i]]
            #print("TC:",TC)
            G.add_edge(TC[0], TC[2])
            renamed[TC[1]] = TC[0]
            G.remove_node(TC[1])

    def load_labels(self, label_file):
        if isfile(label_file):
            self.site_labels = pickle.load(open(label_file, "rb"))
        else:
            self.site_labels = {}
        
    def save_labels(self, label_file):
        pickle.dump(self.site_labels, open(label_file, "wb"))

if __name__ == "__main__":

    #from gevent import monkey
    #monkey.patch_all()

    prefix = "https://webarchive.nationalarchives.gov.uk/"
    print(sys.argv)
    C = Crawl("abc","ABC")
    #print("\t",C.get_url_parts("http://webarchive.nationalarchives.gov.uk/20090101000000/https://www.gov.uk/index.html"))
    site = sys.argv[1] #"www.gov.uk"
    site_page = sys.argv[2] # "/" or "/browse"
    snapshot = sys.argv[3] #"20160101000000"
    protocol = sys.argv[4] #https or http
    #data_folder = "/data/" + site + "/" + snapshot + "/"
    #C.set_data_folder(data_folder)
    #C.set_site(site)
    #C.set_snapshot(snapshot)
    #C.set_batchsize(10)
    #C.set_protocol(protocol)

    C.load_labels(site + ".labels.pck")

    if snapshot[0:4] == "0000":
        snapshot_list = [prefix[:-1] + v for v in C.crawl_versions(prefix + "*" + "/" + protocol + "://" + site + site_page)]
    else:
        snapshot_list = [prefix + snapshot + "/" + protocol + "://" + site + site_page]
    #url = "https://webarchive.nationalarchives.gov.uk/" + snapshot + "/" + protocol + "://" + "www.eatwell.gov.uk/healthydiet/fss/salt/"

    for url in snapshot_list:
        page = C.get_page(url)
        print(url)
        print("\t",page[0],type(page[0]))
        if page[0] == 404:
            print("\tskipping",page[0])
            continue

        #C.get_page("https://webarchive.nationalarchives.gov.uk/20100101000000/http://www.abcxyz.com")
        soup = C.get_soup(page[1])
        #print(soup.prettify())

        G = C.graph_from_soup(soup)

        site_text = url[len(prefix):].replace("/","_").replace(":","_")
        nx.write_gml(G, site_text + ".0.gml")
        for i in range(5):
            print("Nodes Before:",len(G))
            C.collapse(G)
        print("Nodes After:",len(G))
        nx.write_gml(G, site_text + "." + str(i) + ".gml")
    #T = topological_sort(G)
    #for t in T:
    #    if "name" in G.nodes[t]:
    #        print(t,G.nodes[t]["name"])
        
    C.save_labels(site + ".labels.pck")

    with open(site_text , "w") as file:
        file.write(str(soup))

