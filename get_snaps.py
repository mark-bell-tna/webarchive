#!/home/ec2-user/WEBARCH/env/bin/python3

from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
from time import sleep

prefix = "https://webarchive.nationalarchives.gov.uk/"
#20121204113457/
page = "https://www.gov.uk/government/how-government-works"

def crawl_versions(url,url_file,skip_list = set()):

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

url = prefix + "*/" + page

crawl_versions(url,url.replace("/","_") + ".txt")
