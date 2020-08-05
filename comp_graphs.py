#!/home/ec2-user/WEBARCH/env/bin/python3

import networkx as nx

#G = nx.read_gml("20121204113457_https___www.gov.uk_government_how-government-works.4.gml")
#H = nx.read_gml("20200102234102_https___www.gov.uk_government_how-government-works.4.gml")
G = nx.read_gml("20090210223353_http___www.salt.gov.uk_.4.gml")
H = nx.read_gml("20090210223504_http___www.salt.gov.uk_no_more_than_6.html.4.gml")
H = nx.read_gml("20090210223504_http___www.salt.gov.uk_how_much_is_6.html.4.gml")
H = nx.read_gml("20090210223504_http___www.salt.gov.uk_babies_and_children.html.4.gml")
#20090210223504_http___www.salt.gov.uk_science_on_salt.html.4.gml

g_label_dict = {v["name"]:k for k,v in G.nodes(data=True) if len(v) > 0 and v["nodetype"] == "link"}
h_label_dict = {v["name"]:k for k,v in H.nodes(data=True) if len(v) > 0 and v["nodetype"] == "link"}

g_label_set = set([k for k in g_label_dict.keys()])
h_label_set = set([k for k in h_label_dict.keys()])

print("G:",len(g_label_set),"H:",len(h_label_set),"INT:",len(g_label_set.intersection(h_label_set)),"UN:",len(g_label_set.union(h_label_set)))

for i in g_label_set.intersection(h_label_set):
    print(i, g_label_dict[i], h_label_dict[i])

for d in g_label_set.difference(h_label_set):
    print("G:",d,g_label_dict[d])

for d in h_label_set.difference(g_label_set):
    print("H:",d,h_label_dict[d])
