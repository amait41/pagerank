#!/usr/bin/env python3
import os
import json
import pandas as pd
import networkx as nx
from pyvis.network import Network

def run_PageRank(filein="soc-Epinions1.txt", fileout="results.txt"):
    os.system(f"./PageRank.py {filein} > {fileout}")

def convert_results_txt2json(fp_in="results.txt", fp_out="results.json"):
    with open(fp_in, "r") as filein:
        with open(fp_out, "w") as fileout:
            fileout.write("[")
            for line in filein:
                fileout.write("{" + line.strip().replace("\t", ":") + "},\n")
            fileout.write('{"-1" : {"rank":0.0,"AdjacencyList":[]}}\n]')


if __name__=="__main__":
    if not os.path.isfile('results.txt'):
        run_PageRank()
    
    if not os.path.isfile('results.json'):
        convert_results_txt2json()
    
    id = []
    pagerank = []
    redirect_list = []
    with open("results.json", "r") as f:
        results = json.load(f)
        for elm in results:
            id.append(int(list(elm.keys())[0]))
            pagerank.append(list(elm.values())[0]["rank"])
            redirect_list.append(list(elm.values())[0]["AdjacencyList"])
    sites = pd.DataFrame({"id":id, "pagerank":pagerank, "redirect_list":redirect_list})
    
    source = []
    target = []
    with open("soc-Epinions1.txt", "r") as f:
        for line in f:
            line = line.strip().split('\t')
            source.append(int(line[0]))
            target.append(int(line[1]))
    df = pd.DataFrame({"source":source, "target":target})
    
    n = 5
    topn = sites[["id", "pagerank"]].sort_values(by="pagerank", ascending=False)[:n]
    df = df[df.source.isin(topn.id)]
    G = nx.from_pandas_edgelist(df, source='source', target="target")
    net = Network(notebook=False)
    net.from_nx(G)
    net.show("visualization.html")
    print(topn)


    # got_net = Network(height='750px', width='100%', bgcolor='#222222', font_color='white')

    # # set the physics layout of the network
    # got_net.barnes_hut()
    # got_data = pd.read_csv('https://www.macalester.edu/~abeverid/data/stormofswords.csv')

    # sources = got_data['Source']
    # targets = got_data['Target']
    # weights = got_data['Weight']

    # edge_data = zip(sources, targets, weights)

    # for e in edge_data:
    #     src = e[0]
    #     dst = e[1]
    #     w = e[2]

    #     got_net.add_node(src, src, title=src)
    #     got_net.add_node(dst, dst, title=dst)
    #     got_net.add_edge(src, dst, value=w)

    # neighbor_map = got_net.get_adj_list()

    # # add neighbor data to node hover data
    # for node in got_net.nodes:
    #     node['title'] += ' Neighbors:<br>' + '<br>'.join(neighbor_map[node['id']])
    #     node['value'] = len(neighbor_map[node['id']])

    # got_net.show('gameofthrones.html')