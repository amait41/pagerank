#!/usr/bin/env python3
import os
import json
import pandas as pd
import numpy as np
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
    print(topn)
    df = df[df.source.isin(topn.id)].astype(str)
    # G = nx.from_pandas_edgelist(df, source='source', target="target")
    # net = Network(notebook=False)
    # net.from_nx(G)
    # net.show("visualization.html")
    # print(topn)

    network = Network(height='1000px', width='100%', bgcolor='#222222', font_color='white')
    network.barnes_hut()

    sources = df.source
    targets = df.target
    weights = np.ones(len(df))

    edge_data = zip(sources, targets, weights)

    for e in edge_data:
        src = e[0]
        dst = e[1]
        w = e[2]

        network.add_node(src, src, title=src)
        network.add_node(dst, dst, title=dst)
        network.add_edge(src, dst, value=w)

    neighbor_map = network.get_adj_list()

    # add neighbor data to node hover data
    for node in network.nodes:
        # node['title'] += ' Neighbors:<br>' + '<br>'.join(neighbor_map[node['id']])
        node['value'] = len(neighbor_map[node['id']]) * 100

    network.show_buttons(filter_=['physics'])
    network.show('network.html')