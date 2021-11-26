#!/usr/bin/env python3
import os, sys, json
import pandas as pd
import numpy as np
import networkx as nx
from pyvis.network import Network


def run_PageRank(filein):
    os.system(f"python PageRank.py {filein} > {os.path.join('output','results.txt')}")

def convert_results_txt2json():
    with open(os.path.join('output','results.txt'), "r") as filein:
        with open(os.path.join('output','results.json'), "w") as fileout:
            fileout.write("[")
            for line in filein:
                fileout.write("{" + line.strip().replace("\t", ":") + "},\n")
            fileout.write('{"-1" : {"rank":0.0,"AdjacencyList":[]}}\n]')



if __name__=="__main__":
    if len(sys.argv)<2:
        raise Exception("Missing arguments")
    filein = sys.argv[1]
    
    os.makedirs('output', exist_ok = True)
    if not os.path.isfile(os.path.join('output','results.txt')):
        print('run')
        run_PageRank(filein)
    if not os.path.isfile(os.path.join('output','results.json')):
        convert_results_txt2json()
    

    ### Visualisation ###

    id = []
    pagerank = []
    redirect_list = []
    with open(os.path.join('output','results.json'), "r") as f:
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
    print(f"\nTop {n} nodes : \n\n", topn)
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
    network.show(os.path.join('output','network.html'))