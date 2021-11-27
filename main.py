#!/usr/bin/env python3

import os, sys, json
import pandas as pd
import numpy as np
from pyvis.network import Network


def run_PageRank(filein):
    """Run PageRank.py and create a file to save results.

    Args:
        filein (str): File path of a file fotmated as follow :
                    fromNode\ttoNode\n
                    fromNode\ttoNode\n
                    etc
    """     
    os.system(f"python PageRank.py {filein} > {os.path.join('output','results.txt')}")

def convert_results_txt2json():
    """Convert results.txt (created by run_PageRank function) to json format.
    """    
    with open(os.path.join('output','results.txt'), "r") as filein:
        with open(os.path.join('output','results.json'), "w") as fileout:
            fileout.write("[")
            for line in filein:
                fileout.write("{" + line.strip().replace("\t", ":") + "},\n")
            fileout.write('{"-1" : {"rank":0.0,"AdjacencyList":[]}}\n]')

def get_rank_df():
    """Create Dataframe : rank = pd.DataFrame({"id":id, "pagerank":pagerank, "redirect_list":redirect_list})
    """
    id = []
    pagerank = []
    redirect_list = []
    with open(os.path.join('output','results.json'), "r") as f:
        results = json.load(f)
        for elm in results:
            id.append(int(list(elm.keys())[0]))
            pagerank.append(list(elm.values())[0]["rank"])
            redirect_list.append(list(elm.values())[0]["AdjacencyList"])
    return pd.DataFrame({"id":id, "pagerank":pagerank, "redirect_list":redirect_list})
    
def get_net_df():
    """Create Dataframe : net = pd.DataFrame({"source":source, "target":target})
    """
    source = []
    target = []
    with open(filein, "r") as f:
        for line in f:
            line = line.strip().split('\t')
            source.append(int(line[0]))
            target.append(int(line[1]))
    return pd.DataFrame({"source":source, "target":target})

def get_topn_df(rank, net, n):
    """Get Top n sort by pagerank from net DataFrame
    """
    topn = rank[["id", "pagerank"]].sort_values(by="pagerank", ascending=False)[:n]
    print(f"\nTop {n} nodes :\n", topn)
    return net[net.source.isin(topn.id)].astype(str)
    
def visualization(net_topn) :
    network = Network(height='1000px', width='100%', bgcolor='#222222', font_color='white')
    network.barnes_hut()
    
    sources = net_topn.source
    targets = net_topn.target
    weights = np.ones(len(net_topn))
    for src, dst, w in zip(sources, targets, weights):
        network.add_node(src, src, title=src)
        network.add_node(dst, dst, title=dst)
        network.add_edge(src, dst, value=w)

    neighbor_map = network.get_adj_list()

    # add neighbor data to node hover data
    for node in network.nodes:
        node['value'] = len(neighbor_map[node['id']]) * 100

    network.show_buttons(filter_=['physics'])
    network.show(os.path.join('output','network.html'))
    
    
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
    
    # Get top n
    rank = get_rank_df()
    net = get_net_df()
    net_topn = get_topn_df(rank, net, 10)

    # Visualization
    visualization(net_topn)
