import sys, re
from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRank(MRJob):
    c = 0.15
    nIt = 2
    nodesInstances = {}

    # Lecture du fichier et identification des couples source -> destination
    # Ajouts des noeuds au dictionnaire des instances de noeuds
    def nodesInit(self,_, line):
        lineSplit = line.split('\t',maxsplit=1)
        is_quoted_by,node = lineSplit
        PageRank.nodesInstances[node] = {'rank':0,'AdjacencyList':set()}
        PageRank.nodesInstances[is_quoted_by] = {'rank':0,'AdjacencyList':set()}
        yield node, is_quoted_by

    def rankInit(self, node, AdjacencyList):
        tmp = set(AdjacencyList)
        PageRank.nodesInstances[node]['rank'] = 1/len(PageRank.nodesInstances)
        PageRank.nodesInstances[node]['AdjacencyList'] = tmp
        for neighbour in tmp:
            PageRank.nodesInstances[neighbour]['rank'] = 1/len(PageRank.nodesInstances)
        yield node, None

    def rankUpdate(self, node, _):
        new_rank = 0
        for neighbour in PageRank.nodesInstances[node]['AdjacencyList']:
                new_rank += (1-PageRank.c)*PageRank.nodesInstances[neighbour]['rank']

        PageRank.nodesInstances[node]['rank'] = \
            PageRank.c*PageRank.nodesInstances[node]['rank'] + \
            new_rank/len(PageRank.nodesInstances[node]['AdjacencyList'])

        yield node, PageRank.nodesInstances[node]['rank']


    def steps(self):
        return [MRStep(mapper=self.nodesInit,reducer=self.rankInit)] +\
            PageRank.nIt * [MRStep(mapper=self.rankUpdate)]

if __name__ == '__main__':
    #./main.py file_path
    file_path = sys.argv[1]
    
    PageRank.run()