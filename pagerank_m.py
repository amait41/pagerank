import sys, re
from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRank(MRJob):
    c = 0.15
    nIt = 2
    nodesInstances = {}

    # Lecture du fichier et identification des couples source -> destination
    def mapper1(self,_, line):
        list = re.findall(r"\d+",str(line))
        if len(list)==2:
            [is_quoted_by,node] = list
            yield node, is_quoted_by

    # Ajouts des noeuds au dictionnaire des instances de noeuds
    def combiner(self, node, AdjacencyList):
        tmp = set(AdjacencyList)
        PageRank.nodesInstances[node] = {'rank':0,'AdjacencyList':tmp}

        for neighbour in tmp:
            if neighbour not in PageRank.nodesInstances:
                PageRank.nodesInstances[neighbour] = {'rank':0,'AdjacencyList':set()}      

        yield node, None

    def reducer1(self, node, _):
        PageRank.nodesInstances[node]['rank'] = 1/len(PageRank.nodesInstances)
        yield node, None

    def mapper2(self, node, _):
        new_rank = 0
        for neighbour in PageRank.nodesInstances[node]['AdjacencyList']:
                new_rank += (1-PageRank.c)*PageRank.nodesInstances[neighbour]['rank']

        PageRank.nodesInstances[node]['rank'] = \
            PageRank.c*PageRank.nodesInstances[node]['rank'] + \
            new_rank/len(PageRank.nodesInstances[node]['AdjacencyList'])

        yield node, PageRank.nodesInstances[node]['rank']


    def steps(self):
        return [MRStep(mapper=self.mapper1,combiner=self.combiner,reducer=self.reducer1)] +\
            PageRank.nIt * [MRStep(mapper=self.mapper2)]

if __name__ == '__main__':
    #./main.py file_path
    file_path = sys.argv[1]
    
    PageRank.run()