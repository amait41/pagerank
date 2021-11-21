from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class PageRank(MRJob):
    c = 0.15
    nIt = 2
    nodesInstances = {}

    # Data extraction from the file
    def nodeInit(self,_, line):
        lineSplit = line.split('\t',maxsplit=1)
        node, nextNode = lineSplit
        PageRank.nodesInstances[node] = 1
        PageRank.nodesInstances[nextNode] = 1
        yield node, nextNode

    # Nodes rank initialized as 1/N
    def rankInit(self, nodeId, AdjacencyList):
        node = {'rank':1/len(PageRank.nodesInstances),'AdjacencyList':list(AdjacencyList)}

        yield nodeId, node

    #### One iteration of rank update ####
    def mapper(self, nodeId, node):
        yield nodeId, ('node',node)
        
        if node['AdjacencyList']:
            contribution = node['rank']/len(node['AdjacencyList']) # Node contribution
            for neighbourId in node['AdjacencyList']:
                yield neighbourId, ('contribution',contribution) # Pass contribution to neighbours

    def reducer(self, nodeId, values):
        contributions = 0
        node = {'rank':1/len(PageRank.nodesInstances),'AdjacencyList':list()} # if node note created
        for value in values:
            if value[0]=='node':
                node = value[1]
            else:
                contributions+=value[1]

        node['rank'] = PageRank.c*node['rank'] + (1-PageRank.c)*contributions
        yield nodeId, node
    #### End of iteration ####

    def steps(self):
        return [MRStep(mapper=self.nodeInit, reducer=self.rankInit)] +\
        PageRank.nIt * [MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    #./main.py file_path
    file_path = sys.argv[1]
    
    PageRank.run()