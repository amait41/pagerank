#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRank(MRJob):
    c = 0.15
    nIt = 10
    nodesInstances = set()

    # Data extraction from the file
    def nodeInit(self,_, line):
        lineSplit = line.split('\t',maxsplit=1)
        nodeFrom, nodeTo = lineSplit
        PageRank.nodesInstances.add(nodeFrom)
        PageRank.nodesInstances.add(nodeTo)
        yield nodeFrom, nodeTo

    # Nodes rank initialized as 1/N
    def rankInit(self, nodeId, AdjacencyList):
        node = {'rank':1/len(PageRank.nodesInstances),'AdjacencyList':list(AdjacencyList)}
        yield nodeId, node

    # Map contribution 
    def mapper(self, nodeId, node):
        yield nodeId, ('node',node)
        if node['AdjacencyList']:
            contribution = node['rank']/len(node['AdjacencyList']) # Node contribution
            for neighbourId in node['AdjacencyList']:
                yield neighbourId, ('contribution',contribution) # Pass contribution to neighbours

    # Reduce and update pagerank
    def reducer(self, nodeId, values):
        contributions = 0
        node = {'rank':1/len(PageRank.nodesInstances),'AdjacencyList':list()} # if node note created in rankInit
        for value in values:
            if value[0]=='node':
                node = value[1]
            else:
                contributions+=value[1]
        node['rank'] = PageRank.c*node['rank'] + (1-PageRank.c)*contributions
        yield nodeId, node

    def steps(self):
        return [MRStep(mapper=self.nodeInit, reducer=self.rankInit)] +\
        PageRank.nIt * [MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    PageRank.run()