import sys, re
from mrjob.job import MRJob
from mrjob.step import MRStep

class Node():
    def __init__(self,id_n,AdjacencyList):
        self.id = id_n
        self.AdjacencyList = AdjacencyList

class PageRank(MRJob):
    c = 0.15
    nIt = 2
    nodesInstances = {}

    def mapper1(self,_, line):
        #noeud -> noeud cité
        list = re.findall(r"\d+",str(line))
        if len(list)==2:
            [is_quoted_by,node] = list
            yield node, is_quoted_by

    def reducer1(self, node, AdjacencyList):
        """if node not in PageRank.nodesInstances:
            PageRank.nodesInstances[node] = Node(node,[is_quoted_by])
            yield node
        else:
            node = PageRank.nodesInstances.get(node)
            node.AdjacencyList.append(is_quoted_by)"""
        yield Node(node,AdjacencyList)

    def mapper2(self, _, node):
        new_rank = PageRank.c*node.rank
        for neighbour in node.AdjacencyList:
            neighbourInstance = PageRank.nodesInstances.get(neighbour)
            new_rank += (1-PageRank.c)*neighbourInstance.rank/len(node.AdjacencyList)
        node.rank = new_rank
        yield node


    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1)] + PageRank.nIt * [MRStep(mapper=self.mapper2)]

if __name__ == '__main__':
    #./main.py file_path
    file_path = sys.argv[1]
    
    PageRank.run()