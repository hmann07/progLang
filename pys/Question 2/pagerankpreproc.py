from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

# Nodes: 75879 Edges: 508837

class PageRankPreproc(MRJob):




    def preprocmapper(self, _, node):
      node = node.split("\t")
      
      yield node[0] , node[1]
     
      # yield node 1 as a key, to make sure we don't miss it if it's dangling.

      yield node[1], None

    # Here all the nodes will turn up together in one reducer from multiple mappers.
    def preprocreducer(self, node, outlink):
      d = []
      s = 0.0
      pr = 1 / 75879

      for o in outlink: 

        # take account of nodes pointing to themselves (might not be relevant in this case...)
        if(o and o !=node):
        
          d.append(int(o))
        
          s +=1.0
      
      yield int(node), {"count":s, "outlinks":d, "pr":pr}

    def steps(self):
        return [MRStep(
                       mapper=self.preprocmapper,
                       reducer=self.preprocreducer
                       )]


if __name__ == '__main__':
    PageRankPreproc.run()
