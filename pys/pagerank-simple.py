from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

#pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*")
#pattern = re.compile(r"[\w']+")
# Nodes: 75879 Edges: 508837

class PageRank(MRJob):

  def mapper(self, _, nodes):

    nodes = nodes.split('\t')

    outlinks = eval(nodes[1])

    # Send the graph structure onwards
    # force key to an int so graph struct and probs end up in the same reducer as the initial distributions.
    # if not it will generate a 0 line. may as well keep it out...

    yield int(nodes[0]), outlinks

    o = outlinks["outlinks"]
    c = outlinks["count"]
    initPr = 0.2

    if("pr" in outlinks):
      initPr = outlinks["pr"]

    for i in range(len(o)):

      pr = initPr/c

      yield o[i], pr

  def reducer(self, node, pr):
    
    # sum of incoming pr values
    s = 0
    
    # the node's graph data structure and associated data
    ds = None
    
    # previous page rank value TODO: perhaps start with an initial PR in the preproc step. it will get overwritten pretty much immediately.
    old_pr = 0.000000000000000000001
    
    for p in pr:
      
      if(type(p) is dict):
        
        ds = p
        
        if("pr" in p):
        
          old_pr = p["pr"]
      
      else:
      
        s = s + p
    
    ds["pr"] = s
    if((s - old_pr) / old_pr > 0.01):
      self.increment_counter('convergence', 'changes', 1)

    yield node, ds

  def steps(self):
        return [MRStep(
                       mapper=self.mapper,
                       reducer=self.reducer
                       )]


if __name__ == '__main__':
    PageRank.run()
