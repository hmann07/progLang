from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json
import sys


# Nodes: 75879 Edges: 508837

class PageRankComplete(MRJob):

  
  def mapper(self, _, nodes):

    

    nodes = nodes.split('\t')
    
    outlinks = eval(nodes[1])

    # Send the graph structure onwards
    # force key to an int so graph struct and probs end up in the same reducer as the initial distributions.
    # if not it will generate a 0 line. may as well keep it out...

    yield int(nodes[0]), outlinks

    JUMP = 0.85
    o = outlinks["outlinks"]
    c = outlinks["count"]
    initPr = outlinks["pr"]

    if(c==0):
      #add up the pr values of the dangling nodes (a bit hack-ish)
      self.increment_counter('dangling', 'value', int(initPr*1000000))

    # for all outlinks, send them to be aggregated with their share of the node's  page rank
    for i in range(len(o)):

      pr = initPr/c
      
      yield int(o[i]), pr


  def reducer(self, node, pr):

    # sum of incoming pr values
    s = 0
    
    # the node's graph data structure and associated data
    ds = {}
    
    # previous page rank value TODO: perhaps start with an initial PR in the preproc step. it will get overwritten pretty much immediately.
    old_pr = 1
    
    # for all pageranks recieved from other nodes sum them up (wath out for the graph structure coming through.)
    for p in pr:
      
      if(type(p) is dict):
        
        ds = p
        
        if("pr" in p):
        
          old_pr = p["pr"]
      
      else:
      
        s = s + p
    

    # we have now gone through the whole list, we should have all the probabilites and seen the graph structure node.
    # update its probability
    ds["pr"] = s
    
    if(old_pr ==0):
      yield "error", node
    
    # check how much the probability changed... if it's over a certain threshold then it's not close enough to convergence
    else:

      if(abs((s - old_pr) / old_pr) < 0.9):
    
        self.increment_counter('convergence', 'changes', 1)
      
    # yield the updated graph.
    yield node, ds

  def steps(self):
        return [MRStep(
                       mapper=self.mapper,
                       reducer=self.reducer
                       )]


if __name__ == '__main__':
    PageRankComplete.run()
