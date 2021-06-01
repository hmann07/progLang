from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

#pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*")
#pattern = re.compile(r"[\w']+")
# Nodes: 75879 Edges: 508837

class PageRankCompleteRedist(MRJob):

  def configure_options(self):
    super(PageRankCompleteRedist, self).configure_options()
    self.add_passthrough_option('--missingmass', type='int', default=0, help='this number should always be divided by 10000000')

  def mapper(self, _, nodes):

    jumprate = 0.85
    numberOfNodes = 75879
    missingmass = int(self.options.missingmass) / 1000000


    nodes = nodes.split('\t')

    outlinks = eval(nodes[1])
    
    pr = outlinks["pr"]

    outlinks["pr"] = (jumprate*(1/numberOfNodes)) + (1 - jumprate)*((missingmass / numberOfNodes) + pr)
    
    yield int(nodes[0]), outlinks
  
  def reducer(self, node, ol):
    #just pass it out.
    yield node, next(ol)

  def steps(self):
        return [MRStep(
                       mapper=self.mapper,
                       reducer=self.reducer
                       )]


if __name__ == '__main__':
    PageRankCompleteRedist.run()
