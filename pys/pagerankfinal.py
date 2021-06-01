from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json
import sys


# Nodes: 75879 Edges: 508837

class PageRankFinal(MRJob):

  
  def mapper(self, _, nodes):

      x = nodes.split("\t")
      ds = eval(x[1])
      print(x[0], ds["pr"])
      yield ds["pr"], x[0]
    
    


  def reducer(self, pr, node):

    # the partitioner  should ensure all probs are sorted in descending order.
    for item in node:
      yield pr, item




  def steps(self):
        return [MRStep(
                       mapper=self.mapper,
                       reducer=self.reducer,
                       jobconf={"mapreduce.job.output.key.comparator.class":"org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
                               "stream.num.map.output.key.fields":1,
                               "mapreduce.partition.keypartitioner.options":"-k1,1",
                               "mapreduce.partition.keycomparator.options": "-k1nr"}
                       )]


if __name__ == '__main__':
    PageRankFinal.run()
