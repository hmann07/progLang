from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

from pagerankpreproc import PageRankPreproc
from pageranksimple import PageRankSimple

BASE_OUTPUT_PATH = "s3://cwdatahm/output/prank"
BASE_INPUT_PATH = "s3://cwdatahm/inputPR/"
#BASE_OUTPUT_PATH = "prank"
#BASE_INPUT_PATH  = ""

preprocess = PageRankPreproc(args=['-c', 'mrjob.conf', '-r', 'emr', '--output-dir', 's3://cwdatahm/output/prank0', BASE_INPUT_PATH + 'testdata.txt'])
#preprocess = PageRankPreproc(args=['--output-dir', 'prank0', 'soc-Epinions1.txt'])
runner = preprocess.make_runner()
runner.run()


for i in range(50):
	
	#pr = PageRankSimple(args=['--output-dir', BASE_OUTPUT_PATH + str(i+1), BASE_OUTPUT_PATH + str(i)])
	pr = PageRankSimple(args=['-c', 'mrjob.conf', '-r', 'emr', '--output-dir', BASE_OUTPUT_PATH + str(i+1), BASE_OUTPUT_PATH + str(i)])
	runner = pr.make_runner()
	runner.run()
	print(runner.counters()[0])
	#if(not("convergence" in runner.counters()[0])):
	#	print("converged!")
	#	break
