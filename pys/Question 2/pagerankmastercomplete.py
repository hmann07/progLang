from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

from pagerankpreproc import PageRankPreproc
from pagerankcomplete import PageRankComplete
from pagerankcompleteRedist import PageRankCompleteRedist

BASE_OUTPUT_PATH = "s3://cwdatahm/output/prank"
BASE_INPUT_PATH = "s3://cwdatahm/inputPR/"
#BASE_INPUT_PATH = ""
#BASE_OUTPUT_PATH = "prank"




preprocess = PageRankPreproc(args=['-c', 'mrjob.conf', '-r', 'emr', '--output-dir', 's3://cwdatahm/output/prank0', BASE_INPUT_PATH + 'soc-Epinions1.txt'])
#preprocess = PageRankPreproc(args=['--output-dir', 'prank0', 'soc-Epinions1.txt'])
runner = preprocess.make_runner()
runner.run()


for i in range(50):
	pr = PageRankComplete(args=['-c', 'mrjob.conf', '-r', 'emr', '--output-dir', BASE_OUTPUT_PATH + str(i+1), BASE_OUTPUT_PATH + str(i)])
	#pr = PageRankComplete(args=['--output-dir', BASE_OUTPUT_PATH + str(i+1), BASE_OUTPUT_PATH + str(i)])
	runner = pr.make_runner()
	runner.run()
	print(runner.counters()[0])
	
	print("finished page rank, start redistribution")

	# Inspect the counter capturing the dangling node values. If there is value, run the redistribution algorithm passing the missing mass in to it.
	if("dangling" in runner.counters()[0]):
		mm = runner.counters()[0]["dangling"]["value"]
		pr = PageRankCompleteRedist(args=['--missingmass', str(mm) ,'-c', 'mrjob.conf', '-r', 'emr',  '--output-dir', BASE_OUTPUT_PATH + "REDIST" + str(i+1), BASE_OUTPUT_PATH + str(i+1)])
		BASE_INPUT_PATH = "s3://cwdatahm/inputPR/REDIST"
		#pr = PageRankCompleteRedist(args=['--missingmass', str(mm) , '--output-dir', BASE_OUTPUT_PATH + str(i+1), BASE_OUTPUT_PATH + str(i+1)])
		runner = pr.make_runner()
		runner.run()
	else:
		BASE_INPUT_PATH = "s3://cwdatahm/inputPR/"

	# commented out on the basis we will use number of iterations for now.
	#if(not("convergence" in runner.counters()[0])):
	#	print("converged!")
	#	break

