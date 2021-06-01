from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*")
#pattern = re.compile(r"[\w']+")

#a function that will take a number and create a list of arguments for the zip function. i.e. n arrays, each 1 shorter than the next.
# (needs to be stuffed into a wrapper so that the array of arrays can be parsed as arguments to zip properly).
def ngram(d,n):
    newargs = []
    for i in range(n):
        if (i ==0):
            newargs.append(d)
        else:
            newargs.append(d[i:])
    return list(funwrap(zip,newargs))


def funwrap(func, args):
    return func(*args)

# I have borrowed the function from the lecture to clean words in a consistent manner.
def CleanWord(aword):
    """
    Function input: A string which is meant to be
       interpreted as a single word.
    Output: a clean, lower-case version of the word
    """
    # Make Lower Case
    aword = aword.lower()
    # Remvoe special characters from word
    for character in '.,;:\'?':
        aword = aword.replace(character,'')
    # No empty words
    if len(aword)==0:
        return None
    # Restrict word to the standard english alphabet
    for character in aword:
        if character not in 'abcdefghijklmnopqrstuvwxyz':
            return None
    # return the word
    return aword


class BiGramStripes(MRJob):

    def map_init_var(self):
        # some parameters.
        # InvIndex  will be used as the associative array that will hold the word and its associated neighbours. plus some extra bits
        self.invIndex = {}
        
        self.countEmitted = 0

    def mapper(self, _, line):
        # create an array of the words in a line
        line = pattern.findall(line.strip())
        
        # ASSUMPTION!: The input files have been pre processed. Paragraphs have been converted into lines.
        # It seems this could be implemented as part of a custom input split.
        iterRange = len(line) -1

        for i in range(iterRange):

            if (i==iterRange):
                # then we are on the last word or a one word line. it does not have a pair. so we just update it's count.
                lastword = CleanWord(line[i])
                if(lastword in self.invIndex):
                    self.invIndex[lastword]["count"] += 1
                else:
                    self.invIndex[lastword] = {"count":1, "paired":{}}

            else:
                
                word = CleanWord(line[i])
                partner = CleanWord(line[i+1])
                
                if(word in self.invIndex):
                    if (partner in self.invIndex[word]["paired"]):
                        self.invIndex[word]["paired"][partner] += 1
                        self.invIndex[word]["count"] += 1
                    else:
                        self.invIndex[word]["paired"][partner] = 1
                        self.invIndex[word]["count"] += 1
                else:
                    self.invIndex[word] = {"count":1, "paired":{partner:1}}


            

    # Essentially just emits the word pairs from the index. called before the mapper closes.
    def emit_index(self):
        for k, v in self.invIndex.items():
            yield k, v



    def reducer(self, word, data):
        # dictionary's are going to be coming from all over the place. so they need to be merged... data should be a list of dictionarys.
        combinedDictionary = None
        for dictionary in data:
            if (combinedDictionary):
                # if it's not the fist result sum up the counts. 
                combinedDictionary["count"] += dictionary["count"]
                # sum up the paired / proceding words. INsert it if it hasn't been seen before.
                for w in dictionary["paired"].keys():
                    if w in combinedDictionary["paired"]:
                        combinedDictionary["paired"][w] += dictionary["paired"][w]
                    else:
                        combinedDictionary["paired"][w] = dictionary["paired"][w]
            else:
                combinedDictionary = dictionary
        yield word, combinedDictionary

    # the second stage mapper. Filter for all preceding words of "for" and at the same time calculate the conditional probability. 
    # Add the conditional probability to the key to be used by a partitioner which will send in descending order to the reducer.
    # may as well filter the output here. we know we want to and there's no point sending all the data through the rest of the process. 
    # without the filter it would still be able to calculate for all words
    def calcProbabilities(self, word, indexedItems):
        d = dict(indexedItems)["paired"]
        c = dict(indexedItems)["count"]
        if (word == "for"):
            for p in d.keys():
                yield word  + "~" + str((d[p] / c)) + "~" + p, d[p] / c

    # the second stage reducer. items will be grouped by proceding words in descending order of cond prob. restrict output to first 10 items. 
    def returnFOR(self, word, prob):
        if (self.countEmitted < 10):
            self.countEmitted = self.countEmitted + 1
            yield word.split("~")[2], next(prob)

    def steps(self):
        return [MRStep(mapper_init=self.map_init_var,
                       mapper=self.mapper,
                       mapper_final=self.emit_index,
                       reducer=self.reducer
                       ),
                MRStep(mapper=self.calcProbabilities,
                       reducer_init=self.map_init_var,
                       reducer=self.returnFOR,
                       jobconf={"mapreduce.job.output.key.comparator.class":"org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
                               "mapreduce.map.output.key.field.separator":"~",
                               "stream.num.map.output.key.fields":3,
                               "mapreduce.partition.keypartitioner.options":"-k1,1",
                               "mapreduce.partition.keycomparator.options": "k1 -k2nr k3"}
                               )]


if __name__ == '__main__':
    BiGramStripes.run()
