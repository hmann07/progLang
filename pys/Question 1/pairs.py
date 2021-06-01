from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*")
#pattern = re.compile(r"[\w']+")

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


class BiGramPairs(MRJob):

    def reducer_init_var(self):

        self.total = 0

    def mapper(self, _, line):
        #find all the words in the line.
        line = pattern.findall(line)
        
        # add in the last word from previous line
        iterRange = len(line) -1

        for i in range(iterRange):

            if (i==iterRange):
                
                # then we are on the last word or a one word line. it does not have a pair. so we just update it's count.
                yield [CleanWord(line[i]),'*'], 1
            
            else:

            #for each bigram, normalise it, yield one key with the bigram, another with the first word and a count.
            
                word = CleanWord(line[i])
                partner = CleanWord(line[i+1])
                yield [word,partner], 1
                yield [word,'*'],1
            
    
    def reducer(self, word, wordcounts):
        yield word, sum(wordcounts)

    # may as well filter the output here. we know we want to and there's no point sending all the data through the rest of the process.
    def mapstage2(self, pair, count):

        if (pair[0] == "for"):
            yield pair[0] + '!' + pair[1], count

    #partitioner should ensure all preceding words turn up at teh same reducer
    def redstage2(self, pair, count):
        
        pair = pair.split('!')
        total = 0
        for c in count:
            if(pair[1] == "*"):
                self.total = c
            else:
                if(self.total != 0):
                    yield pair, c / self.total
                else:
                    yield pair, "check partitioning"


    # these third stage map/reducers are purely for sorting the output
    def mapstage3(self, pair, prob):
        # yield "for" and the prob for sorting, make the value = the paired word
        yield pair[0] + "!" + str(prob), pair[1]

    def redstage3(self, pair, pairedword):
        # reconstuct - turn paired word into key, and prob as value
        pair = pair.split("!")
        for p in pairedword:
            yield p, pair[1]


    def steps(self):
        return [MRStep(
                       mapper=self.mapper,
                       reducer=self.reducer
                       ),
               MRStep(mapper=self.mapstage2,
                      reducer_init=self.reducer_init_var,
                      reducer=self.redstage2,
                      jobconf={
                               "mapreduce.job.output.key.comparator.class":"org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
                               "mapreduce.map.output.key.field.separator":"!",
                               "stream.num.map.output.key.fields":2,
                               "mapreduce.partition.keypartitioner.options":"-k1,1",
                               "mapreduce.partition.keycomparator.options": "k1 -k2"}
                              ),
               MRStep(mapper=self.mapstage3,
                      reducer=self.redstage3,
                      jobconf={
                               "mapreduce.job.output.key.comparator.class":"org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
                               "mapreduce.map.output.key.field.separator":"!",
                               "stream.num.map.output.key.fields":2,
                               "mapreduce.partition.keypartitioner.options":"-k1,1",
                               "mapreduce.partition.keycomparator.options": "k1 -k2nr"}
                              )]


if __name__ == '__main__':
    BiGramPairs.run()
