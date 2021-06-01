from mrjob.job import MRJob
import re

pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*")



class BiGramStripes(MRJob):
    def ngram(d,n):
        newargs = []
        for i in range(n):
            if (i ==0):
                newargs.append(d)
            else:
                newargs.append(d[i:])
        return funwrap(zip,newargs)


    def funwrap(func, args):
        return func(*args)

    def map_init_var(self):
        self.invIndex = {}
        self.lastWordPrevLine = ""

    def mapper(self, _, line):
        line = pattern.findall(line)
        if (len(line) ==0):
            self.lastWordPrevLine = ""
        else:
            line.insert(0,self.lastWordPrevLine)
            bgs = ngram(line,2)
            for bg in bgs:
                if(bg[0] in invIndex):
                    if (bg[1] in invIndex[bg[0]]["paired"]):
                        invIndex[bg[0]]["paired"][bg[1]] += 1
                        invIndex[bg[0]]["count"] += 1
                    else:
                        invIndex[bg[0]]["paired"][bg[1]] = 1
                        invIndex[bg[0]]["count"] += 1
                else:
                    invIndex[bg[0]] = {"count":1, "paired":{bg[1]:1}}

    def emit_index(self):
        for k, v in self.invIndex.iteritems():
            yield k , str(v)

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, counts

    def steps(self):
        return [MRStep(mapper_init=self.map_init_var,
                       mapper=self.mapper,
                       mapper_final=self.emit_index,
                       reducer=self.reducer)]


if __name__ == '__main__':
    BiGramStripes.run()
