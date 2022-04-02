from mrjob.job import MRJob
from datetime import datetime

# part a: time analysis 1
class NumberOfTransactions(MRJob):
    def mapper(self, key, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                date = datetime.fromtimestamp(int(fields[6]))
                formatted_date = date.strftime("%m %Y")
                yield formatted_date, 1
        except:
            pass

    def combiner(self, date, counts):
        yield date, sum(counts)

    def reducer(self, date, counts):
        yield date, sum(counts)

if __name__ == '__main__':
    NumberOfTransactions.run()
