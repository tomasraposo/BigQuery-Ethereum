from mrjob.job import MRJob
from mrjob.step import MRStep

# part c: top 10 miners
class TopTenMiners(MRJob):
    def mapper(self, key, line):
        try:
            fields = line.split(",")
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4])
                yield miner, size
        except:
            pass

    def reducer(self, miner, values):
        total = sum(values)
        yield None, (miner, total)

    def top10(self, _, records):
        records = sorted(list(records), key = lambda x: x[1], reverse = True)
        for record in records[:10]:
            miner, total = record
            yield miner, total

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(reducer=self.top10)]


if __name__ == '__main__':
    TopTenMiners.run()
