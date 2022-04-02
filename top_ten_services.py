from mrjob.job import MRJob
from mrjob.step import MRStep

# part b: top 10 services
class TopTenServices(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 5:
                address = fields[0]
                dummy_value = 1
                yield address, (dummy_value, "C")

            elif len(fields) == 7:
                to_address = fields[2]
                value = float(fields[3])
                yield to_address, (value, "T")
        except:
            pass

    def join(self, key, values):
        transactions = []
        address = None

        for value in values:
            if value[1] == "C":
                address = key
            elif value[1] == "T":
               transactions.append(value[0])

        if address and transactions:
            total = sum(transactions)
            if total != 0:
                yield None, (address, total)

    def top10(self, _, records):
        records = sorted(list(records), key = lambda x: x[1], reverse = True)
        for record in records[:10]:
            address, total = record
            yield address, total

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.join),
                MRStep(reducer=self.top10)]

if __name__=='__main__':
    TopTenServices.run()
