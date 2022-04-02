from mrjob.job import MRJob
from datetime import datetime

# part d: gas guzzlers
class AverageGasPrice(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                gas_price = float(fields[5])
                date = datetime.fromtimestamp(int(fields[6]))
                formatted_date = date.strftime("%m %Y")
                yield formatted_date, gas_price
        except:
            pass

    def combiner(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += 1
            total += value
        yield date, (total, count)

    def reducer(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield date , total/count

if __name__ == '__main__':
    AverageGasPrice.run()
