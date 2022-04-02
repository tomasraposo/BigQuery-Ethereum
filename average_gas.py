from mrjob.job import MRJob
from mrjob.step import MRStep
from dateutil import parser
from datetime import datetime
import logging

# part d: gaz guzzlers
class AverageGasContracts(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 5:
                address = fields[0]
                dummy_value = 1
                yield address, (dummy_value, "C")

            elif len(fields) == 7:
                to_address = fields[2]
                gas = float(fields[4])
                date = datetime.fromtimestamp(int(fields[6]))
                formatted_date = date.strftime("%m %Y")
                yield to_address, ((formatted_date, gas), "T")
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
            for transaction in transactions:
                formatted_date, gas = transaction
                yield formatted_date, gas

    def average(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += 1
            total += value
        yield date, (total / count)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.join),
                MRStep(reducer=self.average)]

if __name__=='__main__':
    AverageGasContracts.run()
