from mrjob.job import MRJob
from mrjob.step import MRStep
import time

# part d: fork the chain
class TransactionsForkTheChain(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                to_address = fields[2]
                value = float(fields[3])
                gas = float(fields[4])
                gas_price = float(fields[5])
                gas_value = gas * gas_price
                time_epoch = int(fields[6])
                time_t = time.gmtime(time_epoch)
                week = time.strftime("%W", time_t)
                year = time.strftime("%Y", time_t)
                formatted_date = f"{week} {year}"
                if year == "2019":
                    yield formatted_date, (value, gas, gas_price, gas_value)

        except:
            pass

    def combiner(self, date, values):
        total_value = 0
        total_gas = 0
        total_gas_price = 0
        total_gas_value = 0
        count = 0
        for value in values:
            total_value += value[0]
            total_gas += value[1]
            total_gas_price += value[2]
            total_gas_value += value[3]
            count += 1
        yield date, (total_value, total_gas, total_gas_price, total_gas_value, count)

    def reducer(self, date, values):
        total_value = 0
        total_gas = 0
        total_gas_price = 0
        total_gas_value = 0
        count = 0
        for value in values:
            total_value += value[0]
            total_gas += value[1]
            total_gas_price += value[2]
            total_gas_value += value[3]
            count += value[4]
        yield date, (total_value / count, total_gas / count, total_gas_price / count, total_gas_value / count)

if __name__=='__main__':
    TransactionsForkTheChain.run()
