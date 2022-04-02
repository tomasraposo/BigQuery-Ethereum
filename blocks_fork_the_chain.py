from mrjob.job import MRJob
import time

# part d: fork the chain
class BlocksForkTheChain(MRJob):
    def mapper(self, key, line):
        try:
            fields = line.split(",")
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4])
                gas_used = int(fields[5])
                time_epoch = int(fields[7])
                time_t = time.gmtime(time_epoch)
                week =  time.strftime("%W",time_t)
                year =  time.strftime("%Y",time_t)
                formatted_date = f"{week} {year}"
                transaction_count = int(fields[8])
                if year == "2019":
                    yield formatted_date, (size, gas_used, transaction_count)
        except:
            pass


    def combiner(self, date, values):
        total_size = 0
        total_gas_used = 0
        total_trans_count = 0
        count = 0
        for value in values:
            total_size += value[0]
            total_gas_used += value[1]
            total_trans_count += value[2]
            count += 1
        yield date, (total_size, total_gas_used, total_trans_count, count)

    def reducer(self, date, values):
        total_size = 0
        total_gas_used = 0
        total_trans_count = 0
        count = 0
        for value in values:
            total_size += value[0]
            total_gas_used += value[1]
            total_trans_count += value[2]
            count += value[3]
        yield date, (total_size / count, total_gas_used / count, total_trans_count / count)
        

if __name__ == '__main__':
    BlocksForkTheChain.run()
