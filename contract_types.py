from mrjob.job import MRJob
from mrjob.step import MRStep
from dateutil import parser

# part d: contract types
class ContractTypes(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 5:  # contracts
                address = fields[0]  # address
                date = parser.parse(fields[4])  # block fromtimestamp
                formatted_date = f"{date.month} {date.year}" # month year
                is_erc20 = fields[1]  # erc20
                is_erc721 = fields[2]  # erc721
                yield address, ((formatted_date, (is_erc20, is_erc721)), "C")

            elif len(fields) == 7:  # transactions
                to_address = fields[2]  # to_address
                value = float(fields[3])  # value
                gas = float(fields[4])  # gas
                gas_price = float(fields[5])  # gas_price
                timestamp = int(fields[6])  # timestamp
                gas_value = gas * gas_price  # rate * price = value
                yield to_address, ((value, gas_value, timestamp), "T")
        except:
            pass

    def reducer(self, address, values):
        transactions = []
        contract = None

        for value in values:
            if value[1] == "C":
                contract = value[0]  # grab date and erc types
            elif value[1] == "T":
                transactions.append(value[0])
        if contract and transactions:
            values = list(zip(*transactions))

            date = contract[0]
            is_erc20, is_erc721 = contract[1]

            avg_value = sum(values[0]) / len(values[0])
            avg_gas_value = sum(values[1]) / len(values[1])
            incoming_transactions = len(values[0])

            yield address, (avg_value, avg_gas_value, incoming_transactions, date, is_erc20, is_erc721)

if __name__ == '__main__':
    ContractTypes.run()
