from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
import json

# part d: popular scams
class PopularScams(MRJob):
    scams = {}

    def mapper_init(self):
        with open("scams.json") as f:
            data = json.load(f)["result"]
            for address in data:
                category = data[address]["category"]
                addrs = data[address]["addresses"]
                if category not in self.scams:
                    self.scams[category] = addrs # create new category
                else:
                    self.scams[category].extend(addrs) # append to existing category
            for category in self.scams:
                self.scams[category] = list(dict.fromkeys(self.scams[category])) # remove duplicates

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                to_address = fields[2] # to_address
                value = float(fields[3]) # value
                date = datetime.fromtimestamp(int(fields[6]))
                formatted_date = date.strftime("%m %Y") # "month year"
                for category in self.scams:
                    if to_address in self.scams[category]:
                        yield category, (formatted_date, value)
        except:
            pass

    def combiner(self, category, records):
        results = {}
        for (date, value) in records:
            if date not in results:
                results[date] = value
            else:
                results[date] += value
        results = dict(sorted(results.items(), key=lambda x: x[1], reverse=True))
        for (date, value) in results.items():
            yield category, (date, value)

    def reducer(self, category, records):
        results = {}
        for (date, value) in records:
            if date not in results:
                results[date] = value
            else:
                results[date] += value
        results = dict(sorted(results.items(), key=lambda x: x[1], reverse=True))
        for (date, value) in results.items():
            yield category, (date, value)

if __name__ == "__main__":
    PopularScams.run()
