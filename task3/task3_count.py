from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class MRCountTripsByCompany(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        line = line.strip()
        if not line:
            return

        company_id, trip_count = line.split("\t")
        company_id = company_id.strip().strip('"')

        yield company_id, int(trip_count)

    def reducer(self, company_id, counts):
        total = sum(counts)
        yield None, f"{company_id}\t{total}"

if __name__ == "__main__":
    MRCountTripsByCompany.run()
