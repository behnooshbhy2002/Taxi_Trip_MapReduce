from mrjob.job import MRJob
from mrjob.step import MRStep


class MRJoinTripsTaxis(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if not line or line.startswith("#"):
            return

        parts = line.split(",")

        # Distinguish file type by number of columns
        # Taxis: 4 columns
        # Trips: 8 columns
        if len(parts) == 4:
            # Taxi, company, model, year
            taxi_id = parts[0].strip()
            company_id = parts[1].strip()
            yield taxi_id, ("TAXI", company_id)

        elif len(parts) == 8:
            # Trip, Taxi, fare, distance, pickup_x, pickup_y, dropoff_x, dropoff_y
            taxi_id = parts[1].strip()
            yield taxi_id, ("TRIP", 1)

    def reducer(self, taxi_id, values):
        company_id = None
        trip_count = 0

        for tag, val in values:
            if tag == "TAXI":
                company_id = val
            elif tag == "TRIP":
                trip_count += val

        if company_id is not None and trip_count > 0:
            # output: company_id -> trip_count
            yield company_id, trip_count


if __name__ == "__main__":
    MRJoinTripsTaxis.run()
