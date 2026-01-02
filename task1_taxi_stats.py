from mrjob.job import MRJob
from mrjob.step import MRStep


class MRTaxiTripStats(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if not line or line.startswith("#"):
            return

        parts = line.split(",")
        # Trip, Taxi, fare, distance, ...
        taxi_id = parts[1].strip()
        distance = float(parts[3].strip())

        # emit taxi_id -> (count=1, sum_distance)
        yield taxi_id, (1, distance)

    def reducer(self, taxi_id, values):
        total_trips = 0
        total_distance = 0.0

        for c, dist in values:
            total_trips += c
            total_distance += dist

        avg_distance = total_distance / total_trips if total_trips else 0.0
        yield taxi_id, {"trips": total_trips, "avg_distance": round(avg_distance, 4)}


if __name__ == "__main__":
    MRTaxiTripStats.run()
