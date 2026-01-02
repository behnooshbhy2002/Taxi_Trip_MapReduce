import math
from mrjob.job import MRJob
from mrjob.step import MRStep


def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)


class MRKMedoids(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_file_arg("--medoids", help="Path to medoids.txt")

    def load_medoids(self):
        self.medoids = []
        with open(self.options.medoids, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                x, y = line.split(",")
                self.medoids.append((float(x), float(y)))

    def mapper_init(self):
        self.load_medoids()

    def mapper(self, _, line):
        line = line.strip()
        if not line or line.startswith("#"):
            return

        parts = line.split(",")
        pickup_x = float(parts[4])
        pickup_y = float(parts[5])
        point = (pickup_x, pickup_y)

        # nearest medoid
        best_idx = None
        best_dist = float("inf")

        for i, m in enumerate(self.medoids):
            d = euclidean(point, m)
            if d < best_dist:
                best_dist = d
                best_idx = i

        # emit: cluster_id -> point
        yield best_idx, point

    def reducer(self, cluster_id, points):
        pts = list(points)
        if not pts:
            return

        # choose medoid: point with min sum distance to others
        best_point = None
        best_cost = float("inf")

        for candidate in pts:
            cost = 0.0
            for other in pts:
                cost += euclidean(candidate, other)
            if cost < best_cost:
                best_cost = cost
                best_point = candidate

        # output new medoid for this cluster
        yield cluster_id, {"medoid": best_point, "cost": round(best_cost, 4), "size": len(pts)}


if __name__ == "__main__":
    MRKMedoids.run()
