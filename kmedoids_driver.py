import random
import json
import subprocess
import math

TRIPS_FILE = "Trips.txt"
K = 3
MAX_ITERS = 10
EPS = 1e-3

MEDOIDS_FILE = "medoids.txt"
OUT_FILE = "kmedoids_out.txt"


def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)


def init_random_medoids(trips_file, k):
    points = []
    with open(trips_file, "r") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split(",")
            points.append((float(parts[4]), float(parts[5])))

    medoids = random.sample(points, k)
    with open(MEDOIDS_FILE, "w") as f:
        for x, y in medoids:
            f.write(f"{x},{y}\n")
    return medoids


def read_medoids():
    meds = []
    with open(MEDOIDS_FILE, "r") as f:
        for line in f:
            if not line.strip():
                continue
            x, y = line.strip().split(",")
            meds.append((float(x), float(y)))
    return meds


def write_medoids(medoids):
    with open(MEDOIDS_FILE, "w") as f:
        for x, y in medoids:
            f.write(f"{x},{y}\n")


def run_iteration():
    cmd = [
        "python", "kmedoids_job.py",
        "--medoids", MEDOIDS_FILE,
        TRIPS_FILE
    ]
    with open(OUT_FILE, "w") as out:
        subprocess.run(cmd, stdout=out, check=True)


def parse_output():
    new_meds = {}
    with open(OUT_FILE, "r") as f:
        for line in f:
            cluster_id, payload = line.split("\t")
            cluster_id = int(cluster_id)
            payload = json.loads(payload)
            new_meds[cluster_id] = tuple(payload["medoid"])
    return [new_meds[i] for i in sorted(new_meds.keys())]


def main():
    old_medoids = init_random_medoids(TRIPS_FILE, K)

    for it in range(1, MAX_ITERS + 1):
        run_iteration()
        new_medoids = parse_output()

        diff = sum(euclidean(old_medoids[i], new_medoids[i]) for i in range(K))
        print(f"Iteration {it} | total shift = {diff}")

        write_medoids(new_medoids)

        if diff < EPS:
            print("Converged.")
            break

        old_medoids = new_medoids


if __name__ == "__main__":
    main()
