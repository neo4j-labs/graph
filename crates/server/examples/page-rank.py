import json
import math
import pyarrow.flight as flight
import sys
import time
from typing import Tuple
from functools import wraps


def flight_result(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        result = result_to_json(f(*args, **kwargs))
        print_result(f.__name__, result)
        return result

    return wrapper


def result_to_json(result):
    return json.loads(next(result).body.to_pybytes().decode())


@flight_result
def create_graph(client, graph_name, file_format, graph_path):
    create_action = {
        "graph_name": graph_name,
        "file_format": file_format,
        "path": graph_path,
        "csr_layout": "Sorted",
        "orientation": "Directed",
    }

    return client.do_action(
        flight.Action("create", json.dumps(create_action).encode("utf-8"))
    )


@flight_result
def list_graphs(client):
    return client.do_action("list")


@flight_result
def run_page_rank(client, graph_name):
    compute_action = {
        "graph_name": graph_name,
        "algorithm": {
            "PageRank": {
                "max_iterations": 20,
                "tolerance": 0.0001,
                "damping_factor": 0.85,
            }
        },
        "property_key": "page_rank",
    }
    return client.do_action(
        flight.Action("compute", json.dumps(compute_action).encode("utf-8"))
    )


def load_page_rank_result(client, ticket):
    print_step("load_page_rank_result")
    start = time.time()
    reader = client.do_get(flight.Ticket(json.dumps(ticket).encode("utf-8")))
    scores = reader.read_all().to_pandas()
    nbytes = scores.memory_usage().sum()
    size_total, unit_total = convert_bytes(nbytes)
    elapsed = time.time() - start
    thrpt, unit = convert_bytes(nbytes / elapsed)
    count = scores.count(axis=0)["page_rank"]
    print(
        f"Received {count:,} rows ({size_total} {unit_total}) in {elapsed:.2f} seconds ({thrpt} {unit}/s)"
    )
    return scores


def print_page_rank_stats(scores):
    print_step("print_page_rank_stats")
    start = time.time()
    print("size = {:,}".format(scores.size))
    print("min = {}".format(scores.min()["page_rank"]))
    print("max = {}".format(scores.max()["page_rank"]))
    print("mean = {}".format(scores.mean()["page_rank"]))
    print("median = {}".format(scores.median()["page_rank"]))
    elapsed = time.time() - start
    print(f"Computed stats locally in {elapsed:.2f} seconds")


@flight_result
def remove_graph(client, graph_name):
    remove_action = {
        "graph_name": graph_name,
    }
    return client.do_action(
        flight.Action("remove", json.dumps(remove_action).encode("utf-8"))
    )


def convert_bytes(num_bytes) -> Tuple[float, str]:
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(num_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(num_bytes / p, 2)
    return s, size_name[i]


def print_step(step):
    print()
    print("#" * len(step))
    print(f"{step}")
    print("#" * len(step))
    print()


def print_result(step, obj):
    print_step(step)
    print(json.dumps(obj, indent=4))


def main() -> int:
    location = flight.Location.for_grpc_tcp("localhost", 50051)
    client = flight.FlightClient(location)
    graph_name = sys.argv[1]
    file_format = sys.argv[2]
    graph_path = sys.argv[3]

    create_graph(client, graph_name, file_format, graph_path)

    list_graphs(client)

    result = run_page_rank(client, graph_name)
    ticket = result["property_id"]

    scores = load_page_rank_result(client, ticket)

    print_page_rank_stats(scores)

    remove_graph(client, graph_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
