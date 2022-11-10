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


def run_algorithm(client, graph_name, algo_config, property_key):
    compute_action = {
        "graph_name": graph_name,
        "algorithm": algo_config,
        "property_key": property_key,
    }
    return client.do_action(
        flight.Action("compute", json.dumps(compute_action).encode("utf-8"))
    )


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
def run_page_rank(client, graph_name, property_key="page_rank"):
    return run_algorithm(
        client,
        graph_name,
        {
            "PageRank": {
                "max_iterations": 20,
                "tolerance": 0.0001,
                "damping_factor": 0.85,
            }
        },
        property_key,
    )


@flight_result
def run_wcc(client, graph_name, property_key="component"):
    return run_algorithm(
        client,
        graph_name,
        {
            "Wcc": {
                "chunk_size": 16384,
                "neighbor_rounds": 2,
                "sampling_size": 1024,
            }
        },
        property_key,
    )


@flight_result
def run_triangle_count(client, graph_name, property_key="triangle_count"):
    return run_algorithm(
        client,
        graph_name,
        {
            "TriangleCount": None
        },
        property_key,
    )


def load_property(client, ticket):
    print_step(f"load_property `{ticket}`")
    start = time.time()
    reader = client.do_get(flight.Ticket(json.dumps(ticket).encode("utf-8")))
    result = reader.read_all().to_pandas()
    nbytes = result.memory_usage().sum()
    size_total, unit_total = convert_bytes(nbytes)
    elapsed = time.time() - start
    thrpt, unit = convert_bytes(nbytes / elapsed)
    count = result.size
    print(
        f"Received {count:,} rows ({size_total} {unit_total}) in {elapsed:.2f} seconds ({thrpt} {unit}/s)"
    )
    return result


def print_page_rank_stats(scores):
    print_step("print_page_rank_stats")
    start = time.time()
    print(f"size = {scores.size:,}")
    print(f"min = {scores.min()['page_rank']}")
    print(f"max = {scores.max()['page_rank']}")
    print(f"mean = {scores.mean()['page_rank']}")
    print(f"median = {scores.median()['page_rank']}")
    elapsed = time.time() - start
    print(f"Computed stats locally in {elapsed:.2f} seconds")


def print_wcc_stats(components):
    print_step("print_wcc_stats")
    start = time.time()
    print(f"size = {components.size:,}")
    unique_components = components.drop_duplicates()
    print(f"component count = {unique_components.size:,}")
    elapsed = time.time() - start
    print(f"Computed stats locally in {elapsed:.2f} seconds")


def print_tc_stats(triangle_count):
    print_step("print_tc_stats")
    print(f"global triangle count = {triangle_count:,}")


@flight_result
def to_undirected_graph(client, graph_name, csr_layout="Unsorted"):
    to_undirected_action = {
        "graph_name": graph_name,
        "csr_layout": csr_layout,
    }
    return client.do_action(
        flight.Action("to_undirected", json.dumps(to_undirected_action).encode("utf-8"))
    )


@flight_result
def to_relabeled_graph(client, graph_name):
    to_relabeled_action = {
        "graph_name": graph_name,
    }
    return client.do_action(
        flight.Action("to_relabeled", json.dumps(to_relabeled_action).encode("utf-8"))
    )


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
    scores = load_property(client, ticket)
    print_page_rank_stats(scores)

    result = run_wcc(client, graph_name)
    ticket = result["property_id"]
    components = load_property(client, ticket)
    print_wcc_stats(components)

    to_undirected_graph(client, graph_name, "Deduplicated")
    to_relabeled_graph(client, graph_name)

    result = run_triangle_count(client, graph_name)
    print_tc_stats(result["triangle_count"])

    remove_graph(client, graph_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
