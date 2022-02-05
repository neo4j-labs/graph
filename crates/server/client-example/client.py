import json
import pyarrow as pa
import pyarrow.flight as flight
import sys

location = flight.Location.for_grpc_tcp("localhost", 50051)
client = flight.FlightClient(location)
graph_name = sys.argv[1]
file_format = sys.argv[2]
graph_path = sys.argv[3]

create_action = {
    "graph_name": graph_name,
    "file_format": file_format,
    "path": graph_path,
    "csr_layout": "Deduplicated"
}

result = client.do_action(flight.Action("create", json.dumps(create_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("graph create result")
print(json.dumps(obj, indent = 4))

compute_action = {
    "graph_name": graph_name,
    "algorithm": {
        "PageRank": {
            "max_iterations": 20,
            "tolerance": 0.0001,
            "damping_factor": 0.85,
        }
    },
    "property_key": "page_rank"
}

result = client.do_action(flight.Action("compute", json.dumps(compute_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("page rank result")
print(json.dumps(obj, indent = 4))

ticket = obj['property_id']

reader = client.do_get(flight.Ticket(json.dumps(ticket).encode('utf-8')))
scores = reader.read_all().to_pandas()
print(scores.head())
print("count = " + str(scores.count(axis = 0)['page_rank']))
print("sum = " + str(scores.sum(axis = 0)['page_rank']))
