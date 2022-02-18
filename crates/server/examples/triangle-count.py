import json
import pyarrow as pa
import pyarrow.flight as flight
import sys

location = flight.Location.for_grpc_tcp("localhost", 50051)
client = flight.FlightClient(location)
graph_name = sys.argv[1]
file_format = sys.argv[2]
graph_path = sys.argv[3]

# Create undirected graph on server

graph_name = graph_name + "_undirected"

create_action = {
    "graph_name": graph_name,
    "file_format": file_format,
    "path": graph_path,
    "csr_layout": "Deduplicated",
    "orientation": "Undirected",
}

result = client.do_action(flight.Action("create", json.dumps(create_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("graph create result")
print(json.dumps(obj, indent = 4))

# Relabel undirected graph on server

relabel_action = {
    "graph_name": graph_name,
}

result = client.do_action(flight.Action("relabel", json.dumps(relabel_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("graph relabel result")
print(json.dumps(obj, indent = 4))

# Compute Global Triangle Count
compute_action = {
    "graph_name": graph_name,
    "algorithm": "TriangleCount",
    "property_key": "unused",
}

result = client.do_action(flight.Action("compute", json.dumps(compute_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("triangle count result")
print(json.dumps(obj, indent = 4))
