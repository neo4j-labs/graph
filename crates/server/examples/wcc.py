import json
import pyarrow as pa
import pyarrow.flight as flight
import sys

location = flight.Location.for_grpc_tcp("localhost", 50051)
client = flight.FlightClient(location)
graph_name = sys.argv[1]
file_format = sys.argv[2]
graph_path = sys.argv[3]

# Create directed graph on server
create_action = {
    "graph_name": graph_name,
    "file_format": file_format,
    "path": graph_path,
    "csr_layout": "Sorted",
    "orientation": "Directed",
}

result = client.do_action(flight.Action("create", json.dumps(create_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("graph create result")
print(json.dumps(obj, indent = 4))

# Compute WCC
compute_action = {
    "graph_name": graph_name,
    "algorithm": {
        "Wcc": {
            "chunk_size": 16384,
            "neighbor_rounds": 2,
            "sampling_size": 1024,
        }
    },
    "property_key": "components"
}

result = client.do_action(flight.Action("compute", json.dumps(compute_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("WCC result")
print(json.dumps(obj, indent = 4))

ticket = obj['property_id']

# Stream WCC components from server
reader = client.do_get(flight.Ticket(json.dumps(ticket).encode('utf-8')))
components = reader.read_all().to_pandas()
print(components.head())
print("count = " + str(components.count(axis = 0)['component']))
print("sum = " + str(components.sum(axis = 0)['component']))
print("max = " + str(components.max(axis = 0)['component']))
