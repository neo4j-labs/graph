import json
import pyarrow as pa
import pyarrow.flight as flight
import sys

location = flight.Location.for_grpc_tcp("localhost", 50051)
client = flight.FlightClient(location)
graph_name = sys.argv[1]
file_format = sys.argv[2]
graph_path = sys.argv[3]
start_node = int(sys.argv[4])
delta = float(sys.argv[5])

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

# Compute SSSP
compute_action = {
    "graph_name": graph_name,
    "algorithm": {
        "Sssp": {
            "start_node": start_node,
            "delta": delta ,
        }
    },
    "property_key": "sssp"
}

result = client.do_action(flight.Action("compute", json.dumps(compute_action).encode('utf-8')))
obj = json.loads(next(result).body.to_pybytes().decode())
print("SSSP result")
print(json.dumps(obj, indent = 4))

ticket = obj['property_id']

# Stream SSSP distances from server
reader = client.do_get(flight.Ticket(json.dumps(ticket).encode('utf-8')))
distances = reader.read_all().to_pandas()
distances = distances.query('distance < 20000')
print(distances.head())
print("count = " + str(distances.count(axis = 0)['distance']))
print("sum = " + str(distances.sum(axis = 0)['distance']))
print("max = " + str(distances.max(axis = 0)['distance']))
