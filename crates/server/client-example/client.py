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
    "path": graph_path
}

result = client.do_action(flight.Action("create", json.dumps(create_action).encode('utf-8')))
obj = next(result).body.to_pybytes().decode()
print("graph create result = " + str(obj))

algo_action = {
    "graph_name": graph_name,
    "algo_name": "PageRank",
    "property_key": "page_rank"
}

result = client.do_action(flight.Action("algo", json.dumps(algo_action).encode('utf-8')))
obj = next(result).body.to_pybytes().decode()
print("page rank result = " + str(obj))

json_obj = json.loads(obj)
ticket = json_obj['property_id']

reader = client.do_get(flight.Ticket(json.dumps(ticket).encode('utf-8')))
scores = reader.read_all().to_pandas()
print(scores.head())
print("count = " + str(scores.count(axis = 0)['page_rank']))
print("sum = " + str(scores.sum(axis = 0)['page_rank']))
