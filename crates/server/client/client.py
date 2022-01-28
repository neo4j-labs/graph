import json
import pyarrow as pa
import pyarrow.flight as flight

location = flight.Location.for_grpc_tcp("localhost", 50051)
client = flight.FlightClient(location)

result = client.do_action(flight.Action("create", '{"graph_name": "herbert", "path": "/Users/s1ck/Devel/Rust/graph/resources/example.el"}'.encode('utf-8')))
obj = next(result).body.to_pybytes().decode()
print(obj)

result = client.do_action(flight.Action("algo", '{"graph_name": "herbert", "algo_name": "PageRank", "mutate_property": "page_rank"}'.encode('utf-8')))
obj = next(result).body.to_pybytes().decode()
json_obj = json.loads(obj)
print(json_obj['property_id'])

ticket = json_obj['property_id']

reader = client.do_get(flight.Ticket(json.dumps(ticket).encode('utf-8')))
read_table = reader.read_all()
print(read_table)
print(read_table.to_pandas().head())
