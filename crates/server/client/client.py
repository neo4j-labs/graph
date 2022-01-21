import pyarrow as pa
import pyarrow.flight as flight

location = flight.Location.for_grpc_tcp("localhost", 50051)
client = flight.FlightClient(location)

result = client.do_action(flight.Action("create", '{"graph_name": "herbert", "path": "/home/s1ck/Devel/Rust/graph/resources/example.el"}'.encode('utf-8')))
obj = next(result).body.to_pybytes().decode()
print(obj)
