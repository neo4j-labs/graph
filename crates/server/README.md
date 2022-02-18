# graph_server

An [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) server
implementation that allows clients to create and manage graphs in memory,
run algorithms on them and stream results back to the client.

Clients communicate with the server via an Arrow Flight client, such as
[pyarrow](https://pypi.org/project/pyarrow/). Server commands, also called
Flight actions, are encoded via JSON. Currently supported commands include
creating graphs, relabeling graphs and computing algorithms, such as PageRank,
Triangle Count and SSSP. Algorithm results are streamed to the client via
the do_get command and nicely wrapped in Arrow record batches.

Check the `examples` folder for scripts that demonstrate client-server interaction.

License: MIT
