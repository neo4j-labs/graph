from IPython import get_ipython
import gc
import unladen_swallow

ipython = get_ipython()


ipython.run_line_magic(
    "time",
    'g = unladen_swallow.Ungraph.load("/Users/knut/dev/datasets/graph500/scale_24.graph")',
)

print(f"{g!r} degree(0) = {g.degree(0)}")

ipython.run_line_magic("time", "g.reorder_by_degree()")

print(f"{g!r} degree(0) = {g.degree(0)}")

a = g.neighbors(0)
print(f"{a!r} -> {a.base!r}")


ipython.run_line_magic("time", "len(g.copy_neighbors(0))")
ipython.run_line_magic("time", "len(g.neighbors(0))")

del g
gc.collect()


del a
gc.collect()
