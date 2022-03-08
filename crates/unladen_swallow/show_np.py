import gc
import unladen_swallow

g = unladen_swallow.Graph.load("/Users/knut/dev/datasets/graph500/scale_16.graph")

print(f"degree(0) = {g.out_degree(0)}")


print("")
input("[Enter]...")
print("")


a = g.out_neighbors(0)
print(f"{a!r}")
print(f"{a.base!r}")


print("")
input("[Enter]...")
print("")

del g
gc.collect()


print("")
input("[Enter]...")
print("")


print(f"{a!r}")
print(f"{a.base!r}")

del a
gc.collect()

print("")
print("Done")
input("[Enter]...")
