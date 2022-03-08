import gc
import unladen_swallow

g = unladen_swallow.Ungraph.load("/Users/knut/dev/datasets/graph500/scale_24.graph")

print(f"degree(0) = {g.out_degree(0)}")
print(f"(0, 0) = {g.target(0, 0)}")
print(f"(0, 1) = {g.target(0, 1)}")
print(f"(0, 2) = {g.target(0, 2)}")
print(f"(0, 3) = {g.target(0, 3)}")

print("")
input("[Enter]...")
print("")


a = g.out_neighbors(0)
print(f"{a!r}")
print(f"{a.base!r} -> {unladen_swallow.show_directed_nb(a.base)}")


print("")
input("[Enter]...")
print("")


del g
gc.collect()


print("")
input("[Enter]...")
print("")


print(f"{a!r}")
print(f"{a.base!r} -> {unladen_swallow.show_directed_nb(a.base)}")


del a
gc.collect()
