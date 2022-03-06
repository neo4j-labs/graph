#!/usr/bin/env python

import logging
import sys
import unladen_swallow

try:
    FILE = sys.argv[1]
except IndexError:
    print("first argument (graph 500 file) required")
    sys.exit(1)

# install logging
logging.basicConfig(format="%(message)s")
logging.getLogger().setLevel(logging.INFO)

# load graph
g = unladen_swallow.Graph.load(FILE, unladen_swallow.Layout.Sorted)
print(f"{g!r}")

# run page rank
pr = g.page_rank(max_iteration=20, tolerance=1e-4, damping_factor=0.85)
print(f"{pr!r}")

for node_id, score in enumerate(pr):
    if score > 0.00042:
        print(f"{node_id} score: {score}")
