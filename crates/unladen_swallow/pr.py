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
FORMAT = "%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

# run page rank
pr = unladen_swallow.page_rank(FILE)
print(f"{pr!r}")
