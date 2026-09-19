#!/usr/bin/env python3
"""gen_graph.py -- reproducible random graph generator.

    python3 gen_graph.py <V> <E> [seed] > graph.txt

Same seed + same V/E always produces the same graph, so experiments can be
reproduced exactly. Edge weights are uniform in [1, 1000] per the assignment.
"""
import random
import sys

V, E = int(sys.argv[1]), int(sys.argv[2])
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 42
rng = random.Random(seed)

lines = ["%d %d" % (V, E)]
for _ in range(E):
    lines.append("%d %d %d" % (rng.randrange(V), rng.randrange(V), rng.randint(1, 1000)))
print("\n".join(lines))
