#!/usr/bin/env python3
"""combiner.py -- local pre-aggregation, run on each mapper's sorted output.

Collapses all D records for the same node into a single minimum, and passes
N records through untouched. This is a pure optimisation: it cuts the volume
of data crossing the shuffle without changing the result, because min() is
associative and commutative.

Combiner expects the keys to be sorted at input

stdin/stdout: same record format as the mapper's output.
"""
import sys

INF = 1000000


def main():
    out = sys.stdout
    cur_node = None
    best = INF

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        node, _, value = line.partition("\t")

        if node != cur_node:
            if cur_node is not None and best < INF:
                out.write("%s\tD|%d\n" % (cur_node, best))      #only write when theres a new key
            cur_node, best = node, INF

        if value.startswith("N|"):
            out.write(line + "\n")        # structure records pass straight through
        elif value.startswith("D|"):
            cand = int(value[2:])
            if cand < best:
                best = cand

    if cur_node is not None and best < INF:
        out.write("%s\tD|%d\n" % (cur_node, best))


if __name__ == "__main__":
    main()
