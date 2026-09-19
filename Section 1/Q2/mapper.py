#!/usr/bin/env python3
"""mapper.py -- one Bellman-Ford relaxation step.

stdin : "node \t dist|adj"
stdout: "node \t N|dist|adj"   structure record: carries the graph and the
                               node's OLD distance into the next iteration
        "v    \t D|dist+w"     candidate distance for each out-neighbour v,
                               emitted only when this node is reachable

Every line is handled independently -- no communication between mappers.
"""
import sys

INF = 1000000


def main():
    out = sys.stdout
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        node, _, value = line.partition("\t")
        dist_str, _, adj = value.partition("|")
        dist = int(dist_str)

        # keep the graph structure (and the old distance) alive
        out.write("%s\tN|%d|%s\n" % (node, dist, adj))

        if dist >= INF or not adj:          #not adj is jsut checking if adj is an empty string
            continue                      # unreachable: nothing to relax

        for edge in adj.split(";"):
            v, _, w = edge.partition(",")
            out.write("%s\tD|%d\n" % (v, dist + int(w)))


if __name__ == "__main__":
    main()
