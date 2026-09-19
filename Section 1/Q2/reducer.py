#!/usr/bin/env python3
"""reducer.py -- keeps the minimum distance for each node.

stdin : shuffled+sorted "node \t N|dist|adj" and "node \t D|cand" records
stdout: "node \t dist|adj"  -- exactly the next iteration's input

Whenever a node's distance actually decreases, a Hadoop-style counter is
written to stderr; the driver script reads it to decide whether to iterate.
"""
import sys

INF = 1000000


def main():
    out = sys.stdout
    updates = 0

    node = None
    old_dist = INF     # distance the node had at the start of this iteration
    best = INF         # best candidate distance seen this iteration
    adj = ""

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        key, _, value = line.partition("\t")

        if key != node:                                  # key boundary
            if node is not None:
                if best < old_dist:
                    updates += 1
                out.write("%s\t%d|%s\n" % (node, min(best, old_dist), adj))
            node, old_dist, best, adj = key, INF, INF, ""

        if value.startswith("N|"):
            _, old_str, adj = value.split("|", 2)        # "N|old_dist|adj"
            old_dist = int(old_str)
        elif value.startswith("D|"):
            cand = int(value[2:])
            if cand < best:
                best = cand

    if node is not None:
        if best < old_dist:
            updates += 1
        out.write("%s\t%d|%s\n" % (node, min(best, old_dist), adj))

    sys.stderr.write("reporter:counter:SSSP,UPDATED,%d\n" % updates)


if __name__ == "__main__":
    main()
