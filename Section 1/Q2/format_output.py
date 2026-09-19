#!/usr/bin/env python3
"""format_output.py -- final node state -> the required output format.

stdin : "node \t dist|adj" in any order
stdout: "node dist" sorted by node id ascending, INF printed as "INF"
"""
import sys

INF = 1000000


def main():
    dist = {}
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        node, _, value = line.partition("\t")
        dist[int(node)] = int(value.partition("|")[0])

    out = sys.stdout
    for node in sorted(dist):              #here dist is a dict, so by sorting we are sorting the keys and not the values
        d = dist[node]
        out.write("%d INF\n" % node if d >= INF else "%d %d\n" % (node, d))


if __name__ == "__main__":
    main()
