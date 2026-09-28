#!/usr/bin/env python3
"""combiner.py -- local pre-aggregation, and optionally the partitioner.

Collapses all D records for the same node into a single minimum, and passes
N records through untouched. This is a pure optimisation: it cuts the volume
of data crossing the shuffle without changing the result, because min() is
associative and commutative.

Combiner expects the keys to be sorted at input.

    combiner.py                      records -> stdout          (single reducer)
    combiner.py <R> <prefix>         records -> <prefix>0 .. <prefix>R-1

With R and a prefix, each record is also partitioned by `node_id % R`, which
is what lets several reducers run in parallel: every record for a given node
lands in the same partition, so no node is ever split across two reducers.
Node ids are dense integers, so the modulo keeps the partitions balanced.
All R files are created even when a partition ends up empty, so the driver can
merge them without special cases. Each file keeps the sorted order of the
input, so `sort -m` can still merge partitions across mappers.
"""
import sys

INF = 1000000


def main():
    nparts = 1
    outs = [sys.stdout]
    if len(sys.argv) == 3:
        nparts = int(sys.argv[1])
        prefix = sys.argv[2]
        outs = [open("%s%d" % (prefix, r), "w") for r in range(nparts)]
    elif len(sys.argv) != 1:
        sys.stderr.write("usage: combiner.py [<num_partitions> <output_prefix>]\n")
        return 1

    def stream(node):
        return outs[0] if nparts == 1 else outs[int(node) % nparts]

    cur_node = None
    best = INF

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        node, _, value = line.partition("\t")

        if node != cur_node:
            if cur_node is not None and best < INF:
                # only write when there's a new key
                stream(cur_node).write("%s\tD|%d\n" % (cur_node, best))
            cur_node, best = node, INF

        if value.startswith("N|"):
            stream(node).write(line + "\n")   # structure records pass straight through
        elif value.startswith("D|"):
            cand = int(value[2:])
            if cand < best:
                best = cand

    if cur_node is not None and best < INF:
        stream(cur_node).write("%s\tD|%d\n" % (cur_node, best))

    if nparts > 1:
        for f in outs:
            f.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
