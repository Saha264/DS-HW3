#!/usr/bin/env python3
"""dijkstra_ref.py -- sequential reference implementation for verification.

Same input and output format as the MapReduce pipeline, so the two outputs
can be compared with a plain `diff`.
"""
import heapq
import sys

INF = 1000000


def main():
    data = sys.stdin.read().split()
    V, E = int(data[0]), int(data[1])
    g = [[] for _ in range(V)]
    pos = 2
    for _ in range(E):
        u, v, w = int(data[pos]), int(data[pos + 1]), int(data[pos + 2])
        pos += 3
        if 0 <= u < V and 0 <= v < V:
            g[u].append((v, w))

    dist = [INF] * V
    dist[0] = 0
    pq = [(0, 0)]
    while pq:
        d, u = heapq.heappop(pq)
        if d != dist[u]:
            continue
        for v, w in g[u]:
            if d + w < dist[v]:
                dist[v] = d + w
                heapq.heappush(pq, (d + w, v))

    out = sys.stdout
    for i in range(V):
        out.write("%d INF\n" % i if dist[i] >= INF else "%d %d\n" % (i, dist[i]))


if __name__ == "__main__":
    main()
