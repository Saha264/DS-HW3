#!/usr/bin/env python3

"""
take input and form the adjacency lists. Node 0 starts at dist 0 and every other node at INF.


"""

import sys
INF=1000000

def main():
    data= sys.stdin.read().split()
    if len(data) < 2:
        return
    V,E= int(data[0]), int(data[1])
    
    adj= [[]for _ in range(V)]
    pos =2
    for _ in range(E):
        u,v,w = int(data[pos]),int(data[pos+1]),int(data[pos+2])
        pos+=3
        if 0<=u <V and 0<=v < V:
            adj[u].append("%d,%d" %(v,w))
            
    out=sys.stdout
    for i in range(V):
        out.write("%d\t%d|%s\n" % (i, 0 if i == 0 else INF, ";".join(adj[i])))


if __name__ == "__main__":
    main()

    