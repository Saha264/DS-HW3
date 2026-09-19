#!/bin/bash
# verify.sh -- checks the MapReduce pipeline against sequential Dijkstra.
# Runs every graph in tests/ with 1, 2 and 3 map tasks, plus 3 random graphs.
cd "$(dirname "$0")"
FAIL=0
check() {
    ./sssp_local.sh "$1" /tmp/mr.out "$2" > /dev/null
    python3 dijkstra_ref.py < "$1" > /tmp/ref.out
    if diff -q /tmp/mr.out /tmp/ref.out > /dev/null; then
        echo "PASS  $1 (map tasks=$2)"
    else
        echo "FAIL  $1 (map tasks=$2)"; diff /tmp/mr.out /tmp/ref.out | head; FAIL=1
    fi
}
for f in tests/*.txt; do for n in 1 2 3; do check "$f" "$n"; done; done
for s in 1 2 3; do
    python3 gen_graph.py 150 600 "$s" > /tmp/rand.txt
    check /tmp/rand.txt 4
done
[ $FAIL -eq 0 ] && echo "ALL TESTS PASSED" || echo "SOME TESTS FAILED"
exit $FAIL
