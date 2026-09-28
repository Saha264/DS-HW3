#!/bin/bash
# verify.sh -- checks the MapReduce pipeline against sequential Dijkstra.
#
# Every graph is run at several map-task and reducer counts. The answer must
# not depend on either: mappers must not lose records at a chunk boundary, and
# the partitioner must not split one node's proposals across two reducers.
cd "$(dirname "$0")"
FAIL=0

check() {
    local graph=$1 nmap=$2 nred=$3
    ./sssp_local.sh "$graph" /tmp/mr.out "$nmap" "$nred" > /dev/null
    python3 dijkstra_ref.py < "$graph" > /tmp/ref.out
    if diff -q /tmp/mr.out /tmp/ref.out > /dev/null; then
        echo "PASS  $graph (map tasks=$nmap, reducers=$nred)"
    else
        echo "FAIL  $graph (map tasks=$nmap, reducers=$nred)"
        diff /tmp/mr.out /tmp/ref.out | head
        FAIL=1
    fi
}

for f in tests/*.txt; do
    for n in 1 2 3; do
        for r in 1 2 3; do
            check "$f" "$n" "$r"
        done
    done
done

for s in 1 2 3; do
    python3 gen_graph.py 150 600 "$s" > /tmp/rand.txt
    for r in 1 4 7; do
        check /tmp/rand.txt 4 "$r"
    done
done

# The iteration count must also be independent of the reducer count.
python3 gen_graph.py 400 1600 11 > /tmp/iter.txt
COUNTS=""
for r in 1 2 3 5 8; do
    C=$(./sssp_local.sh /tmp/iter.txt /tmp/iter_out.txt 4 "$r" | grep -c "^iteration")
    COUNTS="$COUNTS $r:$C"
done
UNIQ=$(echo "$COUNTS" | tr ' ' '\n' | sed '/^$/d' | cut -d: -f2 | sort -u | wc -l)
if [ "$UNIQ" -eq 1 ]; then
    echo "PASS  iteration count identical across reducer counts ($COUNTS )"
else
    echo "FAIL  iteration count varies with reducer count ($COUNTS )"
    FAIL=1
fi

[ $FAIL -eq 0 ] && echo "ALL TESTS PASSED" || echo "SOME TESTS FAILED"
exit $FAIL
