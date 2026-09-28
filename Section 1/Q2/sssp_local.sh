#!/bin/bash
# Local MapReduce driver for iterative SSSP (no cluster needed).
#
#   ./sssp_local.sh <input-file> [output-file] [num-map-tasks] [num-reducers]
#
# One MapReduce job per Bellman-Ford iteration:
#     mapper | sort | combiner  ->  partitions  ->  merge  ->  reducer
# repeated until the reducers report zero distance updates between them.
#
# With num-reducers > 1 the combiner partitions its output by node id, so each
# reducer owns a disjoint set of nodes and they can run concurrently.

INPUT_FILE=${1:-tests/sample.txt}
OUTPUT_FILE=${2:-output.txt}
NMAP=${3:-3}
NRED=${4:-1}

cd "$(dirname "$0")"

if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: input file '$INPUT_FILE' not found." >&2
    echo "usage: ./sssp_local.sh <input-file> [output-file] [num-map-tasks] [num-reducers]" >&2
    exit 1
fi

WORK=work_sssp
rm -rf "$WORK"; mkdir -p "$WORK"

python3 prep.py < "$INPUT_FILE" > "$WORK/state.0"
V=$(wc -l < "$WORK/state.0")

ITER=0
while [ "$ITER" -lt "$V" ]; do
    NEXT=$((ITER + 1))

    # split the state into NMAP chunks -- each chunk is one independent map task
    rm -f "$WORK"/chunk_* "$WORK"/map_* "$WORK"/shuffled.* "$WORK"/counter.* "$WORK"/part.*
    split -d -a 2 -n l/$NMAP "$WORK/state.$ITER" "$WORK/chunk_"

    # map + local sort + combine, partitioned by node id
    for CHUNK in "$WORK"/chunk_*; do
        TID=${CHUNK##*chunk_}
        python3 mapper.py < "$CHUNK" | sort | python3 combiner.py "$NRED" "$WORK/map_${TID}_part"
    done

    # shuffle: merge each partition across all mappers, then reduce it
    R=0
    while [ "$R" -lt "$NRED" ]; do
        sort -m "$WORK"/map_*_part"$R" > "$WORK/shuffled.$R"
        python3 reducer.py < "$WORK/shuffled.$R" \
            > "$WORK/part.$R" 2> "$WORK/counter.$R" &
        R=$((R + 1))
    done
    wait
    cat "$WORK"/part.* > "$WORK/state.$NEXT"

    UPDATED=$(cat "$WORK"/counter.* | sed -n 's/.*UPDATED,\([0-9]*\).*/\1/p' \
              | awk '{ s += $1 } END { print s + 0 }')
    echo "iteration $NEXT: ${UPDATED:-0} distance update(s)"
    ITER=$NEXT
    [ "${UPDATED:-0}" -eq 0 ] && break
done

python3 format_output.py < "$WORK/state.$ITER" > "$OUTPUT_FILE"
echo "SSSP pipeline completed in $ITER iteration(s). Output saved to $OUTPUT_FILE"
