#!/bin/bash
# Local MapReduce driver for iterative SSSP (no cluster needed).
#
#   ./sssp_local.sh <input-file> [output-file] [num-map-tasks]
#
# One MapReduce job per Bellman-Ford iteration:
#     mapper | sort | combiner | sort | reducer
# repeated until the reducer reports zero distance updates.

INPUT_FILE=${1:-tests/sample.txt}
OUTPUT_FILE=${2:-output.txt}
NMAP=${3:-3}

cd "$(dirname "$0")"

if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: input file '$INPUT_FILE' not found." >&2
    echo "usage: ./sssp_local.sh <input-file> [output-file] [num-map-tasks]" >&2
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
    rm -f "$WORK"/chunk_*
    split -d -a 2 -n l/$NMAP "$WORK/state.$ITER" "$WORK/chunk_"

    : > "$WORK/map.$NEXT"
    for CHUNK in "$WORK"/chunk_*; do
        python3 mapper.py < "$CHUNK" | sort | python3 combiner.py >> "$WORK/map.$NEXT"
    done

    # shuffle: group all records of a node together, then reduce
    sort "$WORK/map.$NEXT" \
        | python3 reducer.py > "$WORK/state.$NEXT" 2> "$WORK/counter.$NEXT"

    UPDATED=$(sed -n 's/.*UPDATED,\([0-9]*\).*/\1/p' "$WORK/counter.$NEXT")
    echo "iteration $NEXT: ${UPDATED:-0} distance update(s)"
    ITER=$NEXT
    [ "${UPDATED:-0}" -eq 0 ] && break
done

python3 format_output.py < "$WORK/state.$ITER" > "$OUTPUT_FILE"
echo "SSSP pipeline completed in $ITER iteration(s). Output saved to $OUTPUT_FILE"
