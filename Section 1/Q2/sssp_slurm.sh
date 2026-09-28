#!/bin/bash
#SBATCH --job-name=sssp_mapreduce
#SBATCH --output=sssp_results_%j.out
#SBATCH --error=sssp_results_%j.err
#SBATCH -A cs3401
#SBATCH --qos=normal
#SBATCH -p debug
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#SBATCH --time=00:30:00

# ============================================================
# Iterative SSSP via distributed MapReduce.
#
#   sbatch --ntasks=<P> sssp_slurm.sh <input-file> [output-file]
#
# One MapReduce job per Bellman-Ford iteration. Mappers and combiners run in
# parallel across the allocated tasks; the shuffle gathers everything and a
# single reducer produces the next state. Per-iteration stage timings go to
# perf_results/sssp_iteration_timings.csv.
#
# Without Slurm the map tasks run sequentially, so the same script also works
# on a laptop for testing.
# ============================================================

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
cd "$SCRIPT_DIR" || exit 1

INPUT_FILE=${1:-tests/sample.txt}
OUTPUT_FILE=${2:-sssp_output.txt}
NTASKS=${SLURM_NTASKS:-4}

if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: input file '$INPUT_FILE' not found." >&2
    exit 1
fi

now_ns() { date +%s%N; }
# awk, not bc: bc is not installed on a minimal Rocky image.
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.6f", (b - a) / 1000000000 }'; }

# Unique per job: several of these can be scheduled on the SAME node at the
# same time, and a shared work directory makes them delete each other's files.
WORK="$SCRIPT_DIR/work_sssp_${SLURM_JOB_ID:-local}_${NTASKS}"
RESULTS_DIR="$SCRIPT_DIR/perf_results"
rm -rf "$WORK"; mkdir -p "$WORK" "$RESULTS_DIR"
SUMMARY_FILE="$RESULTS_DIR/sssp_iteration_timings.csv"
echo "iteration,num_tasks,map_combine_time_s,shuffle_time_s,reduce_time_s,iteration_time_s,updates" > "$SUMMARY_FILE"

run_map_stage() {
    if command -v srun > /dev/null 2>&1 && [ -n "$SLURM_JOB_ID" ]; then
        srun --ntasks="$NTASKS" bash -c "
            TID=\$(printf '%02d' \$SLURM_PROCID)
            cd '$SCRIPT_DIR' || exit 1
            python3 mapper.py < '$WORK/chunk_'\$TID \
                | sort \
                | python3 combiner.py > '$WORK/map_'\$TID'.out'
        "
    else
        for CHUNK in "$WORK"/chunk_*; do
            TID=${CHUNK##*chunk_}
            python3 mapper.py < "$CHUNK" | sort | python3 combiner.py > "$WORK/map_$TID.out"
        done
    fi
}

echo "============================================"
echo "Iterative SSSP MapReduce"
echo "Date:    $(date)"
echo "Input:   $INPUT_FILE"
echo "Nodes:   ${SLURM_JOB_NODELIST:-(no Slurm: running locally)}"
echo "Tasks:   $NTASKS"
echo "Python:  $(python3 --version 2>&1)"
echo "============================================"

python3 prep.py < "$INPUT_FILE" > "$WORK/state.0" || exit 1
V=$(wc -l < "$WORK/state.0")
echo "Vertices: $V"
echo ""

TOTAL_START=$(now_ns)
ITER=0
while [ "$ITER" -lt "$V" ]; do
    NEXT=$((ITER + 1))
    ITER_START=$(now_ns)

    rm -f "$WORK"/chunk_* "$WORK"/map_*.out
    split -d -a 2 -n l/$NTASKS "$WORK/state.$ITER" "$WORK/chunk_"

    STAGE_START=$(now_ns)
    run_map_stage
    STAGE_END=$(now_ns)
    MAP_TIME=$(elapsed "$STAGE_START" "$STAGE_END")

    # All records for one node must reach the same reducer, so this merge is global.
    STAGE_START=$(now_ns)
    sort -m "$WORK"/map_*.out > "$WORK/shuffled.$NEXT"
    STAGE_END=$(now_ns)
    SHUFFLE_TIME=$(elapsed "$STAGE_START" "$STAGE_END")

    STAGE_START=$(now_ns)
    python3 reducer.py < "$WORK/shuffled.$NEXT" \
        > "$WORK/state.$NEXT" 2> "$WORK/counter.$NEXT"
    STAGE_END=$(now_ns)
    REDUCE_TIME=$(elapsed "$STAGE_START" "$STAGE_END")

    ITER_END=$(now_ns)
    ITER_TIME=$(elapsed "$ITER_START" "$ITER_END")

    UPDATED=$(sed -n 's/.*UPDATED,\([0-9]*\).*/\1/p' "$WORK/counter.$NEXT")
    UPDATED=${UPDATED:-0}

    echo "iteration $NEXT: map=${MAP_TIME}s shuffle=${SHUFFLE_TIME}s reduce=${REDUCE_TIME}s total=${ITER_TIME}s updates=$UPDATED"
    echo "${NEXT},${NTASKS},${MAP_TIME},${SHUFFLE_TIME},${REDUCE_TIME},${ITER_TIME},${UPDATED}" >> "$SUMMARY_FILE"

    ITER=$NEXT
    [ "$UPDATED" -eq 0 ] && break
done
TOTAL_END=$(now_ns)
TOTAL_TIME=$(elapsed "$TOTAL_START" "$TOTAL_END")

python3 format_output.py < "$WORK/state.$ITER" > "$OUTPUT_FILE"
rm -rf "$WORK"

echo ""
echo "============================================"
echo "Converged in $ITER iteration(s)"
echo "TOTAL: ${TOTAL_TIME}s"
echo "Output:      $OUTPUT_FILE"
echo "Timings CSV: $SUMMARY_FILE"
echo "============================================"
