#!/bin/bash
#SBATCH --job-name=sssp_mapreduce
#SBATCH --output=sssp_results_%j.out
#SBATCH --error=sssp_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00

# ============================================================
# Iterative SSSP via distributed MapReduce -- Slurm script
#
#   sbatch sssp_slurm.sh <input-file> [output-file]
#
# One MapReduce job per Bellman-Ford iteration. Mappers + combiners run
# in parallel across the allocated tasks; the shuffle gathers everything
# and a single reducer produces the next state. Per-iteration stage
# timings are recorded to perf_results/sssp_iteration_timings.csv.
# ============================================================

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
cd "$SCRIPT_DIR"

INPUT_FILE=${1:-tests/sample.txt}
OUTPUT_FILE=${2:-sssp_output.txt}
[ -z "$SLURM_NTASKS" ] && SLURM_NTASKS=4

WORK="$SCRIPT_DIR/work_sssp"
RESULTS_DIR="$SCRIPT_DIR/perf_results"
rm -rf "$WORK"; mkdir -p "$WORK" "$RESULTS_DIR"
SUMMARY_FILE="$RESULTS_DIR/sssp_iteration_timings.csv"
echo "iteration,num_tasks,map_combine_time_s,shuffle_time_s,reduce_time_s,iteration_time_s,updates" > "$SUMMARY_FILE"

echo "============================================"
echo "Iterative SSSP MapReduce Benchmark"
echo "Date: $(date)"
echo "Input: $INPUT_FILE"
echo "Nodes Allocated: $SLURM_JOB_NODELIST"
echo "Number of Tasks: $SLURM_NTASKS"
echo "============================================"

python3 prep.py < "$INPUT_FILE" > "$WORK/state.0"
V=$(wc -l < "$WORK/state.0")
echo "Vertices: $V"
echo ""

TOTAL_START=$(date +%s%N)
ITER=0
while [ "$ITER" -lt "$V" ]; do
    NEXT=$((ITER + 1))
    ITER_START=$(date +%s%N)

    # ---- Setup: partition the node state across tasks ----
    rm -f "$WORK"/chunk_* "$WORK"/map_*.out
    split -d -a 2 -n l/$SLURM_NTASKS "$WORK/state.$ITER" "$WORK/chunk_"

    # ---- Stage 1: distributed map + local sort + combine ----
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        cd '$SCRIPT_DIR'
        python3 mapper.py < '$WORK/chunk_'\$TID \
            | sort \
            | python3 combiner.py > '$WORK/map_'\$TID'.out'
    "
    STAGE_END=$(date +%s%N)
    MAP_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)

    # ---- Stage 2: shuffle -- gather and globally sort by node id ----
    # All records for one node must reach the same reducer, so the merge is global.
    STAGE_START=$(date +%s%N)
    sort -m "$WORK"/map_*.out > "$WORK/shuffled.$NEXT"
    STAGE_END=$(date +%s%N)
    SHUFFLE_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)

    # ---- Stage 3: reduce ----
    STAGE_START=$(date +%s%N)
    python3 reducer.py < "$WORK/shuffled.$NEXT" \
        > "$WORK/state.$NEXT" 2> "$WORK/counter.$NEXT"
    STAGE_END=$(date +%s%N)
    REDUCE_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)

    ITER_END=$(date +%s%N)
    ITER_TIME=$(echo "scale=6; ($ITER_END - $ITER_START) / 1000000000" | bc)

    UPDATED=$(sed -n 's/.*UPDATED,\([0-9]*\).*/\1/p' "$WORK/counter.$NEXT")
    UPDATED=${UPDATED:-0}

    echo "iteration $NEXT: map=${MAP_TIME}s shuffle=${SHUFFLE_TIME}s reduce=${REDUCE_TIME}s total=${ITER_TIME}s updates=$UPDATED"
    echo "${NEXT},${SLURM_NTASKS},${MAP_TIME},${SHUFFLE_TIME},${REDUCE_TIME},${ITER_TIME},${UPDATED}" >> "$SUMMARY_FILE"

    ITER=$NEXT
    [ "$UPDATED" -eq 0 ] && break
done
TOTAL_END=$(date +%s%N)
TOTAL_TIME=$(echo "scale=6; ($TOTAL_END - $TOTAL_START) / 1000000000" | bc)

python3 format_output.py < "$WORK/state.$ITER" > "$OUTPUT_FILE"

echo ""
echo "============================================"
echo "Converged in $ITER iteration(s)"
echo "TOTAL: ${TOTAL_TIME}s"
echo "Output: $OUTPUT_FILE"
echo "Timings CSV: $SUMMARY_FILE"
echo "============================================"
echo ""
echo "--- CSV Summary ---"
column -t -s',' "$SUMMARY_FILE"
