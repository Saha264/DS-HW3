#!/bin/bash
#SBATCH --job-name=sssp_bench
#SBATCH --dependency=singleton
#SBATCH --output=bench_results_%j.out
#SBATCH --error=bench_results_%j.err
#SBATCH -A cs3401
#SBATCH --qos=normal
#SBATCH -p debug
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#SBATCH --time=01:00:00

# ============================================================
# Benchmark sweep. Submit once per process count:
#
#   for p in 1 2 4 8; do sbatch --ntasks=$p bench_slurm.sh; done
#
# --dependency=singleton makes jobs sharing this name and user run ONE AT A
# TIME, so the four process counts do not compete for cores on the same node.
# Submit all four at once; Slurm queues them and runs them back to back.
#
# For every graph in GRAPHS it runs the full iterative pipeline with the
# allocated number of map tasks, verifies the answer against sequential
# Dijkstra, and appends
#   perf_results/bench_<P>tasks.csv      one row per iteration
#   perf_results/summary_<P>tasks.csv    one row per graph
# The filenames include P so concurrent jobs cannot overwrite each other.
# ============================================================

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
cd "$SCRIPT_DIR" || exit 1

NTASKS=${SLURM_NTASKS:-4}
# Reducers default to the map-task count, so one knob (P) scales the whole job.
# Override with REDUCERS=1 to reproduce the single-reducer baseline.
NRED=${REDUCERS:-$NTASKS}
SEED=7

GRAPHS=(
    "1000 5000"
    "10000 50000"
    "50000 250000"
)

WORK="$SCRIPT_DIR/work_bench_${SLURM_JOB_ID:-local}_${NTASKS}"
RESULTS_DIR="$SCRIPT_DIR/perf_results"
GRAPH_DIR="$SCRIPT_DIR/bench_graphs"
mkdir -p "$RESULTS_DIR" "$GRAPH_DIR"

TAG="${NTASKS}tasks_${NRED}red"
ITER_CSV="$RESULTS_DIR/bench_${TAG}.csv"
SUM_CSV="$RESULTS_DIR/summary_${TAG}.csv"
CAL_CSV="$RESULTS_DIR/launch_overhead_${TAG}.csv"
echo "V,E,num_tasks,num_reducers,iteration,map_combine_time_s,shuffle_time_s,reduce_time_s,iteration_time_s,updates,intermediate_records,intermediate_bytes" > "$ITER_CSV"
echo "V,E,num_tasks,num_reducers,iterations,total_time_s,sequential_dijkstra_time_s,peak_reducer_rss_kb,correct" > "$SUM_CSV"

now_ns() { date +%s%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.6f", (b - a) / 1000000000 }'; }

# Each iteration pays one srun launch. Time an empty launch at this task count
# so the map stage can be split into fixed overhead versus real work.
calibrate_launch() {
    echo "num_tasks,rep,srun_launch_s" > "$CAL_CSV"
    if command -v srun > /dev/null 2>&1 && [ -n "$SLURM_JOB_ID" ]; then
        for REP in 1 2 3 4 5; do
            C0=$(now_ns)
            srun --ntasks="$NTASKS" true > /dev/null 2>&1
            C1=$(now_ns)
            echo "${NTASKS},${REP},$(elapsed "$C0" "$C1")" >> "$CAL_CSV"
        done
        echo "srun launch overhead at P=$NTASKS:"
        awk -F, 'NR>1 { n++; s+=$3; if (m=="" || $3<m) m=$3 } END {
            printf "    mean %.4fs over %d launches, fastest %.4fs\n", s/n, n, m }' "$CAL_CSV"
        echo
    fi
}

run_map_stage() {
    if command -v srun > /dev/null 2>&1 && [ -n "$SLURM_JOB_ID" ]; then
        srun --ntasks="$NTASKS" bash -c "
            TID=\$(printf '%02d' \$SLURM_PROCID)
            cd '$SCRIPT_DIR' || exit 1
            python3 mapper.py < '$WORK/chunk_'\$TID \
                | sort \
                | python3 combiner.py $NRED '$WORK/map_'\$TID'_part'
        "
    else
        for CHUNK in "$WORK"/chunk_*; do
            TID=${CHUNK##*chunk_}
            python3 mapper.py < "$CHUNK" | sort | python3 combiner.py "$NRED" "$WORK/map_${TID}_part"
        done
    fi
}

echo "============================================"
echo "MapReduce SSSP benchmark sweep"
echo "Date:    $(date)"
echo "Node(s): ${SLURM_JOB_NODELIST:-(no Slurm: local run)}"
echo "Map tasks: $NTASKS"
echo "Reducers:  $NRED"
echo "Python:  $(python3 --version 2>&1)"
echo "Seed:    $SEED"
echo "============================================"
echo

calibrate_launch

for PAIR in "${GRAPHS[@]}"; do
    set -- $PAIR
    V=$1; E=$2
    GRAPH="$GRAPH_DIR/g_${V}_${E}_s${SEED}.txt"
    # Generate into a private temp file and rename, so two concurrent jobs
    # never read a half-written graph. Same seed => identical file either way.
    if [ ! -s "$GRAPH" ]; then
        TMPG="$GRAPH.tmp.$$"
        python3 gen_graph.py "$V" "$E" "$SEED" > "$TMPG" && mv -f "$TMPG" "$GRAPH"
    fi

    echo "--- V=$V E=$E  (map tasks=$NTASKS, reducers=$NRED)"
    rm -rf "$WORK"; mkdir -p "$WORK"
    python3 prep.py < "$GRAPH" > "$WORK/state.0" || exit 1

    PEAK_RSS=0
    TOTAL_START=$(now_ns)
    ITER=0
    while [ "$ITER" -lt "$V" ]; do
        NEXT=$((ITER + 1))
        ITER_START=$(now_ns)

        rm -f "$WORK"/chunk_* "$WORK"/map_* "$WORK"/shuffled.* "$WORK"/counter.* "$WORK"/part.*
        split -d -a 2 -n l/$NTASKS "$WORK/state.$ITER" "$WORK/chunk_"

        STAGE_START=$(now_ns)
        run_map_stage
        STAGE_END=$(now_ns)
        MAP_TIME=$(elapsed "$STAGE_START" "$STAGE_END")

        # data-movement metrics: what the shuffle actually has to carry
        INTER_RECORDS=$(cat "$WORK"/map_*_part* | wc -l)
        INTER_BYTES=$(cat "$WORK"/map_*_part* | wc -c)

        # Shuffle: merge each partition across all mappers. The R merges are
        # independent of each other, so they run concurrently.
        STAGE_START=$(now_ns)
        R=0
        while [ "$R" -lt "$NRED" ]; do
            sort -m "$WORK"/map_*_part"$R" > "$WORK/shuffled.$R" &
            R=$((R + 1))
        done
        wait
        STAGE_END=$(now_ns)
        SHUFFLE_TIME=$(elapsed "$STAGE_START" "$STAGE_END")

        # Reduce: one process per partition, run concurrently inside the
        # allocation. These are not launched with srun -- the allocation is a
        # single node, and a second srun per iteration would cost more launch
        # overhead than the parallel reduce saves.
        STAGE_START=$(now_ns)
        rm -f "$WORK"/rss.*
        R=0
        while [ "$R" -lt "$NRED" ]; do
            if command -v /usr/bin/time > /dev/null 2>&1; then
                /usr/bin/time -f "%M" -o "$WORK/rss.$R" \
                    python3 reducer.py < "$WORK/shuffled.$R" \
                    > "$WORK/part.$R" 2> "$WORK/counter.$R" &
            else
                python3 reducer.py < "$WORK/shuffled.$R" \
                    > "$WORK/part.$R" 2> "$WORK/counter.$R" &
            fi
            R=$((R + 1))
        done
        wait
        STAGE_END=$(now_ns)
        REDUCE_TIME=$(elapsed "$STAGE_START" "$STAGE_END")

        cat "$WORK"/part.* > "$WORK/state.$NEXT"
        for RSSF in "$WORK"/rss.*; do
            [ -f "$RSSF" ] || continue
            RSS=$(tail -1 "$RSSF" 2>/dev/null)
            case "$RSS" in (*[!0-9]*|"") RSS=0 ;; esac
            [ "$RSS" -gt "$PEAK_RSS" ] && PEAK_RSS=$RSS
        done

        ITER_END=$(now_ns)
        ITER_TIME=$(elapsed "$ITER_START" "$ITER_END")

        UPDATED=$(cat "$WORK"/counter.* | sed -n 's/.*UPDATED,\([0-9]*\).*/\1/p' \
                  | awk '{ s += $1 } END { print s + 0 }')
        UPDATED=${UPDATED:-0}

        echo "${V},${E},${NTASKS},${NRED},${NEXT},${MAP_TIME},${SHUFFLE_TIME},${REDUCE_TIME},${ITER_TIME},${UPDATED},${INTER_RECORDS},${INTER_BYTES}" >> "$ITER_CSV"
        printf "    iter %-3s map=%-10s shuffle=%-10s reduce=%-10s updates=%s\n" \
            "$NEXT" "$MAP_TIME" "$SHUFFLE_TIME" "$REDUCE_TIME" "$UPDATED"

        ITER=$NEXT
        [ "$UPDATED" -eq 0 ] && break
    done
    TOTAL_END=$(now_ns)
    TOTAL_TIME=$(elapsed "$TOTAL_START" "$TOTAL_END")

    python3 format_output.py < "$WORK/state.$ITER" > "$WORK/mr_out.txt"

    SEQ_START=$(now_ns)
    python3 dijkstra_ref.py < "$GRAPH" > "$WORK/ref_out.txt"
    SEQ_END=$(now_ns)
    SEQ_TIME=$(elapsed "$SEQ_START" "$SEQ_END")

    if diff -q "$WORK/mr_out.txt" "$WORK/ref_out.txt" > /dev/null; then
        CORRECT=YES
    else
        CORRECT=NO
    fi

    echo "    => iterations=$ITER total=${TOTAL_TIME}s dijkstra=${SEQ_TIME}s correct=$CORRECT"
    echo
    echo "${V},${E},${NTASKS},${NRED},${ITER},${TOTAL_TIME},${SEQ_TIME},${PEAK_RSS},${CORRECT}" >> "$SUM_CSV"
done

rm -rf "$WORK"

echo "============================================"
echo "Benchmark sweep complete (map tasks=$NTASKS, reducers=$NRED)"
echo "Per-iteration CSV: $ITER_CSV"
echo "Summary CSV:       $SUM_CSV"
echo "Launch overhead:   $CAL_CSV"
echo "============================================"
echo
echo "--- Summary ---"
if command -v column > /dev/null 2>&1; then column -t -s',' "$SUM_CSV"; else cat "$SUM_CSV"; fi
