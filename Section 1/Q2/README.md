# Section 1 / Q2 — Single Source Shortest Path (SSSP) with MapReduce

Finds the shortest distance from node 0 to every node in a weighted directed
graph, using iterative MapReduce (parallel Bellman-Ford). Written in Python,
orchestrated with Bash. Works both on a laptop and on a Slurm cluster.

---

## Files

| File | What it does |
|---|---|
| `prep.py` | Turns the raw edge list into the starting node state |
| `mapper.py` | One relaxation step: re-emits the graph + proposes new distances |
| `combiner.py` | Merges duplicate proposals locally before the shuffle (optimisation) |
| `reducer.py` | Picks the minimum distance per node, counts how many changed |
| `format_output.py` | Prints the final `node distance` lines, `INF` if unreachable |
| `sssp_local.sh` | Runs the whole thing on one machine |
| `sssp_slurm.sh` | Runs the whole thing on a Slurm cluster |
| `gen_graph.py` | Makes random test graphs (same seed = same graph) |
| `dijkstra_ref.py` | Ordinary sequential Dijkstra, used to check our answers |
| `verify.sh` | Runs all tests and compares against Dijkstra |
| `tests/` | Small hand-made graphs (sample from the PDF, chain, cycle, etc.) |

---

## Input / output format

Input file:
```
V E          <- number of vertices, number of edges
u v w        <- one line per edge: from u, to v, weight w
...
```

Output:
```
0 0
1 3
2 2
3 7          <- "node_id distance", sorted by node, INF if unreachable
```

---

## Running locally (your own computer)

You only need `python3` and `bash`. No Hadoop, no cluster.

**Step 1 — go to this folder**
```bash
cd "Section 1"
```

**Step 2 — run on the sample from the assignment PDF**
```bash
./sssp_local.sh tests/sample.txt output.txt
cat output.txt
```
You should see:
```
0 0
1 3
2 2
3 7
```

**Step 3 — run on a bigger graph**

First create one (`gen_graph.py V E seed`), then run on it:
```bash
python3 gen_graph.py 10000 50000 7 > big.txt
./sssp_local.sh big.txt output.txt 4
```
The last number (`4`) is how many map tasks to simulate. Any number works;
the answer is always the same. To use your own graph file, just put its
name in place of `big.txt`.

**Step 4 — check correctness against Dijkstra**
```bash
./verify.sh
```
Prints `PASS`/`FAIL` for each test and ends with `ALL TESTS PASSED`.

**Making a big graph for timing**
```bash
python3 gen_graph.py 10000 50000 7 > big.txt     # V=10000, E=50000, seed=7
./sssp_local.sh big.txt big_out.txt 4
python3 dijkstra_ref.py < big.txt > big_ref.txt
diff big_out.txt big_ref.txt && echo MATCH
```

> If you get `Permission denied`, run `chmod +x *.sh *.py` once.

---

## Running on the Slurm cluster (RCE)

**Step 1 — copy this folder to the cluster**
```bash
scp -r "Section 1" <your-username>@<cluster-address>:~/
```

**Step 2 — log in and go to the folder**
```bash
ssh <your-username>@<cluster-address>
cd "Section 1"
chmod +x *.sh *.py
```

**Step 3 — make a graph to run on** (or upload your own)
```bash
python3 gen_graph.py 10000 50000 7 > big.txt
```

**Step 4 — submit the job**
```bash
sbatch sssp_slurm.sh big.txt big_out.txt
```
Slurm replies with something like `Submitted batch job 12345`.

**Step 5 — wait for it to finish**
```bash
squeue -u $USER          # your job is listed while it runs; empty when done
```

**Step 6 — look at the results**
```bash
cat sssp_results_12345.out                         # progress + timing table (use your job number)
cat big_out.txt                                    # the shortest distances
cat perf_results/sssp_iteration_timings.csv        # per-iteration timings for the report
```
If anything went wrong, the error is in `sssp_results_12345.err`.

**Step 7 — verify the cluster answer**
```bash
python3 dijkstra_ref.py < big.txt > big_ref.txt
diff big_out.txt big_ref.txt && echo MATCH
```

### Changing the number of nodes / tasks

Edit the top of `sssp_slurm.sh`:
```
#SBATCH --nodes=4
#SBATCH --ntasks=4
```
Set both to 1, 2, 4, 8, ... and re-submit to get scaling numbers for the report.
Or override without editing: `sbatch --nodes=2 --ntasks=2 sssp_slurm.sh big.txt out2.txt`

### Things that can go wrong on the cluster

- **`python3: command not found`** — some clusters need `module load python` first.
  Add that line near the top of `sssp_slurm.sh`, right after `cd "$SCRIPT_DIR"`.
- **Job stays `PD` (pending) forever** — the cluster is busy or you asked for more
  nodes than your partition allows. Try fewer nodes, or add `#SBATCH --partition=<name>`
  as given in the RCE document.
- **Files from other tasks missing** — the folder must be on the shared filesystem
  (your home directory is fine). Don't run it from `/tmp`.

---

## How it works (short version)

Each node's state is one line: `node \t dist|neighbour,weight;neighbour,weight;...`

Each iteration is one MapReduce job:

1. **Mapper** — for every node it re-emits the node's own line (so the graph
   survives to the next round) and, if the node is reachable, emits
   `neighbour \t D|dist+weight` for every out-edge. That is one Bellman-Ford
   relaxation.
2. **Shuffle** (`sort`) — groups everything for the same node together.
3. **Combiner** — on each mapper's output, collapses repeated proposals for the
   same node into one minimum, so less data crosses the network.
4. **Reducer** — for each node takes the minimum of its old distance and all
   proposals, writes the new line, and counts how many nodes improved.
5. The driver script repeats until the reducer reports **0 improvements** —
   at that point no edge can be relaxed further, so the distances are final.

Unreachable nodes never receive a proposal, stay at `1000000`, and are printed
as `INF`. Converges in about the graph's hop-diameter rounds (22 rounds for
V=10000, E=50000), never more than V.

## Correctness

`verify.sh` compares the MapReduce output byte-for-byte with `dijkstra_ref.py`
on: the assignment sample, a chain, a cycle, an edgeless graph, a graph with
an unreachable component, and 3 random 150-node graphs — each with 1, 2 and 3
map tasks, confirming the result does not depend on how the input is split.
A V=10000 / E=50000 random graph also matches exactly.
