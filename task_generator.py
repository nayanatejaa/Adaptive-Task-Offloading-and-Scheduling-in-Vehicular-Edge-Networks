# task_generator.py
# Generates independent and dependent tasks (DAGs) and logs task definitions + events.
# Usage: import TaskGenerator and call on simulation steps to create tasks tied to vehicles.

import os
import json
import csv
import time
import random
import uuid
from typing import Dict, Any, List, Tuple

try:
    import networkx as nx
except Exception:
    nx = None
    # networkx is only required to create and export DAG structure nicely.
    # If not present, a simple adjacency list is used.

# ---------- Configurable defaults ----------
OUT_TASK_DEF = "task_definitions.json"   # stores DAGs and task metadata
TASK_EVENTS_CSV = "task_events.csv"      # logs assigned tasks and runtime events

# probabilities / rates
DEFAULT_INDEPENDENT_RATE = 0.02   # per vehicle per simulation second (probability)
DEFAULT_DEPENDENT_RATE = 0.005    # per vehicle per simulation second (probability)

# DAG defaults
DEPENDENT_DAG_MIN_NODES = 4
DEPENDENT_DAG_MAX_NODES = 12
DAG_EDGE_PROB = 0.25   # probability for an edge between earlier->later node (for random DAG)

# task size ranges (bytes) and CPU cycles (abstract units)
INDEPENDENT_SIZE_BYTES = (50_000, 500_000)        # small tasks (50KB - 500KB)
INDEPENDENT_CPU = (1e6, 1e8)                      # CPU cycles / cost (abstract)
SUBTASK_SIZE_BYTES = (200_000, 5_000_000)         # bytes for subtask outputs if any
SUBTASK_CPU = (1e7, 5e8)

# deadlines (seconds) relative to generation time
CRITICAL_DEADLINE_RANGE = (1.0, 10.0)    # tight deadlines in seconds
NONCRITICAL_DEADLINE_RANGE = (20.0, 200.0)

# portion of critical tasks
CRITICAL_PROB = 0.3

# ---------------- helper utils ----------------

def now_s():
    return time.time()

def rand_bytes(range_tuple):
    return int(random.uniform(range_tuple[0], range_tuple[1]))

def rand_cpu(range_tuple):
    return float(random.uniform(range_tuple[0], range_tuple[1]))

def rand_bool(p):
    return random.random() < p

def unique_id(prefix="task"):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

# ---------------- DAG generator ----------------

def make_random_dag(n_nodes:int,
                    edge_prob:float = DAG_EDGE_PROB,
                    node_cpu_range:Tuple[float,float] = SUBTASK_CPU,
                    edge_comm_range:Tuple[int,int] = SUBTASK_SIZE_BYTES) -> Dict[str,Any]:
    """
    Create a random DAG with n_nodes nodes, topologically ordered,
    return a dictionary with nodes list and edges list and node/edge attributes.
    """
    if nx is None:
        # Fallback simple DAG generator without networkx
        nodes = [f"n{i}" for i in range(n_nodes)]
        edges = []
        node_attrs = {}
        edge_attrs = {}
        for i, n in enumerate(nodes):
            node_attrs[n] = {"cpu": rand_cpu(node_cpu_range)}
            for j in range(i+1, n_nodes):
                if random.random() < edge_prob:
                    edges.append((n, nodes[j]))
                    edge_attrs[f"{n}->{nodes[j]}"] = {"comm_bytes": rand_bytes(edge_comm_range)}
        return {"nodes": nodes, "node_attrs": node_attrs, "edges": edges, "edge_attrs": edge_attrs}

    # networkx approach
    G = nx.DiGraph()
    # create nodes in topological order
    nodes = [f"n{i}" for i in range(n_nodes)]
    for n in nodes:
        G.add_node(n, cpu=rand_cpu(node_cpu_range))
    # allow edges only from lower index -> higher index (ensures acyclic)
    for i in range(n_nodes):
        for j in range(i+1, n_nodes):
            if random.random() < edge_prob:
                comm = rand_bytes(edge_comm_range)
                G.add_edge(nodes[i], nodes[j], comm_bytes=comm)
    # compress structure
    node_attrs = {n: dict(G.nodes[n]) for n in G.nodes()}
    edge_list = [(u,v,dict(G.edges[u,v])) for u,v in G.edges()]
    # return serializable structure
    return {"nodes": nodes, "node_attrs": node_attrs, "edges": [(u,v) for u,v,_ in edge_list],
            "edge_attrs": {f"{u}->{v}": attrs for u,v,attrs in edge_list}}

# ---------------- TaskGenerator class ----------------

class TaskGenerator:
    """
    Task generator that can be polled from your simulation loop to create tasks.
    It writes task definitions (DAGs + metadata) to JSON and logs assignment events to CSV.
    """
    def __init__(self,
                 out_task_def: str = OUT_TASK_DEF,
                 task_events_csv: str = TASK_EVENTS_CSV,
                 indep_rate: float = DEFAULT_INDEPENDENT_RATE,
                 dep_rate: float = DEFAULT_DEPENDENT_RATE,
                 critical_prob: float = CRITICAL_PROB):
        self.out_task_def = out_task_def
        self.task_events_csv = task_events_csv
        self.indep_rate = indep_rate
        self.dep_rate = dep_rate
        self.critical_prob = critical_prob
        # store generated tasks in memory
        self.task_defs = {}   # task_id -> dict
        # open events CSV (append mode)
        self._ensure_outputs()
        self._open_event_csv()

    def _ensure_outputs(self):
        if not os.path.exists(self.out_task_def):
            with open(self.out_task_def, "w", encoding="utf-8") as f:
                json.dump({}, f)
        if not os.path.exists(self.task_events_csv):
            with open(self.task_events_csv, "w", newline='', encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["time", "event", "task_id", "veh_id", "info"])

    def _open_event_csv(self):
        self.event_file = open(self.task_events_csv, "a", newline='', encoding="utf-8")
        self.event_writer = csv.writer(self.event_file)

    def close(self):
        try:
            self.event_file.close()
        except Exception:
            pass

    def _dump_defs(self):
        # write all task defs to JSON (overwrite)
        with open(self.out_task_def, "w", encoding="utf-8") as f:
            json.dump(self.task_defs, f, indent=2)

    def _log_event(self, sim_time, event, task_id, veh_id, info=None):
        row = [sim_time, event, task_id, veh_id, json.dumps(info) if info is not None else ""]
        self.event_writer.writerow(row)
        self.event_file.flush()

    # ----- generation helpers -----
    def generate_independent_task(self, sim_time: float, veh_id: str) -> Dict[str,Any]:
        tid = unique_id("ind")
        size = rand_bytes(INDEPENDENT_SIZE_BYTES)
        cpu_cost = rand_cpu(INDEPENDENT_CPU)
        critical = rand_bool(self.critical_prob)
        if critical:
            deadline = sim_time + random.uniform(*CRITICAL_DEADLINE_RANGE)
        else:
            deadline = sim_time + random.uniform(*NONCRITICAL_DEADLINE_RANGE)
        task = {
            "task_id": tid,
            "type": "independent",
            "created_time": sim_time,
            "vehicle": veh_id,
            "size_bytes": size,
            "cpu_cost": cpu_cost,
            "critical": bool(critical),
            "deadline": deadline
        }
        self.task_defs[tid] = task
        self._dump_defs()
        self._log_event(sim_time, "generated", tid, veh_id, {"kind":"independent", "size":size, "cpu":cpu_cost, "deadline":deadline})
        return task

    def generate_dependent_task(self, sim_time: float, veh_id: str,
                                n_nodes_min:int=DEPENDENT_DAG_MIN_NODES,
                                n_nodes_max:int=DEPENDENT_DAG_MAX_NODES) -> Dict[str,Any]:
        tid = unique_id("dep")
        n_nodes = random.randint(n_nodes_min, n_nodes_max)
        dag = make_random_dag(n_nodes)
        # convert node attrs to a subtask list with ids
        subtasks = {}
        for node in dag["nodes"]:
            sid = unique_id("s")
            subtasks[sid] = {
                "node_name": node,
                "cpu_cost": dag["node_attrs"].get(node, {}).get("cpu", rand_cpu(SUBTASK_CPU)),
                # optional produced bytes to be sent to children
                "output_bytes": rand_bytes(SUBTASK_SIZE_BYTES)
            }
        edges = []
        for (u,v) in dag["edges"]:
            # find sids for u and v
            su = next(sid for sid,data in subtasks.items() if data["node_name"]==u)
            sv = next(sid for sid,data in subtasks.items() if data["node_name"]==v)
            comm = dag["edge_attrs"].get(f"{u}->{v}", {}).get("comm_bytes", rand_bytes(SUBTASK_SIZE_BYTES))
            edges.append({"from": su, "to": sv, "comm_bytes": comm})

        critical = rand_bool(self.critical_prob)
        if critical:
            deadline = sim_time + random.uniform(*CRITICAL_DEADLINE_RANGE) * n_nodes  # scale with size
        else:
            deadline = sim_time + random.uniform(*NONCRITICAL_DEADLINE_RANGE) * n_nodes

        task = {
            "task_id": tid,
            "type": "dependent",
            "created_time": sim_time,
            "vehicle": veh_id,
            "num_subtasks": n_nodes,
            "subtasks": subtasks,
            "edges": edges,
            "critical": bool(critical),
            "deadline": deadline
        }
        self.task_defs[tid] = task
        self._dump_defs()
        self._log_event(sim_time, "generated", tid, veh_id, {"kind":"dependent", "num_subtasks":n_nodes, "deadline":deadline})
        return task

    # ----- public poll function -----
    def poll_and_maybe_generate(self, sim_time: float, veh_id: str) -> List[Dict[str,Any]]:
        """
        Called from simulation loop per vehicle (or at some schedule) with current sim_time and vehicle id.
        Returns a list of generated tasks (possibly empty).
        """
        created = []
        # independent tasks: Bernoulli with p = indep_rate per second
        if rand_bool(self.indep_rate):
            created.append(self.generate_independent_task(sim_time, veh_id))
        # dependent tasks less frequently
        if rand_bool(self.dep_rate):
            created.append(self.generate_dependent_task(sim_time, veh_id))
        return created

    # helper to force create tasks (for experiments)
    def force_create_independent(self, sim_time, veh_id, size=None, cpu=None, critical=None, deadline=None):
        return self.generate_independent_task(sim_time, veh_id)

    def force_create_dependent(self, sim_time, veh_id, n_nodes=6):
        return self.generate_dependent_task(sim_time, veh_id, n_nodes_min=n_nodes, n_nodes_max=n_nodes)
