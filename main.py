import traci
import random
import math
import glob

# -----------------------------
# CONFIG
# -----------------------------
RSU_RANGE = 300
V2V_RANGE = 120

RSUS = {
    "RSU1": (500, 500),
    "RSU2": (1000, 500),
    "RSU3": (500, 1000),
    "RSU4": (1000, 1000)
}

RSU_QUEUES = {r: [] for r in RSUS.keys()}

# slower RSU → congestion builds
RSU_PROCESS_RATE = 0.3

EDGE_SERVER = "EDGE"

# -----------------------------
# METRICS
# -----------------------------
STATS = {
    "total_tasks": 0,
    "executed_local": 0,
    "executed_v2v": 0,
    "executed_rsu": 0,
    "offloaded_edge": 0,
    "mdcr_values": []
}

# -----------------------------
# AUTO FIND SUMO CONFIG
# -----------------------------
def find_sumocfg():
    files = glob.glob("*.sumocfg")
    if not files:
        raise FileNotFoundError("No .sumocfg file found.")
    return files[0]

# -----------------------------
# UTILS
# -----------------------------
def distance(p1, p2):
    return math.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def get_nearest_rsu(pos):
    rsu_id, dmin = None, float("inf")
    for r, rpos in RSUS.items():
        d = distance(pos, rpos)
        if d < dmin:
            rsu_id, dmin = r, d
    return rsu_id if dmin <= RSU_RANGE else None

def get_neighbor_vehicle(veh_id, vehicles):

    vpos = traci.vehicle.getPosition(veh_id)

    neighbors = []

    for v in vehicles:
        if v == veh_id:
            continue

        if distance(vpos, traci.vehicle.getPosition(v)) <= V2V_RANGE:
            neighbors.append(v)

    if neighbors:
        return random.choice(neighbors)

    return None

# -----------------------------
# MOBILITY WINDOW
# -----------------------------
def mobility_window(veh_id, rsu_id):

    pos = traci.vehicle.getPosition(veh_id)
    speed = traci.vehicle.getSpeed(veh_id)

    if speed < 0.1:
        speed = random.uniform(5, 15)

    d = distance(pos, RSUS[rsu_id])
    remaining = max(0, RSU_RANGE - d)

    mw = remaining / speed

    return round(mw, 2)

# -----------------------------
# TASK GENERATION
# -----------------------------
def generate_task():

    return {
        "task_id": random.randint(1000, 9999),
        "type": random.choice(["INDEPENDENT", "DEPENDENT"])
    }

def create_dag(task_id):

    dag = [
        {"id": f"{task_id}_T1", "data": 1, "compute": random.randint(1,2)},
        {"id": f"{task_id}_T2", "data": 4, "compute": random.randint(3,5)},
        {"id": f"{task_id}_T3", "data": 2, "compute": random.randint(2,4)}
    ]

    for t in dag:
        t["critical"] = ("T2" in t["id"])

    return dag

# -----------------------------
# EXECUTION
# -----------------------------
def execute_locally(veh_id, task):

    STATS["executed_local"] += 1

    print(f"{veh_id}: EXECUTING {task['id']} locally")

def execute_independent_task(veh_id, task):

    STATS["executed_local"] += 1

    print(f"{veh_id}: Independent task executed locally")

def offload_to_vehicle(src, dst, task):

    STATS["executed_v2v"] += 1

    print(f"{src}: OFFLOADING {task['id']} → VEHICLE {dst}")

# -----------------------------
# ADD TASK TO RSU
# -----------------------------
def add_to_rsu_queue(veh_id, rsu_id, task):

    task["veh_id"] = veh_id
    task["mobility_window"] = mobility_window(veh_id, rsu_id)

    # smaller deadlines so MDCR rises
    task["deadline"] = random.randint(2, 6)

    RSU_QUEUES[rsu_id].append(task)

    print(f"{veh_id}: OFFLOADING {task['id']} → {rsu_id} "
          f"| MW={task['mobility_window']} "
          f"| Deadline={task['deadline']}")

# -----------------------------
# MDCR OFFLOADING
# -----------------------------
def mdcr_offloading(task, rsu_id):

    queue_len = len(RSU_QUEUES[rsu_id])
    mu = RSU_PROCESS_RATE

    waiting_time = queue_len / mu

    safe_time = min(task["deadline"], task["mobility_window"])

    mdcr = waiting_time / safe_time if safe_time > 0 else 999

    STATS["mdcr_values"].append(mdcr)

    print(f"{rsu_id}: MDCR CHECK → Waiting={round(waiting_time,2)} "
          f"| SafeTime={round(safe_time,2)} "
          f"| MDCR={round(mdcr,2)}")

    if mdcr >= 1:

        print(f"{rsu_id}: OFFLOADING {task['id']} → EDGE SERVER")

        STATS["offloaded_edge"] += 1

        return False

    return True

# -----------------------------
# RSU SCHEDULING
# -----------------------------
def schedule_rsu_tasks():

    for rsu_id, queue in RSU_QUEUES.items():

        if not queue:
            continue

        print(f"\n{rsu_id}: Applying MW-EDF-CPS Scheduling")

        queue.sort(key=lambda t: (
            not t["critical"],
            t["mobility_window"],
            t["deadline"]
        ))

        # only one task executed per step
        task = queue[0]

        execute = mdcr_offloading(task, rsu_id)

        if not execute:
            queue.pop(0)
            continue

        task = queue.pop(0)

        STATS["executed_rsu"] += 1

        print(f"{rsu_id}: EXECUTING {task['id']} "
              f"| Vehicle={task['veh_id']}")

# -----------------------------
# PARTITIONING LOGIC
# -----------------------------
def partition_subtask(veh_id, vehicles, subtask):

    pos = traci.vehicle.getPosition(veh_id)

    rsu = get_nearest_rsu(pos)

    neighbor = get_neighbor_vehicle(veh_id, vehicles)

    STATS["total_tasks"] += 1

    compute = subtask["compute"]

    # heavy tasks → RSU
    if compute >= 4 and rsu:
        return ("RSU", rsu)

    # medium tasks → neighbor
    if compute == 3 and neighbor:
        return ("V2V", neighbor)

    # moderate tasks → RSU
    if compute == 2 and rsu:
        return ("RSU", rsu)

    # small tasks → OBU
    return ("OBU", None)

# -----------------------------
# MAIN LOOP
# -----------------------------
def run():

    cfg = find_sumocfg()

    traci.start(["sumo-gui", "-c", cfg])

    processed = set()

    while traci.simulation.getMinExpectedNumber() > 0:

        traci.simulationStep()

        vehicles = traci.vehicle.getIDList()

        for veh_id in vehicles:

            if veh_id in processed:
                continue

            processed.add(veh_id)

            task = generate_task()

            if task["type"] == "INDEPENDENT":
                execute_independent_task(veh_id, task)
                continue

            dag = create_dag(task["task_id"])

            print(f"\n{veh_id}: DAG TASK {task['task_id']}")

            for subtask in dag:

                decision, target = partition_subtask(
                    veh_id, vehicles, subtask
                )

                if decision == "RSU":
                    add_to_rsu_queue(veh_id, target, subtask)

                elif decision == "V2V":
                    offload_to_vehicle(veh_id, target, subtask)

                else:
                    execute_locally(veh_id, subtask)

        schedule_rsu_tasks()

    traci.close()

    # -----------------------------
    # FINAL RESULTS
    # -----------------------------
    print("\n==============================")
    print("PROJECT PERFORMANCE SUMMARY")
    print("==============================")

    total = STATS["total_tasks"]

    print("Total Tasks:", total)
    print("OBU Execution:", STATS["executed_local"])
    print("V2V Execution:", STATS["executed_v2v"])
    print("RSU Execution:", STATS["executed_rsu"])
    print("Edge Offloading:", STATS["offloaded_edge"])

# -----------------------------
if __name__ == "__main__":
    run()