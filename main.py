
import threading
import time
import random
import sqlite3
import socket
import json
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from flask import Flask, jsonify, request
from datetime import datetime

# ============================================================
# DATABASE SETUP (SQLite)
# ============================================================

DB_FILE = "bee_network.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS packet_logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      INTEGER,
            destination INTEGER,
            path        TEXT,
            status      TEXT,
            timestamp   TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS node_events (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id   INTEGER,
            event     TEXT,
            timestamp TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS queen_history (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            queen_id  INTEGER,
            reason    TEXT,
            timestamp TEXT
        )
    ''')

    conn.commit()
    conn.close()
    print("[DB] Database initialized.")


def log_packet(source, destination, path, status):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT INTO packet_logs (source, destination, path, status, timestamp) VALUES (?,?,?,?,?)",
        (source, destination, str(path), status, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def log_event(node_id, event):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT INTO node_events (node_id, event, timestamp) VALUES (?,?,?)",
        (node_id, event, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def log_queen(queen_id, reason):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT INTO queen_history (queen_id, reason, timestamp) VALUES (?,?,?)",
        (queen_id, reason, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


# ============================================================
# BEE NODE CLASS
# ============================================================

class BeeNode:
    def __init__(self, node_id):
        self.node_id  = node_id
        self.load     = 0
        self.is_queen = False
        self.status   = "normal"
        self.neighbors = []
        self.lock     = threading.Lock()
        self.route_scores = {}

    def run(self):
        while True:
            if self.status == "failed":
                time.sleep(2)
                continue

            with self.lock:
                self.load += random.randint(0, 2)
                if self.load > 10:
                    print(f"[Node {self.node_id}] Cooling down...")
                    self.load = 0

            queen_marker = " [QUEEN]" if self.is_queen else ""
            print(f"[Node {self.node_id}] Load: {self.load} | Status: {self.status}{queen_marker}")
            time.sleep(2)

    def send_packet(self, packet, nodes):
        current_id = self.node_id

        while True:
            current_node = nodes[current_id]

            if current_node.status == "failed":
                print(f"[Node {current_id}] Failed. Packet dropped.")
                log_packet(packet["source"], packet["destination"], packet["visited"], "dropped")
                return "dropped"

            packet["visited"].append(current_id)

            with current_node.lock:
                current_node.load += 1

            if current_id == packet["destination"]:
                print(f"[Node {current_id}] Packet DELIVERED!")
                for i in range(len(packet["visited"]) - 1):
                    src = packet["visited"][i]
                    nxt = packet["visited"][i + 1]
                    nodes[src].route_scores[nxt] = nodes[src].route_scores.get(nxt, 1) + 1
                log_packet(packet["source"], packet["destination"], packet["visited"], "delivered")
                print(f"Path rewarded: {packet['visited']}")
                return "delivered"

            best_neighbor = None
            best_value    = float('-inf')

            for neighbor in current_node.neighbors:
                if neighbor not in packet["visited"] and nodes[neighbor].status != "failed":
                    score        = current_node.route_scores.get(neighbor, 1)
                    load_penalty = nodes[neighbor].load
                    value        = score - load_penalty
                    if value > best_value:
                        best_value    = value
                        best_neighbor = neighbor

            if best_neighbor is not None:
                print(f"[Node {current_id}] Forwarding to Node {best_neighbor}")
                time.sleep(0.3)
                current_id = best_neighbor
            else:
                print("[Router] No available path. Packet dropped.")
                log_packet(packet["source"], packet["destination"], packet["visited"], "dropped")
                return "dropped"


# ============================================================
# NETWORK SETUP
# ============================================================

def create_network():
    G = nx.Graph()
    for i in range(1, 7):
        G.add_node(i)
    edges = [
        (1, 2), (1, 3),
        (2, 3), (2, 4),
        (3, 5),
        (4, 5), (4, 6),
        (5, 6)
    ]
    G.add_edges_from(edges)
    return G


def visualize_network(G, highlight_queen=None, failed_nodes=None, filename="network.png"):
    plt.figure(figsize=(8, 6))
    pos = nx.spring_layout(G, seed=42)
    node_colors = []
    for node in G.nodes():
        if failed_nodes and node in failed_nodes:
            node_colors.append("red")
        elif node == highlight_queen:
            node_colors.append("gold")
        else:
            node_colors.append("skyblue")
    nx.draw(G, pos, with_labels=True, node_color=node_colors,
            node_size=800, font_weight='bold', edge_color='gray')
    plt.title("Bee Network Topology  [ Queen=Gold  Failed=Red  Normal=Blue ]")
    plt.savefig(filename)
    plt.close()
    print(f"[VIZ] Saved: {filename}")


# ============================================================
# QUEEN ELECTION
# ============================================================

queen_id     = None
queen_lock   = threading.Lock()
failed_nodes = set()

def elect_queen(nodes, reason="initial"):
    global queen_id
    with queen_lock:
        for node in nodes.values():
            node.is_queen = False
        active = [n for n in nodes.values() if n.status != "failed"]
        if not active:
            print("[Election] No active nodes!")
            return None
        queen_node = min(active, key=lambda n: n.load)
        queen_node.is_queen = True
        queen_id = queen_node.node_id
        print(f"[Election] Node {queen_id} elected as Queen (reason: {reason})")
        log_queen(queen_id, reason)
        log_event(queen_id, "elected_queen")
        return queen_id


def simulate_queen_failure(nodes):
    global queen_id, failed_nodes
    if queen_id is None:
        return None
    old_queen = queen_id
    print(f"[Failure] Queen Node {old_queen} FAILED!")
    nodes[old_queen].status   = "failed"
    nodes[old_queen].is_queen = False
    failed_nodes.add(old_queen)
    log_event(old_queen, "queen_failed")
    new_queen = elect_queen(nodes, reason="queen_failed")
    visualize_network(graph, highlight_queen=new_queen,
                      failed_nodes=failed_nodes,
                      filename="network_after_queen_failure.png")
    return new_queen


def crash_node(nodes, node_id):
    global failed_nodes
    nodes[node_id].status = "failed"
    failed_nodes.add(node_id)
    print(f"[Failure] Node {node_id} CRASHED!")
    log_event(node_id, "crashed")
    if nodes[node_id].is_queen:
        nodes[node_id].is_queen = False
        elect_queen(nodes, reason="queen_crashed")
    visualize_network(graph, highlight_queen=queen_id,
                      failed_nodes=failed_nodes,
                      filename="network_after_crash.png")


# ============================================================
# SOCKET SERVER
# ============================================================

def start_socket_server(node, nodes):
    port = 5100 + node.node_id
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(5)
    print(f"[Socket] Node {node.node_id} listening on port {port}")
    while True:
        try:
            conn, _ = server.accept()
            data = conn.recv(4096).decode()
            if data:
                packet = json.loads(data)
                print(f"[Socket] Node {node.node_id} received packet via socket")
                result = node.send_packet(packet, nodes)
                conn.sendall(result.encode())
            conn.close()
        except Exception as e:
            print(f"[Socket] Node {node.node_id} error: {e}")


def send_via_socket(source_node_id, packet):
    port = 5100 + source_node_id
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port))
        s.sendall(json.dumps(packet).encode())
        result = s.recv(1024).decode()
        s.close()
        return result
    except Exception as e:
        return f"error: {e}"


# ============================================================
# FLASK REST API
# ============================================================

app   = Flask(__name__)
nodes = {}
graph = None


@app.route("/status", methods=["GET"])
def get_status():
    result = {}
    for nid, node in nodes.items():
        result[nid] = {
            "node_id":      node.node_id,
            "load":         node.load,
            "status":       node.status,
            "is_queen":     node.is_queen,
            "neighbors":    node.neighbors,
            "route_scores": node.route_scores
        }
    return jsonify(result)


@app.route("/queen", methods=["GET"])
def get_queen():
    if queen_id and nodes[queen_id].status != "failed":
        return jsonify({"queen_id": queen_id, "load": nodes[queen_id].load})
    return jsonify({"queen_id": None, "message": "No active queen"})


@app.route("/send_packet", methods=["POST"])
def api_send_packet():
    body = request.json
    src  = body.get("source", 1)
    dst  = body.get("destination", 6)
    data = body.get("data", "test")

    if src not in nodes or dst not in nodes:
        return jsonify({"error": "Invalid source or destination"}), 400
    if nodes[src].status == "failed":
        return jsonify({"error": f"Source node {src} is failed"}), 400

    packet = {"source": src, "destination": dst, "data": data, "priority": "normal", "visited": []}
    result = send_via_socket(src, packet)
    return jsonify({"status": result, "source": src, "destination": dst})


@app.route("/fail_node/<int:node_id>", methods=["POST"])
def api_fail_node(node_id):
    if node_id not in nodes:
        return jsonify({"error": "Node not found"}), 404
    if nodes[node_id].status == "failed":
        return jsonify({"message": f"Node {node_id} already failed"})
    crash_node(nodes, node_id)
    return jsonify({"message": f"Node {node_id} crashed", "new_queen": queen_id})


@app.route("/fail_queen", methods=["POST"])
def api_fail_queen():
    new_queen = simulate_queen_failure(nodes)
    return jsonify({"message": "Queen failed", "new_queen": new_queen})


@app.route("/elect_queen", methods=["POST"])
def api_elect_queen():
    new_queen = elect_queen(nodes, reason="manual_trigger")
    return jsonify({"new_queen": new_queen})


@app.route("/logs/packets", methods=["GET"])
def get_packet_logs():
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM packet_logs ORDER BY id DESC LIMIT 50")
    rows = c.fetchall()
    conn.close()
    return jsonify([
        {"id": r[0], "source": r[1], "destination": r[2],
         "path": r[3], "status": r[4], "timestamp": r[5]}
        for r in rows
    ])


@app.route("/logs/events", methods=["GET"])
def get_event_logs():
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM node_events ORDER BY id DESC LIMIT 50")
    rows = c.fetchall()
    conn.close()
    return jsonify([
        {"id": r[0], "node_id": r[1], "event": r[2], "timestamp": r[3]}
        for r in rows
    ])


@app.route("/logs/queens", methods=["GET"])
def get_queen_logs():
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM queen_history ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    return jsonify([
        {"id": r[0], "queen_id": r[1], "reason": r[2], "timestamp": r[3]}
        for r in rows
    ])


@app.route("/topology", methods=["GET"])
def get_topology():
    return jsonify({
        "nodes": list(graph.nodes()),
        "edges": list(graph.edges())
    })


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    init_db()

    graph = create_network()
    for node_id in graph.nodes():
        nodes[node_id] = BeeNode(node_id)

    for node_id in graph.nodes():
        nodes[node_id].neighbors = list(graph.neighbors(node_id))
        for neighbor in nodes[node_id].neighbors:
            nodes[node_id].route_scores[neighbor] = 1

    elect_queen(nodes, reason="startup")
    visualize_network(graph, highlight_queen=queen_id, filename="network_initial.png")

    # Start node behavior threads
    for node in nodes.values():
        t = threading.Thread(target=node.run)
        t.daemon = True
        t.start()

    # Start socket servers
    for node in nodes.values():
        t = threading.Thread(target=start_socket_server, args=(node, nodes))
        t.daemon = True
        t.start()

    time.sleep(1)

    # Test packet via socket
    print("\n[TEST] Sending test packet Node 1 -> Node 6 via socket...")
    packet = {"source": 1, "destination": 6, "data": "Hello Bee", "priority": "normal", "visited": []}
    result = send_via_socket(1, packet)
    print(f"[TEST] Result: {result}")

    print("\n[API] Flask API running on http://127.0.0.1:5000")
    print("""
  Endpoints:
  GET  /status              - All node statuses
  GET  /queen               - Current queen
  POST /send_packet         - Route a packet (body: source, destination, data)
  POST /fail_node/<id>      - Crash a node
  POST /fail_queen          - Crash the queen
  POST /elect_queen         - Force re-election
  GET  /logs/packets        - Packet history (DB)
  GET  /logs/events         - Node events (DB)
  GET  /logs/queens         - Queen history (DB)
  GET  /topology            - Network graph
    """)

    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
