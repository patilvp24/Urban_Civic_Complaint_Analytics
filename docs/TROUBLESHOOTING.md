# Urban Civic Analytics — System Troubleshooting & Operations Guide

This guide details common operational issues, diagnostics, and recovery procedures for the Hadoop 3.3.6 and WSL2 architecture.

---

## 1. Hadoop Daemons Management in WSL2

### Checking Active Daemons
In Ubuntu-22.04 WSL2, verify running Java daemons using `jps`:
```bash
jps
```
**Expected Output:**
- `NameNode` (HDFS Master)
- `DataNode` (HDFS Worker)
- `SecondaryNameNode` (Checkpointing)
- `ResourceManager` (YARN Master)
- `NodeManager` (YARN Worker)

### Starting Services Safely
If services are stopped upon Windows reboot:
```bash
# 1. Start HDFS daemons
/home/veetrag24/hadoop/sbin/start-dfs.sh

# 2. Start YARN daemons with session detachment (prevents SIGHUP on shell exit)
setsid /home/veetrag24/hadoop/bin/yarn resourcemanager </dev/null >/tmp/yarn-rm.log 2>&1 &
/home/veetrag24/hadoop/sbin/start-yarn.sh
```

### Stopping Services Cleanly
```bash
/home/veetrag24/hadoop/sbin/stop-yarn.sh
/home/veetrag24/hadoop/sbin/stop-dfs.sh
```

---

## 2. Common Errors and Solutions

### Issue A: `ConnectionRefused` on `localhost:9000` or `localhost:8032`
- **Root Cause:** NameNode (port 9000) or ResourceManager (port 8032) is not currently running.
- **Fix:**
  ```bash
  # Check ports:
  ss -tlpn | grep -E '9000|8032'
  
  # If 9000 is missing, start DFS:
  /home/veetrag24/hadoop/sbin/start-dfs.sh
  
  # If 8032 is missing, start ResourceManager:
  setsid /home/veetrag24/hadoop/bin/yarn resourcemanager </dev/null >/tmp/yarn-rm.log 2>&1 &
  ```

### Issue B: NameNode Stuck in Safe Mode
- **Symptom:** `Cannot create directory ... Name node is in safe mode.`
- **Root Cause:** HDFS is waiting for DataNode block reports after startup.
- **Fix:** Manually command HDFS to leave safe mode:
  ```bash
  /home/veetrag24/hadoop/bin/hdfs dfsadmin -safemode leave
  ```

### Issue C: WSL2 Shuts Down When Terminal is Closed
- **Root Cause:** By default, WSL2 closes its virtual machine instance when no active shell processes remain.
- **Fix:** A background keep-alive task maintains the VM instance. To run a standalone keepalive:
  ```bash
  nohup sleep infinity >/dev/null 2>&1 &
  ```

### Issue D: Streamlit Port 8501 Already in Use
- **Symptom:** `Port 8501 is already in use.`
- **Fix:** Specify an alternative port:
  ```powershell
  .\.venv\Scripts\streamlit run dashboard/app.py --server.port 8502
  ```
  Or terminate previous instances:
  ```powershell
  Get-Process -Name python* | Where-Object { $_.CommandLine -like "*streamlit*" } | Stop-Process
  ```

---

## 3. Web UI Ports Reference
| Service | URL | Description |
| :--- | :--- | :--- |
| **Streamlit Web Dashboard** | `http://localhost:8501` | Main interactive analytics interface |
| **HDFS NameNode UI** | `http://localhost:9870` | Browse HDFS blocks and file system |
| **YARN ResourceManager UI** | `http://localhost:8088` | Monitor MapReduce job execution and containers |
| **DataNode Web UI** | `http://localhost:9864` | DataNode status and block storage |
