import json
import requests
import sys
from datetime import datetime, timedelta # Ensure timedelta is imported
import time

# --- Configuration and Authentication ---

def load_config():
    """Loads configuration from config.json"""
    try:
        with open('config.json') as f:
            return json.load(f)
    except FileNotFoundError:
        sys.exit("Error: config.json not found.")
    except json.JSONDecodeError:
        sys.exit("Error: config.json is not valid JSON.")
    except Exception as exc:
        sys.exit(f"Error loading config.json: {exc}")

def get_auth_token(client_id, client_secret, base_url):
    """Gets API authentication token from Rubrik."""
    try:
        url = f"{base_url}/api/client_token"
        response = requests.post(url, json={"client_id": client_id, "client_secret": client_secret}, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        if 'access_token' not in token_data:
             sys.exit(f"Authentication error: 'access_token' not found in response. Response: {token_data}")
        return token_data['access_token']
    except requests.exceptions.Timeout:
        sys.exit(f"Authentication request timed out connecting to {url}")
    except requests.HTTPError as exc: 
        response_text = "(Could not retrieve response body)"
        if exc.response is not None:
             try:
                  response_json = exc.response.json(); response_text = json.dumps(response_json, indent=2)
             except json.JSONDecodeError: response_text = exc.response.text
        sys.exit(f"Authentication HTTP error: {exc}\nResponse body:\n{response_text}")
    except requests.exceptions.RequestException as exc: 
        sys.exit(f"Authentication network error: {exc}")
    except Exception as exc:
         sys.exit(f"An unexpected error occurred during authentication: {exc}")

def graphql_query(token, base_url, query, variables=None):
    """Executes a GraphQL query against the Rubrik API."""
    headers = {"Authorization": f"Bearer {token}"}
    api_url = f"{base_url}/api/graphql"
    response = None 
    try:
        response = requests.post(api_url, json={"query": query, "variables": variables or {}}, headers=headers, timeout=60)
        response.raise_for_status()
        result = response.json()
        if 'errors' in result and result['errors']:
            error_messages = [err.get('message', 'Unknown GraphQL error') for err in result['errors']]
            error_details = [str(err.get('extensions', '')) for err in result['errors']]
            full_error_message = "\n- ".join([f"{msg} {det}".strip() for msg, det in zip(error_messages, error_details)])
            sys.exit(f"GraphQL API errors returned:\n- {full_error_message}")
        if 'data' not in result:
             print(f"Warning: No 'data' key in GraphQL response. Full response: {json.dumps(result, indent=2)}")
             return {}
        return result.get("data", {})
    except requests.HTTPError as exc:
        response_text = "(No response object available)"; response_status = "N/A"
        error_response_obj = exc.response if exc.response is not None else response
        if error_response_obj is not None:
             response_status = error_response_obj.status_code
             try: response_json = error_response_obj.json(); response_text = json.dumps(response_json, indent=2)
             except json.JSONDecodeError:
                 try: response_text = error_response_obj.text
                 except Exception: response_text = "(Failed to read response body)"
             except Exception as e: response_text = f"(Error processing response body: {e})"
        sys.exit(f"GraphQL HTTP error: {exc}\nResponse body (Status: {response_status}):\n{response_text}")
    except requests.exceptions.Timeout: sys.exit(f"GraphQL request timed out accessing {api_url}")
    except requests.exceptions.RequestException as exc: sys.exit(f"GraphQL network error (non-HTTP): {exc}")
    except json.JSONDecodeError as exc:
        status_code = "N/A"; resp_text = "(Response object not available)"
        if response is not None: status_code = response.status_code; resp_text = (response.text[:500] + '...') if len(response.text) > 500 else response.text
        sys.exit(f"GraphQL error: Could not decode JSON response. Status: {status_code}. Excerpt:\n{resp_text}\nOriginal: {exc}")
    except Exception as exc:
         import traceback; print("--- Unexpected Error Traceback ---"); traceback.print_exc(); print("--- End Traceback ---")
         sys.exit(f"An unexpected error during GraphQL query: {exc}")

# --- Hyper-V Specific Functions ---

def get_protected_connected_hyperv_vms(token, base_url):
    query = ''' query GetHyperVVms { hypervVirtualMachines(filter: [{field: IS_RELIC, texts: ["false"]}, {field: IS_REPLICATED, texts: ["false"]}]) { nodes { id name osType effectiveSlaDomain { id name } agentStatus { connectionStatus } cluster { id name } } } }'''
    print("Querying Rubrik for Hyper-V VMs...")
    data = graphql_query(token, base_url, query)
    valid_vms = []; vm_data = data.get('hypervVirtualMachines', {}); vm_nodes = vm_data.get('nodes', []) if vm_data else []
    if not vm_nodes: print("No Hyper-V VMs returned from the query."); return []
    for vm in vm_nodes:
        sla=vm.get('effectiveSlaDomain'); agent=vm.get('agentStatus')
        if sla and sla.get('id') and agent and agent.get('connectionStatus')=="CONNECTED":
            if vm.get('id') and vm.get('name') and vm.get('cluster') and vm.get('cluster').get('id'): valid_vms.append(vm)
            else: print(f"Warning: Skipping VM missing info: {vm.get('name', 'N/A')}")
    print(f"Found {len(valid_vms)} eligible Hyper-V VMs.")
    return valid_vms

def get_latest_snapshot_for_hyperv_vm(token, base_url, vm_id):
    query = ''' query GetVmSnapshots($workloadId: String!) { snapshotOfASnappableConnection(workloadId: $workloadId) { nodes { id date isExpired isQuarantined } } }'''
    print(f"Querying snapshots for VM ID: {vm_id} (using snapshotOfASnappableConnection)...")
    data = graphql_query(token, base_url, query, {"workloadId": vm_id})
    snapshot_data = data.get('snapshotOfASnappableConnection', {}); snapshot_nodes = snapshot_data.get('nodes', []) if snapshot_data is not None else []
    if not snapshot_nodes: sys.exit(f"Query successful, but no snapshots found via snapshotOfASnappableConnection for VM ID: {vm_id}.")
    valid_snaps = [s for s in snapshot_nodes if s.get('id') and not s.get('isExpired', False) and not s.get('isQuarantined', False)]
    if not valid_snaps: sys.exit(f"Found snapshots, but none are valid.")
    try:
        valid_snaps_with_dates = [s for s in valid_snaps if 'date' in s and s['date']]
        if not valid_snaps_with_dates: sys.exit(f"Error: Valid snapshots lack 'date' field.")
        latest_snap = max(valid_snaps_with_dates, key=lambda s: datetime.strptime(s['date'], '%Y-%m-%dT%H:%M:%S.%fZ'))
    except ValueError as e: sys.exit(f"Error parsing snapshot date: {e}. Excerpt: {valid_snaps_with_dates[:2]}")
    except KeyError: sys.exit(f"Error: 'date' field missing unexpectedly.")
    snapshot_fid = latest_snap.get('id')
    if not snapshot_fid: sys.exit(f"Error: Latest snapshot missing 'id'.")
    print(f"Found latest valid snapshot FID: {snapshot_fid} (Date: {latest_snap.get('date', 'N/A')})")
    return snapshot_fid

def get_connected_hyperv_servers_for_cluster(token, base_url, cluster_id):
    query = ''' query GetHyperVHosts { hypervServersPaginated(filter: [{field: IS_REPLICATED, texts: ["false"]}, {field: IS_RELIC, texts: ["false"]}]) { nodes { id name cluster { id name } status { connectivity } } } }'''
    print(f"Querying Hyper-V hosts for Cluster ID: {cluster_id}...")
    data = graphql_query(token, base_url, query)
    matching_servers = []; server_data = data.get('hypervServersPaginated', {}); server_nodes = server_data.get('nodes', []) if server_data else []
    if not server_nodes: print("No Hyper-V servers returned."); return []
    for srv in server_nodes:
        status=srv.get('status',{}); cluster=srv.get('cluster',{})
        if status.get('connectivity')=="Connected" and cluster.get('id')==cluster_id:
             if srv.get('id') and srv.get('name'): matching_servers.append(srv)
             else: print(f"Warning: Skipping server missing info: {srv}")
    print(f"Found {len(matching_servers)} connected hosts in Cluster {cluster_id}.")
    return matching_servers

def live_mount_vm(token, base_url, snapshot_fid, host_id, vm_name):
    mutation=''' mutation CreateHypervMount($input: CreateHypervVirtualMachineSnapshotMountInput!) { createHypervVirtualMachineSnapshotMount(input: $input) { id status } }'''
    variables = {"input": {"id": snapshot_fid, "config": { "hostId": host_id, "vmName": vm_name, "powerOn": True, "removeNetworkDevices": True }}}
    data = graphql_query(token, base_url, mutation, variables)
    return data.get('createHypervVirtualMachineSnapshotMount')

def check_rubrik_task_status(token, base_url, taskchain_uuid_for_context, source_vm_id, mount_start_time_iso):
    event_series_fragment = """ fragment EventSeriesFragment on ActivitySeries { id fid activitySeriesId lastUpdated lastActivityType lastActivityStatus objectId objectName objectType severity progress isCancelable isPolarisEventSeries location effectiveThroughput dataTransferred logicalSize organizations { id name __typename } clusterUuid clusterName __typename }"""
    query = """ query EventSeriesListQuery( $after: String, $filters: ActivitySeriesFilter, $first: Int, $sortBy: ActivitySeriesSortField, $sortOrder: SortOrder ) { activitySeriesConnection( after: $after first: $first filters: $filters sortBy: $sortBy sortOrder: $sortOrder ) { edges { cursor node { ...EventSeriesFragment } } pageInfo { endCursor hasNextPage } } } """ + event_series_fragment
    TARGET_MOUNT_ACTIVITY_TYPES = ["Recovery", "Mount", "LiveMount", "HyperVLiveMount", "HypervMount", "HyperV Snapshot Mount"] 
    variables = { "first": 5, "filters": { "objectFid": [source_vm_id], "lastUpdatedTimeGt": mount_start_time_iso, }, "sortBy": "LAST_UPDATED", "sortOrder": "DESC" }
    try:
        data = graphql_query(token, base_url, query, variables)
        activity_series_conn = data.get('activitySeriesConnection', {}); edges = activity_series_conn.get('edges', [])
        if not edges: return None 
        for i, edge in enumerate(edges):
            node = edge.get('node')
            if not node: continue
            node_activity_type = node.get('lastActivityType'); node_activity_status_raw = node.get('lastActivityStatus') 
            if node_activity_type in TARGET_MOUNT_ACTIVITY_TYPES:
                if node_activity_status_raw:
                    status_upper = node_activity_status_raw.upper()
                    if status_upper == "SUCCESS": normalized_status = "SUCCEEDED" 
                    else: normalized_status = status_upper 
                    return normalized_status
                else: print(f"Warning: Matched activity (type: {node_activity_type}) but no 'lastActivityStatus'."); return "UNKNOWN_STATUS_FIELD"
        return None 
    except Exception as e: print(f"Warning: Error during activity series check: {e}"); return None

def find_hyperv_mount_id(token, base_url, mounted_vm_name_to_find, source_vm_fid_for_context=None):
    query = """ query FindSpecificHyperVMount($filters: [HypervLiveMountFilterInput!]) { hypervMounts(filters: $filters) { nodes { id name } } } """
    variables = { "filters": [{"field": "MOUNT_NAME", "texts": [mounted_vm_name_to_find]}] }
    print(f"Querying for active Hyper-V mount with name: '{mounted_vm_name_to_find}' using 'hypervMounts' (filter field MOUNT_NAME)...")
    try:
        data = graphql_query(token, base_url, query, variables)
        mounts_data_root = data.get('hypervMounts', {}); mount_nodes = mounts_data_root.get('nodes', []) if isinstance(mounts_data_root, dict) else mounts_data_root if isinstance(mounts_data_root, list) else []
        if not mount_nodes:
            if not mounts_data_root : print(f"Warning: 'hypervMounts' field returned no data. Cannot find mount '{mounted_vm_name_to_find}'.")
            else: print(f"Warning: No active Hyper-V mounts found matching name '{mounted_vm_name_to_find}'. Response: {mounts_data_root}")
            return None
        if len(mount_nodes) > 1: print(f"Warning: Found multiple ({len(mount_nodes)}) mounts named '{mounted_vm_name_to_find}'. Using first ID.")
        first_mount = mount_nodes[0]; mount_id = first_mount.get('id'); found_mounted_name_in_node = first_mount.get('name') 
        print(f"Found potential mount: ID='{mount_id}', Reported Name on Mount Object='{found_mounted_name_in_node}'")
        if found_mounted_name_in_node != mounted_vm_name_to_find: print(f"Info: Mount 'name' ('{found_mounted_name_in_node}') vs search ('{mounted_vm_name_to_find}').")
        if not mount_id: print(f"Error: Found mount for '{mounted_vm_name_to_find}' but missing 'id'."); return None
        return mount_id
    except Exception as e: import traceback; print(f"Error querying mount ID: {e}"); traceback.print_exc(); return None

# --- MODIFIED unmount_vm Function ---
def unmount_vm(token, base_url, mount_id):
    """
    Unmounts a Hyper-V Live Mount using its specific mount ID.
    Returns True on successful initiation, False otherwise.
    """
    mutation = """
    mutation HyperVUnmount($input: DeleteHypervVirtualMachineSnapshotMountInput!) {
      deleteHypervVirtualMachineSnapshotMount(input: $input) {
        id       # ID of the asynchronous task created for the unmount
        status   # Initial status of the unmount task (e.g., QUEUED, RUNNING)
        error { message } # Still useful to catch immediate errors
      }
    }
    """
    variables = {"input": {"id": mount_id, "force": True}} 

    print(f"\nAttempting to unmount Hyper-V mount ID: {mount_id}...")
    try:
        data = graphql_query(token, base_url, mutation, variables)
        result = data.get('deleteHypervVirtualMachineSnapshotMount')

        if not result:
            print("Error: Received empty response for unmount mutation.")
            return False

        mutation_error = result.get('error')
        if mutation_error and mutation_error.get('message'):
            print(f"Error from unmount mutation: {mutation_error.get('message')}")
            return False

        unmount_task_id = result.get('id') 
        initial_task_status = result.get('status')

        if unmount_task_id:
            print(f"Unmount task successfully initiated. Task ID: {unmount_task_id}, Initial Status: {initial_task_status}")
            return True
        else:
            print(f"Warning: Unmount mutation initiated but did not return a task ID as 'id' for monitoring.")
            print(f"API Response for unmount: {json.dumps(result, indent=2)}")
            return True 

    except Exception as e:
        print(f"An unexpected error occurred during the unmount API call: {e}")
        import traceback
        traceback.print_exc()
        return False
# --- END MODIFIED unmount_vm Function ---

# --- Main Execution Logic (with Polling and Unmount) ---
def main():
    print("--- Rubrik Backup Validator ---")
    config = load_config()
    required_keys = ['RUBRIK_CLIENT_ID','RUBRIK_CLIENT_SECRET','RUBRIK_BASE_URL']
    if not all(k in config for k in required_keys): sys.exit(f"Error: Missing config keys: {', '.join(k for k in required_keys if k not in config)}")
    print("Authenticating...")
    token = get_auth_token(config['RUBRIK_CLIENT_ID'], config['RUBRIK_CLIENT_SECRET'], config['RUBRIK_BASE_URL'])
    print("Authentication successful.")
    while True:
        selection = input("\nSelect:\n 1) Oracle DB Backup\n 2) Hyper-V VM Live Mount & Unmount\nEnter choice: ").strip()
        if selection in ["1", "2"]: break
        else: print("Invalid selection. Please enter 1 or 2.")
    exit_code = 1; mount_id_to_unmount = None; mount_task_final_state = None
    if selection == "1": print("\n--- Oracle ---"); print("Not implemented."); exit_code = 1
    elif selection == "2":
        print("\n--- Hyper-V Live Mount & Unmount ---")
        try:
            vms = get_protected_connected_hyperv_vms(token, config['RUBRIK_BASE_URL'])
            if not vms: print("No eligible VMs found."); sys.exit(0)
            print("\nAvailable VMs:")
            for i, v_item in enumerate(vms): sla_info=v_item.get('effectiveSlaDomain',{}); cluster_info=v_item.get('cluster',{}); print(f" {i+1:>2}) {v_item.get('name','N/A')} (Cluster: {cluster_info.get('name','N/A')})")
            selected_vm=None
            while selected_vm is None:
                try: vm_choice_input = input(f"Select VM (1-{len(vms)}): "); vm_choice_idx = int(vm_choice_input)-1; selected_vm=vms[vm_choice_idx] if 0<=vm_choice_idx<len(vms) else print(f"Invalid selection (1-{len(vms)}).")
                except ValueError: print("Invalid input.")
            selected_vm_id=selected_vm.get('id'); selected_vm_name=selected_vm.get('name','N/A'); selected_vm_cluster_id=selected_vm.get('cluster',{}).get('id')
            if not selected_vm_id or not selected_vm_cluster_id: raise Exception(f"Cannot get IDs for '{selected_vm_name}'.")
            print(f"\nSelected VM: '{selected_vm_name}'")
            snapshot_fid = get_latest_snapshot_for_hyperv_vm(token, config['RUBRIK_BASE_URL'], selected_vm_id)
            if not snapshot_fid: raise Exception("Failed to get snapshot FID.")
            hyperv_hosts = get_connected_hyperv_servers_for_cluster(token, config['RUBRIK_BASE_URL'], selected_vm_cluster_id)
            if not hyperv_hosts: raise Exception(f"No connected hosts in Cluster {selected_vm_cluster_id}.")
            print("\nAvailable Hosts:")
            for i,h_item in enumerate(hyperv_hosts): print(f" {i+1:>2}) {h_item.get('name','N/A')} (ID: {h_item.get('id')})")
            selected_host=None
            while selected_host is None:
                 try: host_choice_input = input(f"Select Host (1-{len(hyperv_hosts)}): "); host_choice_idx = int(host_choice_input)-1; selected_host=hyperv_hosts[host_choice_idx] if 0<=host_choice_idx<len(hyperv_hosts) else print(f"Invalid selection (1-{len(hyperv_hosts)}).")
                 except ValueError: print("Invalid input.")
            selected_host_id=selected_host.get('id'); selected_host_name=selected_host.get('name','N/A')
            if not selected_host_id: raise Exception("Cannot get host ID.")
            print(f"Selected Host: '{selected_host_name}'")
            mount_vm_name = "";
            while not mount_vm_name: mount_vm_name=input("Enter unique name for mounted VM: ").strip(); print("Name required." if not mount_vm_name else "", end="")
            event_filter_start_time = datetime.utcnow() - timedelta(minutes=2)
            mount_start_time_iso = event_filter_start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            print(f"\n\nInitiating Live Mount for '{mount_vm_name}'...")
            result = live_mount_vm(token, config['RUBRIK_BASE_URL'], snapshot_fid, selected_host_id, mount_vm_name)
            taskchain_uuid = result.get('id') if result else None; initial_status = result.get('status', 'N/A') if result else "N/A"
            if not taskchain_uuid: print(f"\n❌ Mount initiation failed. Resp:\n{json.dumps(result, indent=2)}"); raise Exception("Mount failed.")
            print("\n✅ Live Mount task initiated."); print(f"  TaskChain UUID: {taskchain_uuid}"); print(f"  Initial Status: {initial_status}")
            print("\nMonitoring mount task status via Activity Log...")
            wait_seconds = 5; max_checks = 72
            for i in range(max_checks):
                current_status = check_rubrik_task_status(token, config['RUBRIK_BASE_URL'], taskchain_uuid, selected_vm_id, mount_start_time_iso)
                if current_status:
                    print(f"  [{time.strftime('%H:%M:%S')}] Poll {i+1}/{max_checks}: State = {current_status}")
                    final_states = ['SUCCEEDED', 'FAILED', 'CANCELED', 'WARNING', 'PARTIALLY_SUCCEEDED']
                    if current_status in final_states: mount_task_final_state=current_status; print(f"  Mount task final state: {mount_task_final_state}"); break
                else: print(f"  [{time.strftime('%H:%M:%S')}] Poll {i+1}/{max_checks}: Status check failed/task not in activity log.")
                if mount_task_final_state is None and i < max_checks - 1: print(f"  Waiting {wait_seconds}s..."); time.sleep(wait_seconds)
            else: 
                 if mount_task_final_state is None: mount_task_final_state = "TIMEOUT"; print("❌ Error: Mount task polling timed out.")
            if mount_task_final_state == 'SUCCEEDED':
                print("\nMount task SUCCEEDED."); print("Waiting before querying mount ID..."); time.sleep(10)
                mount_id_to_unmount = find_hyperv_mount_id(token, config['RUBRIK_BASE_URL'], mount_vm_name, selected_vm_id)
                if not mount_id_to_unmount: print(f"⚠️ CRITICAL: Mount SUCCEEDED, but Mount ID for '{mount_vm_name}' not found. MANUAL UNMOUNT REQUIRED."); exit_code = 1
            else: print(f"\n❌ Mount task failed (State: {mount_task_final_state})."); exit_code = 1
        except Exception as e: print(f"\n❌ Error during mount process: {e}"); exit_code = 1 
        if mount_id_to_unmount:
            print("\n--- Initiating Unmount ---")
            unmount_initiated = unmount_vm(token, config['RUBRIK_BASE_URL'], mount_id_to_unmount)
            if unmount_initiated: print("✅ Unmount initiated."); exit_code = 0 if mount_task_final_state == 'SUCCEEDED' else 1
            else: print("❌ Error during unmount initiation."); exit_code = 1
        elif mount_task_final_state == 'SUCCEEDED' and not mount_id_to_unmount: print("Skipping unmount: Mount ID not found."); 
        elif mount_task_final_state != 'SUCCEEDED': print(f"Skipping unmount: Mount task state {mount_task_final_state}."); 
    print(f"\n--- Script finished with exit code {exit_code} ---")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()