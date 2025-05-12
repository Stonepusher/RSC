import json
import requests
import sys
from datetime import datetime, timedelta
import time
import argparse 
import random   
import string   

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
    """Executes a GraphQL query against the Rubrik API. Raises exceptions on error."""
    headers = {"Authorization": f"Bearer {token}"}
    api_url = f"{base_url}/api/graphql"
    response = None 
    try:
        response = requests.post(api_url, json={"query": query, "variables": variables or {}}, headers=headers, timeout=120)
        response.raise_for_status() # Raises HTTPError for 4xx/5xx
        result = response.json()
        if 'errors' in result and result['errors']:
            error_messages = [err.get('message', 'Unknown GraphQL error') for err in result['errors']]
            error_details = [str(err.get('extensions', '')) for err in result['errors']]
            full_error_message = "\n- ".join([f"{msg} {det}".strip() for msg, det in zip(error_messages, error_details)])
            raise Exception(f"GraphQL API errors returned:\n- {full_error_message}")
        if 'data' not in result and not (result.get('errors')):
             print(f"Warning: No 'data' key in GraphQL response and no errors. Full response: {json.dumps(result, indent=2)}")
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
             except Exception as e_parse: response_text = f"(Error processing response body: {e_parse})"
        raise Exception(f"GraphQL HTTP error: {exc}\nResponse body (Status: {response_status}):\n{response_text}")
    except requests.exceptions.Timeout: 
        raise Exception(f"GraphQL request timed out accessing {api_url}")
    except requests.exceptions.RequestException as exc: 
        raise Exception(f"GraphQL network error (non-HTTP): {exc}")
    except json.JSONDecodeError as exc_json:
        status_code = "N/A"; resp_text = "(Response object not available)"
        if response is not None: status_code = response.status_code; resp_text = (response.text[:500] + '...') if len(response.text) > 500 else response.text
        raise Exception(f"GraphQL error: Could not decode JSON response. Status: {status_code}. Excerpt:\n{resp_text}\nOriginal: {exc_json}")
    except Exception as exc: 
         raise Exception(f"An unexpected error occurred during GraphQL query: {exc}")

def get_all_paginated_nodes(token, base_url, query_string, variables, connection_path_keys):
    all_nodes = []
    current_variables = variables.copy()
    current_variables['first'] = 50 
    current_variables['after'] = None
    page_count = 0
    print(f"Fetching paginated results for: {'.'.join(connection_path_keys)}...")
    while True:
        page_count += 1
        try:
            page_data_root = graphql_query(token, base_url, query_string, current_variables)
            if not page_data_root: 
                print(f"Warning: No data from graphql_query on page {page_count} for {'.'.join(connection_path_keys)}.")
                break
        except Exception as e: 
            print(f"Error fetching page {page_count} for {'.'.join(connection_path_keys)}: {e}")
            break 
        connection = page_data_root; valid_path = True
        for key in connection_path_keys:
            if isinstance(connection, dict) and key in connection:
                connection = connection.get(key)
                if connection is None: 
                    print(f"Info: Path '{'.'.join(connection_path_keys)}' to null on page {page_count}.")
                    valid_path = False; break
            else:
                print(f"Error: Key '{key}' not found in path '{'.'.join(connection_path_keys)}' (Page {page_count}).")
                print(f"Object keys: {list(connection.keys()) if isinstance(connection, dict) else 'Not a dict'}")
                valid_path = False; break
        if not valid_path or connection is None: break
        nodes_on_page = connection.get('nodes', [])
        all_nodes.extend(nodes_on_page)
        page_info = connection.get('pageInfo', {}); has_next_page = page_info.get('hasNextPage', False); end_cursor = page_info.get('endCursor')
        if has_next_page and end_cursor: current_variables['after'] = end_cursor
        else: break
    print(f"Fetched {len(all_nodes)} items across {page_count} page(s) for {'.'.join(connection_path_keys)}.")
    return all_nodes

# --- Oracle Specific Functions ---
def get_protected_oracle_dbs(token, base_url):
    query = ''' query GetOracleDBs($first: Int, $after: String) { oracleDatabases( filter:[ {field:IS_RELIC, texts:"false"}, {field:IS_REPLICATED, texts:"false"} ], first: $first, after: $after ) { nodes { id name effectiveSlaDomain{id name} cluster{id name} } pageInfo { hasNextPage endCursor } } }'''
    print("Querying Rubrik for Oracle Databases (paginated)...")
    all_db_nodes = get_all_paginated_nodes(token, base_url, query, {}, ['oracleDatabases'])
    valid_dbs = []
    if not all_db_nodes: print("No Oracle databases returned."); return []
    for db in all_db_nodes:
        if db.get('id') and db.get('name') and db.get('effectiveSlaDomain') and db.get('effectiveSlaDomain').get('id') and db.get('cluster') and db.get('cluster').get('id'):
            valid_dbs.append(db)
        else: print(f"Warning: Skipping Oracle DB due to missing info: {db.get('name', 'N/A')}")
    print(f"Filtered down to {len(valid_dbs)} eligible Oracle Databases.")
    return valid_dbs

def get_latest_oracle_snapshot(token, base_url, oracle_db_id):
    query = ''' query GetOracleDbSnapshot($fid: UUID!){ oracleDatabase(fid:$fid){ newestSnapshot{id date isExpired isQuarantined} } }'''
    print(f"Querying latest snapshot for Oracle DB ID: {oracle_db_id}...")
    data_root = graphql_query(token, base_url, query, {"fid": oracle_db_id})
    oracle_db_data = data_root.get('oracleDatabase')
    if not oracle_db_data: raise Exception(f"No Oracle DB data for FID: {oracle_db_id}")
    snapshot_data = oracle_db_data.get('newestSnapshot')
    if not snapshot_data: raise Exception(f"Oracle DB {oracle_db_id} has no 'newestSnapshot'.")
    if snapshot_data.get('isExpired') or snapshot_data.get('isQuarantined'): raise Exception(f"Newest snapshot for DB {oracle_db_id} (ID: {snapshot_data.get('id')}) is invalid.")
    if not snapshot_data.get('id') or not snapshot_data.get('date'): raise Exception(f"Snapshot for DB {oracle_db_id} missing ID/date.")
    print(f"Found newest valid snapshot ID: {snapshot_data['id']} (Date: {snapshot_data['date']})")
    return snapshot_data

def get_oracle_hosts_for_cluster(token, base_url, cluster_id_to_filter):
    query=''' query GetOracleHosts($filters: [Filter!], $first: Int, $after: String) { oracleTopLevelDescendants( filter: $filters, first: $first, after: $after ){ nodes{ id name objectType cluster{id name} } pageInfo { hasNextPage endCursor } } }'''
    initial_vars = { "filters": [ {"field": "IS_RELIC", "texts": ["false"]}, {"field": "IS_REPLICATED", "texts": ["false"]} ] }
    print(f"Querying Oracle top-level descendants (paginated). Will filter for cluster '{cluster_id_to_filter}' client-side...")
    all_descendants = get_all_paginated_nodes(token, base_url, query, initial_vars, ['oracleTopLevelDescendants'])
    hosts = []
    if not all_descendants: print("No Oracle top-level descendants returned."); return []
    for h_node in all_descendants:
        if h_node.get('cluster', {}).get('id') == cluster_id_to_filter and h_node.get('objectType') in ["OracleHost", "OracleRac"] and h_node.get('id') and h_node.get('name'):
            hosts.append(h_node)
    print(f"Found {len(hosts)} Oracle Hosts/RACs in target cluster '{cluster_id_to_filter}'.")
    return hosts

def validate_oracle_db_backup(token, base_url, oracle_db_id, snapshot_id, host_id):
    mutation=''' mutation ValidateOracle($input:ValidateOracleDatabaseBackupsInput!){ validateOracleDatabaseBackups(input:$input){ id status } }'''
    variables={ "input":{ "id":oracle_db_id, "config":{ "targetOracleHostOrRacId":host_id, "recoveryPoint":{"snapshotId":snapshot_id,"scn":None} }}}
    print("Sending Oracle DB validation request...")
    data_root = graphql_query(token,base_url,mutation,variables)
    validation_response = data_root.get('validateOracleDatabaseBackups')
    if not validation_response or not validation_response.get('id'): raise Exception(f"Failed to initiate Oracle validation. Resp: {validation_response}")
    return validation_response

def wait_for_oracle_job(token, base_url, job_id, cluster_uuid):
    query = ''' query GetOracleAsyncStatus($id: String!, $clusterUuid: String!) { oracleDatabaseAsyncRequestDetails(input:{id:$id, clusterUuid:$clusterUuid}) { progress status error { message } } }'''
    variables = { "id": job_id, "clusterUuid": cluster_uuid }
    poll_interval_seconds = 5; max_oracle_checks = 120    
    print(f"Monitoring Oracle job ID: {job_id} (cluster: {cluster_uuid}, every {poll_interval_seconds}s)...")
    for check_count in range(max_oracle_checks):
        try:
            data_root = graphql_query(token, base_url, query, variables)
            job_details = data_root.get('oracleDatabaseAsyncRequestDetails')
            status = "UNKNOWN"; progress = 0
            if job_details: status = job_details.get('status', "UNKNOWN").upper(); progress = job_details.get('progress', 0)
            else: print(f"Warning: Could not retrieve details for Oracle job {job_id}. Attempt {check_count+1}.")
            print(f"  [{time.strftime('%H:%M:%S')}] Poll {check_count+1}/{max_oracle_checks}: Status = {status}, Progress = {progress if status!='UNKNOWN' else 'N/A'}%")
            if status == "SUCCEEDED": print("✅ Oracle DB validation job SUCCEEDED."); return True
            elif status in ["FAILED", "FAILURE"]: print(f"❌ Oracle DB validation FAILED: {job_details.get('error',{}).get('message','N/A') if job_details else 'No details'}"); return False
            elif status in ["CANCELLED", "CANCELED"]: print(f"Oracle DB validation job {status}."); return False
        except Exception as e: print(f"Warning: Exception during Oracle job poll: {e}. Attempt {check_count+1}.")
        if check_count < max_oracle_checks - 1: time.sleep(poll_interval_seconds)
    print(f"❌ Oracle DB validation job timed out: {job_id}"); return False

# --- Hyper-V Functions ---
def get_protected_connected_hyperv_vms(token, base_url):
    query = ''' query GetHyperVVms($first: Int, $after: String) { hypervVirtualMachines( filter: [{field: IS_RELIC, texts: ["false"]}, {field: IS_REPLICATED, texts: ["false"]}], first: $first, after: $after ) { nodes { id name osType effectiveSlaDomain { id name } agentStatus { connectionStatus } cluster { id name } } pageInfo { hasNextPage endCursor } } }'''
    print("Querying Rubrik for Hyper-V VMs (paginated)...")
    all_vm_nodes = get_all_paginated_nodes(token, base_url, query, {}, ['hypervVirtualMachines'])
    valid_vms = [];
    if not all_vm_nodes: print("No Hyper-V VMs returned."); return []
    for vm in all_vm_nodes:
        sla=vm.get('effectiveSlaDomain'); agent=vm.get('agentStatus')
        if sla and sla.get('id') and agent and agent.get('connectionStatus')=="CONNECTED":
            if vm.get('id') and vm.get('name') and vm.get('cluster') and vm.get('cluster').get('id'): valid_vms.append(vm)
    print(f"Found {len(valid_vms)} eligible Hyper-V VMs.")
    return valid_vms

def get_latest_snapshot_for_hyperv_vm(token, base_url, vm_id):
    query = ''' query GetVmSnapshots($workloadId: String!, $first: Int, $after: String) { snapshotOfASnappableConnection(workloadId: $workloadId, first: $first, after: $after) { nodes { id date isExpired isQuarantined } pageInfo { hasNextPage endCursor } } }'''
    print(f"Querying snapshots for VM ID: {vm_id} (paginated)...")
    all_snapshot_nodes = get_all_paginated_nodes(token, base_url, query, {"workloadId": vm_id}, ['snapshotOfASnappableConnection'])
    if not all_snapshot_nodes: sys.exit(f"No snapshots found for VM ID: {vm_id}.")
    valid_snaps = [s for s in all_snapshot_nodes if s.get('id') and not s.get('isExpired', False) and not s.get('isQuarantined', False)]
    if not valid_snaps: sys.exit(f"Found snapshots, but none are valid.")
    try:
        valid_snaps_with_dates = [s for s in valid_snaps if 'date' in s and s['date']]
        if not valid_snaps_with_dates: sys.exit(f"Error: Valid snapshots lack 'date' field.")
        latest_snap = max(valid_snaps_with_dates, key=lambda s: datetime.strptime(s['date'], '%Y-%m-%dT%H:%M:%S.%fZ'))
    except ValueError as e: sys.exit(f"Error parsing snapshot date: {e}. Excerpt: {valid_snaps_with_dates[:2]}")
    snapshot_fid = latest_snap.get('id')
    if not snapshot_fid: sys.exit(f"Error: Latest snapshot missing 'id'.")
    print(f"Found latest valid snapshot FID: {snapshot_fid} (Date: {latest_snap.get('date', 'N/A')})")
    return snapshot_fid

def get_connected_hyperv_servers_for_cluster(token, base_url, cluster_id_to_filter):
    query = ''' query GetHyperVHosts($filters: [Filter!], $first: Int, $after: String) { hypervServersPaginated( filter: $filters, first: $first, after: $after ) { nodes { id name cluster { id name } status { connectivity } } pageInfo { hasNextPage endCursor } } }'''
    initial_vars = { "filters": [ {"field": "IS_REPLICATED", "texts": ["false"]}, {"field": "IS_RELIC", "texts": ["false"]}, {"field": "CLUSTER_ID", "texts": [cluster_id_to_filter]} ] }
    print(f"Querying Hyper-V hosts for Cluster ID: {cluster_id_to_filter} (paginated, using [Filter!])...") 
    all_server_nodes = get_all_paginated_nodes(token, base_url, query, initial_vars, ['hypervServersPaginated'])
    matching_servers = []
    if not all_server_nodes: print("No Hyper-V servers returned."); return []
    for srv in all_server_nodes:
        status = srv.get('status', {}); cluster = srv.get('cluster', {})
        if status.get('connectivity') == "Connected" and cluster.get('id') == cluster_id_to_filter: 
             if srv.get('id') and srv.get('name'): matching_servers.append(srv)
        elif status.get('connectivity') == "Connected" and cluster.get('id') != cluster_id_to_filter:
             print(f"Debug: Server '{srv.get('name')}' in cluster '{cluster.get('id')}' (target: '{cluster_id_to_filter}') passed server filter unexpectedly.")
    print(f"Found {len(matching_servers)} connected hosts in Cluster {cluster_id_to_filter}.")
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
                    if status_upper == "SUCCESS": return "SUCCEEDED" 
                    return status_upper 
                else: print(f"Warning: Matched activity (type: {node_activity_type}) but no 'lastActivityStatus'."); return "UNKNOWN_STATUS_FIELD"
        return None 
    except Exception as e: print(f"Warning: Error during activity series check: {e}"); return None

def find_hyperv_mount_id(token, base_url, mounted_vm_name_to_find, source_vm_fid_for_context=None):
    query = """ query FindSpecificHyperVMount($filters: [HypervLiveMountFilterInput!], $first: Int, $after: String) { hypervMounts(filters: $filters, first: $first, after: $after) { nodes { id name } pageInfo { hasNextPage endCursor } } }"""
    initial_vars = { "filters": [{"field": "MOUNT_NAME", "texts": [mounted_vm_name_to_find]}] }
    print(f"Querying all active Hyper-V mounts with name: '{mounted_vm_name_to_find}' (paginated)...")
    all_mount_nodes = get_all_paginated_nodes(token, base_url, query, initial_vars, ['hypervMounts'])
    if not all_mount_nodes: print(f"Warning: No mount found named '{mounted_vm_name_to_find}'."); return None
    if len(all_mount_nodes) > 1: print(f"Warning: Found {len(all_mount_nodes)} mounts named '{mounted_vm_name_to_find}'. Using first.")
    first_mount = all_mount_nodes[0]; mount_id = first_mount.get('id'); found_name = first_mount.get('name')
    print(f"Found potential mount: ID='{mount_id}', Reported Name='{found_name}'")
    if found_name != mounted_vm_name_to_find: print(f"Info: Mount 'name' ('{found_name}') vs search ('{mounted_vm_name_to_find}').")
    if not mount_id: print(f"Error: Found mount for '{mounted_vm_name_to_find}' but missing 'id'."); return None
    return mount_id

# --- MODIFIED unmount_vm Function (with Retries) ---
def unmount_vm(token, base_url, mount_id, max_retries=3, retry_delay_seconds=10):
    """
    Unmounts a Hyper-V Live Mount using its specific mount ID.
    Retries on failure up to 'max_retries' times with a delay.
    Returns True on successful initiation, False otherwise.
    """
    mutation = """
    mutation HyperVUnmount($input: DeleteHypervVirtualMachineSnapshotMountInput!) {
      deleteHypervVirtualMachineSnapshotMount(input: $input) {
        id       # ID of the asynchronous task created for the unmount
        status   # Initial status of the unmount task (e.g., QUEUED, RUNNING)
        error { message } 
      }
    }"""
    variables = {"input": {"id": mount_id, "force": True}} 
    print(f"\nAttempting to unmount Hyper-V mount ID: {mount_id}...")
    for attempt in range(max_retries):
        print(f"--- Unmount Attempt {attempt + 1}/{max_retries} for mount ID {mount_id} ---")
        # print(f"--- [DEBUG unmount_vm] Sending unmount mutation with variables: {json.dumps(variables, indent=2)} ---")
        try:
            data = graphql_query(token, base_url, mutation, variables)
            result = data.get('deleteHypervVirtualMachineSnapshotMount')
            if not result:
                print(f"Attempt {attempt + 1}: Received empty response for unmount mutation.")
            else:
                mutation_error = result.get('error')
                if mutation_error and mutation_error.get('message'):
                    print(f"Attempt {attempt + 1}: Error from unmount mutation: {mutation_error.get('message')}")
                else:
                    unmount_task_id = result.get('id') 
                    initial_task_status = result.get('status')
                    if unmount_task_id:
                        print(f"✅ Unmount task successfully initiated on attempt {attempt + 1}.")
                        print(f"   Task ID: {unmount_task_id}, Initial Status: {initial_task_status}")
                        return True 
                    else:
                        print(f"Attempt {attempt + 1}: Warning: Unmount initiated but no task ID. Response: {json.dumps(result, indent=2)}")
                        return True 
        except Exception as e: 
            print(f"Attempt {attempt + 1}: Error during unmount API call: {e}")
        if attempt < max_retries - 1:
            print(f"Unmount attempt {attempt + 1} failed. Retrying in {retry_delay_seconds} seconds...")
            time.sleep(retry_delay_seconds)
        else:
            print(f"❌ All {max_retries} unmount attempts failed for mount ID {mount_id}.")
            return False 
    return False 
# --- END MODIFIED unmount_vm Function ---

# --- Main Execution Logic ---
def main():
    parser = argparse.ArgumentParser(description="Rubrik Backup Validator: Live Mount & Unmount Utility.")
    parser.add_argument("--workflow", choices=["hyperv", "oracle"], type=str.lower, help="Specify workflow: 'hyperv' or 'oracle'.")
    args = parser.parse_args()

    print("--- Rubrik Backup Validator ---")
    config = load_config()
    required_keys = ['RUBRIK_CLIENT_ID','RUBRIK_CLIENT_SECRET','RUBRIK_BASE_URL']
    if not all(k in config for k in required_keys): sys.exit(f"Error: Missing config keys: {', '.join(k for k in required_keys if k not in config)}")
    print("Authenticating...")
    token = get_auth_token(config['RUBRIK_CLIENT_ID'], config['RUBRIK_CLIENT_SECRET'], config['RUBRIK_BASE_URL'])
    print("Authentication successful.")

    selection = None
    if args.workflow:
        if args.workflow == "hyperv": selection = "2"; print("Hyper-V workflow selected via command line.")
        elif args.workflow == "oracle": selection = "1"; print("Oracle DB workflow selected via command line.")
    else:
        while True: 
            selection = input("\nSelect:\n 1) Oracle DB Backup\n 2) Hyper-V VM Live Mount & Unmount\nEnter choice: ").strip()
            if selection in ["1", "2"]: break
            else: print("Invalid selection. Please enter 1 or 2.")

    exit_code = 1; mount_id_to_unmount = None; mount_task_final_state = None
    
    if selection == "1": # Oracle Workflow
        print("\n--- Oracle DB Backup Validation ---")
        try:
            all_oracle_dbs = get_protected_oracle_dbs(token, config['RUBRIK_BASE_URL'])
            if not all_oracle_dbs: raise Exception("No eligible Oracle DBs found.")
            display_dbs = all_oracle_dbs
            if len(all_oracle_dbs) > 15: 
                print(f"\nFound {len(all_oracle_dbs)} eligible Oracle DBs.")
                filter_name = input("Enter part of DB name to filter (or Enter for first 15): ").strip().lower()
                if filter_name:
                    filtered_dbs = [db for db in all_oracle_dbs if filter_name in db.get('name', '').lower()]
                    if not filtered_dbs: print(f"No DBs match '{filter_name}'. Showing first 15.")
                    else: display_dbs = filtered_dbs
                if len(display_dbs) > 15 : display_dbs = display_dbs[:15]
            if not display_dbs : raise Exception("No Oracle DBs to display.")
            print("\nAvailable Oracle Databases:"); [print(f" {i+1:>2}) {db.get('name','N/A')} (Cluster: {db.get('cluster',{}).get('name','N/A')}, SLA: {db.get('effectiveSlaDomain',{}).get('name','N/A')})") for i,db in enumerate(display_dbs)]
            selected_db_obj = None
            while selected_db_obj is None:
                try:
                    db_choice_input = input(f"Select Oracle DB (1-{len(display_dbs)}): ")
                    db_choice_idx = int(db_choice_input) - 1
                    if 0 <= db_choice_idx < len(display_dbs): selected_db_obj = display_dbs[db_choice_idx]
                    else: print(f"Invalid selection. Please enter a number between 1 and {len(display_dbs)}.")
                except ValueError: print("Invalid input. Please enter a number.")
            selected_db_id=selected_db_obj.get('id'); selected_db_name=selected_db_obj.get('name','N/A'); selected_db_cluster_id=selected_db_obj.get('cluster',{}).get('id')
            if not selected_db_id or not selected_db_cluster_id: raise Exception(f"Cannot get IDs for DB '{selected_db_name}'.")
            print(f"\nSelected Oracle DB: '{selected_db_name}'")
            snapshot = get_latest_oracle_snapshot(token, config['RUBRIK_BASE_URL'], selected_db_id)
            snapshot_id = snapshot.get('id')
            print(f"Using snapshot ID: {snapshot_id} (Date: {snapshot.get('date')})")
            all_oracle_hosts = get_oracle_hosts_for_cluster(token, config['RUBRIK_BASE_URL'], selected_db_cluster_id)
            if not all_oracle_hosts: raise Exception(f"No Oracle hosts for cluster '{selected_db_cluster_id}'.")
            display_hosts = all_oracle_hosts
            if len(all_oracle_hosts) > 15:
                print(f"\nFound {len(all_oracle_hosts)} Oracle hosts/RACs.")
                filter_host = input("Enter part of host/RAC name to filter (or Enter for first 15): ").strip().lower()
                if filter_host:
                    filtered_hosts = [h for h in all_oracle_hosts if filter_host in h.get('name','').lower()]
                    if not filtered_hosts: print(f"No hosts match '{filter_host}'. Showing first 15.")
                    else: display_hosts = filtered_hosts
                if len(display_hosts) > 15: display_hosts = display_hosts[:15]
            if not display_hosts: raise Exception("No Oracle hosts/RACs to display.")
            print("\nAvailable Oracle Hosts/RACs:"); [print(f" {i+1:>2}) {h.get('name','N/A')} (Type: {h.get('objectType','N/A')})") for i,h in enumerate(display_hosts)]
            selected_host_obj = None
            while selected_host_obj is None: 
                try:
                    host_choice_input = input(f"Select Oracle Host/RAC (1-{len(display_hosts)}): ")
                    host_choice_idx = int(host_choice_input) - 1
                    if 0 <= host_choice_idx < len(display_hosts): selected_host_obj = display_hosts[host_choice_idx]
                    else: print(f"Invalid selection. Please enter a number between 1 and {len(display_hosts)}.")
                except ValueError: print("Invalid input. Please enter a number.")
            selected_host_id = selected_host_obj.get('id')
            if not selected_host_id: raise Exception("Cannot get ID for selected Oracle host/RAC.")
            print(f"Selected target Oracle Host/RAC: '{selected_host_obj.get('name')}'")
            print("\nInitiating Oracle DB backup validation...")
            validate_result = validate_oracle_db_backup(token, config['RUBRIK_BASE_URL'], selected_db_id, snapshot_id, selected_host_id)
            job_id = validate_result.get('id') 
            if not job_id: print(f"\n❌ Oracle validation failed. Resp:\n{json.dumps(validate_result, indent=2)}"); raise Exception("Oracle validation initiation failed.")
            print("\n✅ Oracle validation task initiated."); print(f"  Job ID: {job_id}, Initial Status: {validate_result.get('status','N/A')}")
            validation_succeeded = wait_for_oracle_job(token, config['RUBRIK_BASE_URL'], job_id, selected_db_cluster_id)
            if validation_succeeded: print("✅ Oracle DB Validation Overall: SUCCEEDED"); exit_code = 0
            else: print("❌ Oracle DB Validation Overall: FAILED or Timed Out"); exit_code = 1
        except Exception as e: print(f"\n❌ Error during Oracle validation: {e}"); exit_code = 1
    
    elif selection == "2": # Hyper-V Path
        print("\n--- Hyper-V Live Mount & Unmount ---")
        selected_vm_name_for_mount = "default-hyperv-mount" 
        try:
            all_vms = get_protected_connected_hyperv_vms(token, config['RUBRIK_BASE_URL'])
            if not all_vms: print("No eligible VMs found."); sys.exit(0)
            display_vms = all_vms
            if len(all_vms) > 15: 
                print(f"\nFound {len(all_vms)} eligible VMs.")
                filter_name = input("Enter part of VM name to filter (or Enter for first 15): ").strip().lower()
                if filter_name:
                    filtered_vms = [vm for vm in all_vms if filter_name in vm.get('name', '').lower()]
                    if not filtered_vms: print(f"No VMs match '{filter_name}'. Showing first 15.")
                    else: display_vms = filtered_vms
                if len(display_vms) > 15 : display_vms = display_vms[:15] 
            if not display_vms : print("No VMs to display."); sys.exit(0)
            print("\nAvailable VMs for selection:")
            for i, v_item in enumerate(display_vms): sla_info=v_item.get('effectiveSlaDomain',{}); cluster_info=v_item.get('cluster',{}); print(f" {i+1:>2}) {v_item.get('name','N/A')} (Cluster: {cluster_info.get('name','N/A')})")
            selected_vm=None 
            while selected_vm is None: 
                try:
                    vm_choice_input = input(f"Select VM (1-{len(display_vms)}): "); vm_choice_idx = int(vm_choice_input) - 1
                    if 0 <= vm_choice_idx < len(display_vms): selected_vm = display_vms[vm_choice_idx]
                    else: print(f"Invalid selection (1-{len(display_vms)}).")
                except ValueError: print("Invalid input.")
            selected_vm_id=selected_vm.get('id'); selected_vm_name_for_mount=selected_vm.get('name','N/A'); selected_vm_cluster_id=selected_vm.get('cluster',{}).get('id')
            if not selected_vm_id or not selected_vm_cluster_id: raise Exception(f"Cannot get IDs for '{selected_vm_name_for_mount}'.")
            print(f"\nSelected VM: '{selected_vm_name_for_mount}'")
            snapshot_fid = get_latest_snapshot_for_hyperv_vm(token, config['RUBRIK_BASE_URL'], selected_vm_id)
            if not snapshot_fid: raise Exception("Failed to get snapshot FID.")
            all_hyperv_hosts = get_connected_hyperv_servers_for_cluster(token, config['RUBRIK_BASE_URL'], selected_vm_cluster_id)
            if not all_hyperv_hosts: raise Exception(f"No connected hosts in Cluster {selected_vm_cluster_id}.")
            display_hosts = all_hyperv_hosts
            if len(all_hyperv_hosts) > 15:
                print(f"\nFound {len(all_hyperv_hosts)} eligible hosts.")
                filter_host_name = input("Enter part of host name to filter (or Enter for first 15): ").strip().lower()
                if filter_host_name:
                    filtered_hosts=[h for h in all_hyperv_hosts if filter_host_name in h.get('name','').lower()]
                    if not filtered_hosts: print(f"No hosts match '{filter_host_name}'. Showing first 15.")
                    else: display_hosts = filtered_hosts
                if len(display_hosts) > 15: display_hosts = display_hosts[:15]
            if not display_hosts: print("No hosts to display."); sys.exit(0)
            print("\nAvailable Hosts:")
            for i,h_item in enumerate(display_hosts): print(f" {i+1:>2}) {h_item.get('name','N/A')} (ID: {h_item.get('id')})")
            selected_host=None 
            while selected_host is None:
                 try:
                     host_choice_input = input(f"Select Host (1-{len(display_hosts)}): "); host_choice_idx = int(host_choice_input) - 1
                     if 0 <= host_choice_idx < len(display_hosts): selected_host = display_hosts[host_choice_idx]
                     else: print(f"Invalid selection (1-{len(display_hosts)}).")
                 except ValueError: print("Invalid input.")
            selected_host_id=selected_host.get('id'); selected_host_name=selected_host.get('name','N/A')
            if not selected_host_id: raise Exception("Cannot get host ID.")
            print(f"Selected Host: '{selected_host_name}'")
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            base_name = "".join(c if c.isalnum() or c in ['_','-'] else '_' for c in selected_vm_name_for_mount)[:20] 
            mount_vm_name = f"{base_name}-mount-{random_suffix}"
            print(f"Generated mount VM name: {mount_vm_name}")
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
        except Exception as e: print(f"\n❌ Error during Hyper-V mount process: {e}"); exit_code = 1 
        if mount_id_to_unmount:
            print("\n--- Initiating Unmount ---")
            unmount_initiated = unmount_vm(token, config['RUBRIK_BASE_URL'], mount_id_to_unmount)
            if unmount_initiated: print("✅ Unmount initiated."); exit_code = 0 if mount_task_final_state == 'SUCCEEDED' and exit_code != 1 else 1 
            else: print("❌ Error during unmount initiation."); exit_code = 1
        elif mount_task_final_state == 'SUCCEEDED' and not mount_id_to_unmount: print("Skipping unmount: Mount ID not found."); 
        elif mount_task_final_state != 'SUCCEEDED': print(f"Skipping unmount: Mount task state {mount_task_final_state}."); 
    
    print(f"\n--- Script finished with exit code {exit_code} ---")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()