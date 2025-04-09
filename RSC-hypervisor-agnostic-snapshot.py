import json
import requests
import sys

def load_config(config_file):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to parse the configuration file '{config_file}'. Please ensure it is a valid JSON.")
        return None

def get_access_token(client_id, client_secret, rubrik_base_url):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    auth_endpoint = f'{rubrik_base_url}/api/client_token'
    auth_payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(auth_endpoint, headers=headers, data=auth_payload)
    if response.status_code == 200:
        data = response.json()
        access_token = data.get('access_token')
        return access_token
    else:
        print(f"Error retrieving access token: {response.status_code} - {response.text}")
        return None

def query_vms(access_token, rubrik_base_url, query, variables=None):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    vms_endpoint = f'{rubrik_base_url}/api/graphql'
    response = requests.post(vms_endpoint, headers=headers, json={"query": query, "variables": variables})
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error retrieving VMs: {response.status_code} - {response.text}")
        return None

def retrieve_all_pages(access_token, rubrik_base_url, query, node_name):
    all_nodes = []
    variables = {}
    while True:
        data = query_vms(access_token, rubrik_base_url, query, variables)
        if not data:
            break
        nodes = data.get('data', {}).get(node_name, {}).get('nodes', [])
        all_nodes.extend(nodes)
        page_info = data.get('data', {}).get(node_name, {}).get('pageInfo', {})
        if page_info.get('hasNextPage'):
            variables['endCursor'] = page_info.get('endCursor')
        else:
            break
    return all_nodes

def get_connected_vms(access_token, rubrik_base_url):
    vSphere_query = """
        query vspherePagedQuery($endCursor: String) {
            vSphereVmNewConnection(filter: [{field: IS_RELIC texts: "false"}, {field: IS_REPLICATED texts: "false"}], after: $endCursor) {
                nodes {
                    name
                    id
                }
                pageInfo { endCursor hasNextPage }
            }
        }
    """
    ahv_query = """
        query ahvPagedQuery($endCursor: String) {
            nutanixVms(filter: [{field: IS_RELIC texts: "false"}, {field: IS_REPLICATED texts: "false"}], after: $endCursor) {
                nodes {
                    name
                    id
                }
                pageInfo { endCursor hasNextPage }
            }
        }
    """
    hyperv_query = """
        query hyperVPagedQuery($endCursor: String) {
            hypervVirtualMachines(filter: [{field: IS_RELIC texts: "false"}, {field: IS_REPLICATED texts: "false"}], after: $endCursor) {
                nodes {
                    name
                    id
                }
                pageInfo { endCursor hasNextPage }
            }
        }
    """
    vSphere_vms = retrieve_all_pages(access_token, rubrik_base_url, vSphere_query, 'vSphereVmNewConnection')
    ahv_vms = retrieve_all_pages(access_token, rubrik_base_url, ahv_query, 'nutanixVms')
    hyperv_vms = retrieve_all_pages(access_token, rubrik_base_url, hyperv_query, 'hypervVirtualMachines')

    return vSphere_vms, ahv_vms, hyperv_vms

def get_vm_by_id(vm_id, vm_lists):
    for vm_list in vm_lists:
        for vm in vm_list:
            if vm['id'] == vm_id:
                return vm
    return None

def get_sla_domain(access_token, rubrik_base_url, vm_id, vm_type):
    # Dynamically generate the GraphQL query specific configuration.
    vm_type_field = {
        "VMware": {"query": "vSphereVmNew", "id_field": "fid"}, # Corrected vSphere case
        "Nutanix": {"query": "nutanixVm", "id_field": "fid"}, # Updated Nutanix to use 'fid'
        "Hyper-V": {"query": "hypervVirtualMachine", "id_field": "fid"} # Assuming Hyper-V also uses 'fid'
    }

    if vm_type not in vm_type_field:
        print(f"Unsupported VM type: {vm_type}")
        return None, None

    vm_query_config = vm_type_field[vm_type]

    sla_query = f'''
    query GetSlaDomain {{
        {vm_query_config["query"]}({vm_query_config["id_field"]}: "{vm_id}") {{
            effectiveSlaDomain {{
                id
                name
            }}
        }}
    }}
    '''

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.post(f'{rubrik_base_url}/api/graphql', headers=headers, json={"query": sla_query})
    if response.status_code == 200:
        data = response.json()
        vm_query_field = vm_query_config["query"]
        sla_domain = data['data'][vm_query_field]['effectiveSlaDomain']
        return sla_domain['id'], sla_domain['name']
    else:
        print(f"Error retrieving SLA Domain ID: {response.status_code} - {response.text}")
        return None, None

def take_snapshot(access_token, rubrik_base_url, vm_id, vm_sla_id, vm_type, vm_name, sla_name):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }

    if vm_type == "VMware":
        snapshot_mutation = """
            mutation vsphereBulkOnDemandSnapshot($vms: [String!]!, $slaId: String!) {
              vsphereBulkOnDemandSnapshot(
                input: {
                  config: {
                    vms: $vms,
                    slaId: $slaId
                  }
                }
              ) {
                responses {
                  id
                }
              }
            }
        """
        variables = {"vms": [vm_id], "slaId": vm_sla_id}
    elif vm_type == "Hyper-V":
        snapshot_mutation = """
            mutation HypervOnDemandSnapshotMutation($input: HypervOnDemandSnapshotInput!) {
                hypervOnDemandSnapshot(input: $input) {
                    status
                    __typename
                }
            }
        """
        variables = {
            "input": {
                "config": {
                    "slaId": vm_sla_id
                },
                "id": vm_id,
                "userNote": ""
            }
        }
    elif vm_type == "Nutanix":
        snapshot_mutation = """
            mutation NutanixAHVSnapshotMutation($input: CreateOnDemandNutanixBackupInput!) {
                createOnDemandNutanixBackup(input: $input) {
                    status
                    __typename
                }
            }
        """
        variables = {
            "input": {
                "config": {
                    "slaId": vm_sla_id
                },
                "id": vm_id,
                "userNote": ""
            }
        }
    else:
        print(f"Unsupported VM type: {vm_type}")
        return

    mutations_endpoint = f'{rubrik_base_url}/api/graphql'
    response = requests.post(mutations_endpoint, headers=headers, json={"query": snapshot_mutation, "variables": variables})

    if response.status_code == 200:
        print(f"Successfully requested snapshot for VM '{vm_name}' (ID: {vm_id}) on {vm_type} with SLA Domain '{sla_name}'.")
    else:
        print(f"Error taking snapshot for VM '{vm_name}' (ID: {vm_id}): {response.status_code} - {response.text}")

def main():
    config_file = 'config.json'
    config = load_config(config_file)
    if config is None:
        return
    rubrik_base_url = config.get('RUBRIK_BASE_URL')
    client_id = config.get('RUBRIK_CLIENT_ID')
    client_secret = config.get('RUBRIK_CLIENT_SECRET')

    if not client_id or not client_secret or not rubrik_base_url:
        print("Error: Missing configuration for client ID, client secret, or base URL.")
        return

    access_token = get_access_token(client_id, client_secret, rubrik_base_url)
    if not access_token:
        return

    vSphere_vms, ahv_vms, hyperv_vms = get_connected_vms(access_token, rubrik_base_url)

    if len(sys.argv) > 1:
        vm_ids = sys.argv[1].split(',')
    else:
        vm_ids = input("Enter the VM IDs (comma separated): ").split(',')

    vm_lists = [vSphere_vms, ahv_vms, hyperv_vms]

    for vm_id in vm_ids:
        vm_id = vm_id.strip()
        vm = get_vm_by_id(vm_id, vm_lists)

        if vm:
            if vm in vSphere_vms:
                vm_type = "VMware"
            elif vm in ahv_vms:
                vm_type = "Nutanix"
            elif vm in hyperv_vms:
                vm_type = "Hyper-V"
            else:
                vm_type = "Unknown"

            print(f"\nProcessing VM ID: {vm_id}")
            print(f"VM Type: {vm_type}")
            print(f"VM Name: {vm['name']}")

            # Retrieve SLA Domain ID and name dynamically
            sla_id, sla_name = get_sla_domain(access_token, rubrik_base_url, vm_id, vm_type)

            if sla_id and sla_name and vm_type != "Unknown":
                take_snapshot(access_token, rubrik_base_url, vm_id, sla_id, vm_type, vm['name'], sla_name)
            else:
                print("Unable to determine the SLA Domain ID or unsupported VM type.")
        else:
            print(f"VM ID '{vm_id}' not found or is invalid.")

if __name__ == '__main__':
    main()