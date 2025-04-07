import json
import requests
import csv

def load_config(config_file):
    """
    Load configuration from a JSON file.
    
    Parameters:
    config_file (str): The path to the configuration file.
    
    Returns:
    dict: Configuration key-value pairs or None if the file is not found or invalid.
    """
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
    """
    Retrieve the access token from the Rubrik Security Cloud.

    Parameters:
    client_id (str): The client ID.
    client_secret (str): The client secret.
    rubrik_base_url (str): The base URL of the Rubrik Security Cloud.

    Returns:
    str: The access token if successful, None otherwise.
    """
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

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
    """
    Perform a GraphQL query to retrieve VMs.

    Parameters:
    access_token (str): The access token for authorization.
    rubrik_base_url (str): The base URL of the Rubrik Security Cloud.
    query (str): The GraphQL query.
    variables (dict): The GraphQL query variables.

    Returns:
    dict: The response data if successful, None otherwise.
    """
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
    """
    Retrieve all pages of a paginated GraphQL query result.

    Parameters:
    access_token (str): The access token for authorization.
    rubrik_base_url (str): The base URL of the Rubrik Security Cloud.
    query (str): The GraphQL query.
    node_name (str): The node name in the query result that contains the data.

    Returns:
    list: The combined list of nodes from all pages.
    """
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
    """
    Retrieve the list of connected vSphere, AHV, and Hyper-V VMs from the Rubrik Security Cloud.

    Parameters:
    access_token (str): The access token for authorization.
    rubrik_base_url (str): The base URL of the Rubrik Security Cloud.

    Returns:
    tuple: Three lists of vSphere, AHV, and Hyper-V VMs if successful, None otherwise.
    """
    vSphere_query = """
        query vspherePagedQuery($endCursor: String) {
            vSphereVmNewConnection(
                filter: [
                    {field: IS_RELIC texts: "false"},
                    {field: IS_REPLICATED texts: "false"}
                ],
                after: $endCursor
            ) {
                nodes {
                    name
                    id
                    guestOsName
                    agentStatus {
                        agentStatus
                    }
                    cluster {
                        name
                    }
                    effectiveSlaDomain {
                        name
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
    """

    ahv_query = """
        query ahvPagedQuery($endCursor: String) {
            nutanixVms(
                filter: [
                    {field: IS_RELIC texts: "false"},
                    {field: IS_REPLICATED texts: "false"}
                ],
                after: $endCursor
            ) {
                nodes {
                    name
                    id
                    osType
                    agentStatus {
                        connectionStatus
                    }
                    cluster {
                        name
                    }
                    effectiveSlaDomain {
                        name
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
    """
    
    hyperv_query = """
        query hyperVPagedQuery($endCursor: String) {
            hypervVirtualMachines(
                filter: [
                    {field: IS_RELIC texts: "false"},
                    {field: IS_REPLICATED texts: "false"}
                ],
                after: $endCursor
            ) {
                nodes {
                    name
                    id
                    osType
                    agentStatus {
                        connectionStatus
                        disconnectReason
                    }
                    cluster {
                        name
                        id
                    }
                    effectiveSlaDomain {
                        name
                        id
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
    """

    vSphere_vms = retrieve_all_pages(access_token, rubrik_base_url, vSphere_query, 'vSphereVmNewConnection')
    ahv_vms = retrieve_all_pages(access_token, rubrik_base_url, ahv_query, 'nutanixVms')
    hyperv_vms = retrieve_all_pages(access_token, rubrik_base_url, hyperv_query, 'hypervVirtualMachines')

    return vSphere_vms, ahv_vms, hyperv_vms

def write_vms_to_csv(vSphere_vms, ahv_vms, hyperv_vms, filename):
    """
    Write the list of vSphere, AHV, and Hyper-V VMs to a CSV file.
    
    Parameters:
    vSphere_vms (list): List of vSphere VMs.
    ahv_vms (list): List of AHV VMs.
    hyperv_vms (list): List of Hyper-V VMs.
    filename (str): The name of the CSV file to write to.
    """
    headers = ["VM Name", "VM ID", "OS", "Host", "RBS Agent Status", "Cluster", "SLA Domain", "Type"]

    with open(filename, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)

        def write_vm(vm, vm_type):
            agent_status = (vm['agentStatus']['agentStatus'] if vm_type == 'vSphere' and vm.get('agentStatus')
                            else vm['agentStatus']['connectionStatus'] if vm.get('agentStatus') else 'Unknown')
            cluster_name = vm['cluster']['name'] if vm.get('cluster') else 'Unknown'
            os_type = vm['guestOsName'] if vm_type == 'vSphere' else vm.get('osType', 'Unknown')
            host_name = vm.get('hostName', 'Unknown')
            sla_domain = vm['effectiveSlaDomain']['name'] if vm.get('effectiveSlaDomain') and vm['effectiveSlaDomain'].get('name') else 'Unknown'
            writer.writerow([vm['name'], vm['id'], os_type, host_name, agent_status, cluster_name, sla_domain, vm_type])

        for vm in vSphere_vms:
            write_vm(vm, "vSphere")
        
        for vm in ahv_vms:
            write_vm(vm, "AHV")
        
        for vm in hyperv_vms:
            write_vm(vm, "Hyper-V")

def main():
    """
    Main function to load configuration, retrieve access token, and fetch the list
    of connected VMs from the Rubrik Security Cloud including their OS Type, RBS Agent status, and Rubrik Cluster which is protecting them.
    Also writes the VM information to a CSV file.
    """
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

    print(f"Using Client ID: {client_id}")
    print(f"Using Client Secret: {client_secret}")

    access_token = get_access_token(client_id, client_secret, rubrik_base_url)

    if not access_token:
        print("Failed to retrieve access token.")
        return

    print(f"Using Access Token: {access_token}")

    vSphere_vms, ahv_vms, hyperv_vms = get_connected_vms(access_token, rubrik_base_url)

    # Process and print vSphere VMs
    if vSphere_vms is not None:
        vSphere_count = len(vSphere_vms)
        print(f"Total VMware VMs found: {vSphere_count}")
        print()  # Blank line for readability
        for vm in vSphere_vms:
            agent_status = vm['agentStatus']['agentStatus'] if vm.get('agentStatus') else 'Unknown'
            cluster_name = vm['cluster']['name'] if vm.get('cluster') else 'Unknown'
            sla_domain = vm['effectiveSlaDomain']['name'] if vm.get('effectiveSlaDomain') and vm['effectiveSlaDomain'].get('name') else 'Unknown'
            print(f"VM Name: {vm['name']}, VM ID: {vm['id']}, OS: {vm['guestOsName']}, RBS Agent: {agent_status}, Cluster: {cluster_name}, SLA Domain: {sla_domain}")
    
    # Process and print AHV VMs
    if ahv_vms is not None:
        ahv_count = len(ahv_vms)
        print(f"\nTotal AHV VMs found: {ahv_count}")
        print()  # Blank line for readability
        for vm in ahv_vms:
            agent_status = vm['agentStatus']['connectionStatus'] if vm.get('agentStatus') else 'Unknown'
            cluster_name = vm['cluster']['name'] if vm.get('cluster') else 'Unknown'
            os_type = vm.get('osType', 'Unknown')
            sla_domain = vm['effectiveSlaDomain']['name'] if vm.get('effectiveSlaDomain') and vm['effectiveSlaDomain'].get('name') else 'Unknown'
            print(f"VM Name: {vm['name']}, VM ID: {vm['id']}, OS: {os_type}, RBS Agent: {agent_status}, Cluster: {cluster_name}, SLA Domain: {sla_domain}")
            
    # Process and print Hyper-V VMs
    if hyperv_vms is not None:
        hyperv_count = len(hyperv_vms)
        print(f"\nTotal Hyper-V VMs found: {hyperv_count}")
        print()  # Blank line for readability
        for vm in hyperv_vms:
            agent_status = vm['agentStatus']['connectionStatus'] if vm.get('agentStatus') else 'Unknown'
            cluster_name = vm['cluster']['name'] if vm.get('cluster') else 'Unknown'
            os_type = vm.get('osType', 'Unknown')
            sla_domain = vm['effectiveSlaDomain']['name'] if vm.get('effectiveSlaDomain') and vm['effectiveSlaDomain'].get('name') else 'Unknown'
            print(f"VM Name: {vm['name']}, VM ID: {vm['id']}, OS: {os_type}, RBS Agent: {agent_status}, Cluster: {cluster_name}, SLA Domain: {sla_domain}")

    # Write VMs to CSV
    csv_filename = 'rubrik_vms.csv'
    write_vms_to_csv(vSphere_vms, ahv_vms, hyperv_vms, csv_filename)
    print(f"\nVM information written to CSV file: {csv_filename}")

if __name__ == '__main__':
    main()