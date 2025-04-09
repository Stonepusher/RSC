import json
import requests
import sys

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

def get_vm_details(access_token, rubrik_base_url, vm_id):
    """
    Retrieve details for a specific VM, including the assigned SLA Domain ID.

    Parameters:
    access_token (str): The access token for authorization.
    rubrik_base_url (str): The base URL of the Rubrik Security Cloud.
    vm_id (str): The VM ID for which to retrieve details.

    Returns:
    dict: The VM details if successful, None otherwise.
    """
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }

    query = {
        "query": """
            query GetSpecificVMDetail($fid: UUID!) {
              vSphereVmNew(fid: $fid) {
                name
                id
                cdmId
                effectiveSlaDomain {
                  name
                  id
                }
                guestCredentialAuthorizationStatus
                objectType
                powerStatus
                slaAssignment
                snapshotConsistencyMandate
                blueprintId
                guestCredentialId
                guestOsName
                isActive
                isArrayIntegrationPossible
                isBlueprintChild
                isRelic
                numWorkloadDescendants
                slaPauseStatus
                agentStatus {
                  agentStatus
                }
                allOrgs {
                  id
                  name
                }
                cluster {
                  id
                  name
                }
              }
            }
        """,
        "variables": {
            "fid": vm_id
        }
    }

    try:
        response = requests.post(f'{rubrik_base_url}/api/graphql', headers=headers, json=query)
        response.raise_for_status()  # Raise an HTTPError on bad responses

        data = response.json()
        vm_details = data.get('data', {}).get('vSphereVmNew', {})
        return vm_details
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving details for VM ID: {vm_id} - {str(e)}")
        return None

def take_vm_snapshot(access_token, rubrik_base_url, vm_id, sla_id):
    """
    Trigger a snapshot for the specified VM.

    Parameters:
    access_token (str): The access token for authorization.
    rubrik_base_url (str): The base URL of the Rubrik Security Cloud.
    vm_id (str): The VM ID for which to take a snapshot.
    sla_id (str): The SLA Domain ID to be used for the snapshot.

    Returns:
    bool: True if the snapshot was successfully triggered, False otherwise.
    """
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }

    mutation = {
        "query": """
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
        """,
        "variables": {
            "vms": [vm_id],
            "slaId": sla_id
        }
    }

    try:
        response = requests.post(f'{rubrik_base_url}/api/graphql', headers=headers, json=mutation)
        response.raise_for_status()  # Raise an HTTPError on bad responses

        data = response.json()
        snapshot_responses = data.get('data', {}).get('vsphereBulkOnDemandSnapshot', {}).get('responses', [])
        if snapshot_responses:
            print(f"Snapshot successfully triggered for VM ID: {vm_id}, Snapshot ID: {snapshot_responses[0]['id']}")
            return True
        else:
            print(f"No responses received when triggering snapshot for VM ID: {vm_id}.")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error triggering snapshot for VM ID {vm_id} - {str(e)}")
        return False

def main():
    """
    Main function to take on-demand snapshots of specified VMs from the Rubrik Security Cloud.
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

    # Check if VM IDs are provided as command-line arguments
    if len(sys.argv) > 1:
        vm_ids = sys.argv[1:]
    else:
        vm_ids = input("Enter the VM IDs to snapshot (comma-separated): ").split(",")
        vm_ids = [vm_id.strip() for vm_id in vm_ids if vm_id.strip()]  # Clean and filter out empty IDs

    if not vm_ids:
        print("No VM IDs provided.")
        return

    for vm_id in vm_ids:
        vm_details = get_vm_details(access_token, rubrik_base_url, vm_id)
        if vm_details:
            vm_name = vm_details['name']
            sla_id = vm_details['effectiveSlaDomain']['id']
            sla_name = vm_details['effectiveSlaDomain']['name']
            print(f"Taking snapshot for VM: {vm_name} (ID: {vm_id}) using SLA Domain: {sla_name} (ID: {sla_id})")
            if not take_vm_snapshot(access_token, rubrik_base_url, vm_id, sla_id):
                print(f"Failed to take snapshot for VM: {vm_name} (ID: {vm_id})")
        else:
            print(f"Failed to retrieve details for VM ID: {vm_id}")

if __name__ == '__main__':
    main()