import json
import requests
import sys
import time
from datetime import datetime

# Load config parameters from config.json
def load_config():
    try:
        with open('config.json') as f:
            return json.load(f)
    except Exception as e:
        sys.exit(f"Error loading config: {e}")

def get_auth_token(client_id, client_secret, base_url):
    try:
        url = f"{base_url}/api/client_token"
        response = requests.post(url, json={"client_id": client_id, "client_secret": client_secret})
        response.raise_for_status()
        return response.json()['access_token']
    except Exception as err:
        sys.exit(f"Auth failed: {err} - {response.text}")

def graphql_query(token, base_url, query, variables=None):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(f"{base_url}/api/graphql", 
                             json={"query": query, "variables": variables or {}}, 
                             headers=headers)
    try:
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            sys.exit(f"GraphQL errors: {result['errors']}")
        return result["data"]
    except requests.exceptions.RequestException as e:
        sys.exit(f"GraphQL request error: {e}, response: {response.text}")

def get_protected_oracle_dbs(token, base_url):
    query = '''
    query {
      oracleDatabases(filter:[
        {field:IS_RELIC texts:"false"},
        {field:IS_REPLICATED texts:"false"}
      ]) {
        nodes {
          id
          name
          effectiveSlaDomain{id name}
          cluster{id name}
        }
      }
    }'''
    data = graphql_query(token, base_url, query)
    return [db for db in data['oracleDatabases']['nodes'] if db.get('effectiveSlaDomain') and db.get('cluster')]

def get_latest_oracle_snapshot(token, base_url, oracle_db_id):
    query = '''
    query($fid: UUID!){
      oracleDatabase(fid:$fid){
        newestSnapshot{id date isExpired isQuarantined}
      }
    }'''
    data=graphql_query(token,base_url,query,{"fid": oracle_db_id})['oracleDatabase']['newestSnapshot']
    if not data or data['isExpired'] or data['isQuarantined']:
        sys.exit("No valid Oracle snapshot found.")
    return data

def get_hosts_for_cluster(token, base_url, cluster_id):
    query='''
    query {
      oracleTopLevelDescendants(filter:[
        {field:IS_RELIC texts:"false"}
        {field:IS_REPLICATED texts:"false"}
      ]){
        nodes{
          id name objectType cluster{id name}
        }
      }
    }'''
    data=graphql_query(token, base_url, query)
    hosts=[h for h in data['oracleTopLevelDescendants']['nodes']
           if h.get('cluster', {}).get('id')==cluster_id
           and h['objectType'] in ["OracleHost","OracleRac"]]
    return hosts

def validate_oracle_db_backup(token, base_url, oracle_db_id, snapshot_id, host_id):
    mutation='''
    mutation($input:ValidateOracleDatabaseBackupsInput!){
      validateOracleDatabaseBackups(input:$input){
        id links{href rel __typename}
      }
    }'''
    variables={
        "input":{
            "id":oracle_db_id,
            "config":{
                "targetOracleHostOrRacId":host_id,
                "recoveryPoint":{"snapshotId":snapshot_id,"scn":None}
            }
        }
    }
    return graphql_query(token,base_url,mutation,variables)['validateOracleDatabaseBackups']

def wait_for_oracle_job(token, base_url, job_id, cluster_uuid):
    query = '''
    query($id: String!, $clusterUuid: String!) {
      oracleDatabaseAsyncRequestDetails(input:{id:$id, clusterUuid:$clusterUuid}) {
        progress
        status
        result
        error { message }
      }
    }'''
    variables = {
        "id": job_id,
        "clusterUuid": cluster_uuid
    }
    
    while True:
        data = graphql_query(token, base_url, query, variables)
        job_details = data['oracleDatabaseAsyncRequestDetails']
        status = job_details.get('status')
        progress = job_details.get('progress', 0)

        print(f"[{datetime.now()}] Status: {status}, Progress: {progress}%")

        if status == "SUCCEEDED":
            print("Oracle DB backup validation succeeded.")
            return 0
        elif status == "FAILED":
            error_msg = job_details.get('error', {}).get('message', 'Unknown error')
            sys.exit(f"Oracle DB backup validation failed: {error_msg}")

        time.sleep(15)

def main():
    config=load_config()
    token=get_auth_token(config['RUBRIK_CLIENT_ID'],config['RUBRIK_CLIENT_SECRET'],config['RUBRIK_BASE_URL'])

    choice=input("Validation Type:\n1) Oracle DB\n2) Azure/Hyper-V VM\nSelect 1 or 2:").strip()

    if choice=="1":
        oracle_dbs=get_protected_oracle_dbs(token,config['RUBRIK_BASE_URL'])
        if not oracle_dbs:
            sys.exit("No Oracle databases found.")
        print("\nOracle DBs:")
        for idx,db in enumerate(oracle_dbs,1):
            print(f"{idx}){db['name']} (Cluster:{db['cluster']['name']})")
        db_choice=int(input("Select DB:"))-1
        selected_db=oracle_dbs[db_choice]

        snapshot=get_latest_oracle_snapshot(token,config['RUBRIK_BASE_URL'],selected_db['id'])
        matching_hosts=get_hosts_for_cluster(token,config['RUBRIK_BASE_URL'],selected_db['cluster']['id'])

        if not matching_hosts:
            sys.exit("No matching Oracle hosts for cluster.")
        print("\nAvailable Hosts:")
        for idx,h in enumerate(matching_hosts,1):
            print(f"{idx}){h['name']}[{h['objectType']}]")
        host_choice=int(input("Select Host:"))-1
        selected_host_id=matching_hosts[host_choice]['id']

        validate_result=validate_oracle_db_backup(token,config['RUBRIK_BASE_URL'],selected_db['id'],snapshot['id'],selected_host_id)
        job_id=validate_result['id']
        cluster_uuid=selected_db['cluster']['id']

        return wait_for_oracle_job(token,config['RUBRIK_BASE_URL'],job_id,cluster_uuid)

    elif choice=="2":
        print("Azure VM mount logic goes here (restore previous working logic)")
        return 0
    else:
        sys.exit("Invalid choice.")

if __name__=="__main__":
    sys.exit(main())