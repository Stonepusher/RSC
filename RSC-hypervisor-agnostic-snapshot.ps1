# Function to load the configuration from a JSON file
function Load-Config {
    param (
        [string]$configFile
    )

    try {
        $config = Get-Content -Path $configFile -Raw | ConvertFrom-Json
        return $config
    } catch {
        Write-Host "Error: Configuration file '$configFile' not found or invalid JSON format." -ForegroundColor Red
        return $null
    }
}

# Function to retrieve the access token
function Get-AccessToken {
    param (
        [string]$client_id,
        [string]$client_secret,
        [string]$rubrik_base_url
    )

    $headers = @{
        'Content-Type' = 'application/x-www-form-urlencoded'
    }
    $auth_endpoint = "$rubrik_base_url/api/client_token"
    $auth_payload = @{
        'client_id'     = $client_id
        'client_secret' = $client_secret
        'grant_type'    = 'client_credentials'
    }
    $response = Invoke-RestMethod -Uri $auth_endpoint -Headers $headers -Method Post -Body $auth_payload
    if ($response.StatusCode -eq 200) {
        return $response.access_token
    } else {
        Write-Host "Error retrieving access token: $($response.StatusCode) - $($response.Content)" -ForegroundColor Red
        return $null
    }
}

# Function to send GraphQL queries
function Query-VMs {
    param (
        [string]$access_token,
        [string]$rubrik_base_url,
        [string]$query,
        $variables
    )

    $headers = @{
        'Accept'        = 'application/json'
        'Content-Type'  = 'application/json'
        'Authorization' = "Bearer $access_token"
    }
    $body = @{
        query     = $query
        variables = $variables
    } | ConvertTo-Json

    $response = Invoke-RestMethod -Uri "$rubrik_base_url/api/graphql" -Headers $headers -Method Post -Body $body

    if ($response.StatusCode -eq 200) {
        return $response
    } else {
        Write-Host "Error retrieving VMs: $($response.StatusCode) - $($response.Content)" -ForegroundColor Red
        return $null
    }
}

# Function to retrieve all pages of data from the GraphQL API
function Retrieve-AllPages {
    param (
        [string]$access_token,
        [string]$rubrik_base_url,
        [string]$query,
        [string]$node_name
    )

    $all_nodes = @()
    $variables = @{}

    do {
        $data = Query-VMs -access_token $access_token -rubrik_base_url $rubrik_base_url -query $query -variables $variables
        if ($null -eq $data) {
            break
        }
        $nodes = $data.data.$node_name.nodes
        $all_nodes += $nodes
        $page_info = $data.data.$node_name.pageInfo
        if ($page_info.hasNextPage) {
            $variables['endCursor'] = $page_info.endCursor
        } else {
            break
        }
    } while ($true)

    return $all_nodes
}

# Function to get all connected VMs for different platforms
function Get-ConnectedVMs {
    param (
        [string]$access_token,
        [string]$rubrik_base_url
    )

    $vSphere_query = @"
        query vspherePagedQuery(\$endCursor: String) {
            vSphereVmNewConnection(filter: [{field: IS_RELIC texts: "false"}, {field: IS_REPLICATED texts: "false"}], after: \$endCursor) {
                nodes {
                    name
                    id
                }
                pageInfo { endCursor hasNextPage }
            }
        }
"@

    $ahv_query = @"
        query ahvPagedQuery(\$endCursor: String) {
            nutanixVms(filter: [{field: IS_RELIC texts: "false"}, {field: IS_REPLICATED texts: "false"}], after: \$endCursor) {
                nodes {
                    name
                    id
                }
                pageInfo { endCursor hasNextPage }
            }
        }
"@

    $hyperv_query = @"
        query hyperVPagedQuery(\$endCursor: String) {
            hypervVirtualMachines(filter: [{field: IS_RELIC texts: "false"}, {field: IS_REPLICATED texts: "false"}], after: \$endCursor) {
                nodes {
                    name
                    id
                }
                pageInfo { endCursor hasNextPage }
            }
        }
"@

    $vSphere_vms = Retrieve-AllPages -access_token $access_token -rubrik_base_url $rubrik_base_url -query $vSphere_query -node_name "vSphereVmNewConnection"
    $ahv_vms = Retrieve-AllPages -access_token $access_token -rubrik_base_url $rubrik_base_url -query $ahv_query -node_name "nutanixVms"
    $hyperv_vms = Retrieve-AllPages -access_token $access_token -rubrik_base_url $rubrik_base_url -query $hyperv_query -node_name "hypervVirtualMachines"

    return @($vSphere_vms, $ahv_vms, $hyperv_vms)
}

# Function to find a VM by ID from a list of VMs
function Get-VMByID {
    param (
        [string]$vm_id,
        $vm_lists
    )

    foreach ($vm_list in $vm_lists) {
        foreach ($vm in $vm_list) {
            if ($vm.id -eq $vm_id) {
                return $vm
            }
        }
    }
    return $null
}

# Function to retrieve the SLA domain for a specific VM by type
function Get-SLADomain {
    param (
        [string]$access_token,
        [string]$rubrik_base_url,
        [string]$vm_id,
        [string]$vm_type
    )

    $vm_type_field = @{
        "VMware"  = @{ "query" = "vSphereVmNew"; "id_field" = "fid" }
        "Nutanix" = @{ "query" = "nutanixVm"; "id_field" = "fid" }
        "Hyper-V" = @{ "query" = "hypervVirtualMachine"; "id_field" = "fid" }
    }

    if (-not $vm_type_field.ContainsKey($vm_type)) {
        Write-Host "Unsupported VM type: $vm_type" -ForegroundColor Red
        return @($null, $null)
    }

    $vm_query_config = $vm_type_field[$vm_type]

    $sla_query = @"
    query GetSlaDomain {
        $($vm_query_config["query"])($($vm_query_config["id_field"]): "$vm_id") {
            effectiveSlaDomain {
                id
                name
            }
        }
    }
"@

    $headers = @{
        'Accept'        = 'application/json'
        'Content-Type'  = 'application/json'
        'Authorization' = "Bearer $access_token"
    }
    $response = Invoke-RestMethod -Uri "$rubrik_base_url/api/graphql" -Headers $headers -Method Post -Body (@{ query = $sla_query } | ConvertTo-Json)

    if ($response.StatusCode -eq 200) {
        $vm_query_field = $vm_query_config["query"]
        $sla_domain = $response.data.$vm_query_field.effectiveSlaDomain
        return @($sla_domain.id, $sla_domain.name)
    } else {
        Write-Host "Error retrieving SLA Domain ID: $($response.StatusCode) - $($response.Content)" -ForegroundColor Red
        return @($null, $null)
    }
}

# Function to take a snapshot of the specified VM
function Take-Snapshot {
    param (
        [string]$access_token,
        [string]$rubrik_base_url,
        [string]$vm_id,
        [string]$vm_sla_id,
        [string]$vm_type,
        [string]$vm_name,
        [string]$sla_name
    )

    $headers = @{
        'Accept'        = 'application/json'
        'Content-Type'  = 'application/json'
        'Authorization' = "Bearer $access_token"
    }

    if ($vm_type -eq "VMware") {
        $snapshot_mutation = @"
            mutation vsphereBulkOnDemandSnapshot(\$vms: [String!]!, \$slaId: String!) {
              vsphereBulkOnDemandSnapshot(
                input: {
                  config: {
                    vms: \$vms,
                    slaId: \$slaId
                  }
                }
              ) {
                responses {
                  id
                }
              }
            }
"@
        $variables = @{ vms = @($vm_id); slaId = $vm_sla_id }
    } elseif ($vm_type -eq "Hyper-V") {
        $snapshot_mutation = @"
            mutation HypervOnDemandSnapshotMutation(\$input: HypervOnDemandSnapshotInput!) {
                hypervOnDemandSnapshot(input: \$input) {
                    status
                    __typename
                }
            }
"@
        $variables = @{
            input = @{
                config   = @{ slaId = $vm_sla_id }
                id       = $vm_id
                userNote = ""
            }
        }
    } elseif ($vm_type -eq "Nutanix") {
        $snapshot_mutation = @"
            mutation NutanixAHVSnapshotMutation(\$input: CreateOnDemandNutanixBackupInput!) {
                createOnDemandNutanixBackup(input: \$input) {
                    status
                    __typename
                }
            }
"@
        $variables = @{
            input = @{
                config   = @{ slaId = $vm_sla_id }
                id       = $vm_id
                userNote = ""
            }
        }
    } else {
        Write-Host "Unsupported VM type: $vm_type" -ForegroundColor Red
        return
    }

    $body = @{
        query     = $snapshot_mutation
        variables = $variables
    } | ConvertTo-Json

    $response = Invoke-RestMethod -Uri "$rubrik_base_url/api/graphql" -Headers $headers -Method Post -Body $body

    if ($response.StatusCode -eq 200) {
        Write-Host "Successfully requested snapshot for VM '$vm_name' (ID: $vm_id) on $vm_type with SLA Domain '$sla_name'." -ForegroundColor Green
    } else {
        Write-Host "Error taking snapshot for VM '$vm_name' (ID: $vm_id): $($response.StatusCode) - $($response.Content)" -ForegroundColor Red
    }
}

# Main function to coordinate the processing
function Main {
    $config_file = "config.json"
    $config = Load-Config -configFile $config_file
    if ($null -eq $config) {
        return
    }

    $rubrik_base_url = $config.RUBRIK_BASE_URL
    $client_id = $config.RUBRIK_CLIENT_ID
    $client_secret = $config.RUBRIK_CLIENT_SECRET

    if (-not $client_id -or -not $client_secret -or -not $rubrik_base_url) {
        Write-Host "Error: Missing configuration for client ID, client secret, or base URL." -ForegroundColor Red
        return
    }

    $access_token = Get-AccessToken -client_id $client_id -client_secret $client_secret -rubrik_base_url $rubrik_base_url
    if ($null -eq $access_token) {
        return
    }

    $vm_lists = Get-ConnectedVMs -access_token $access_token -rubrik_base_url $rubrik_base_url

    [string[]]$vm_ids
    if ($args.Count -gt 0) {
        $vm_ids = $args[0].Split(',')
    } else {
        $vm_ids = Read-Host -Prompt "Enter the VM IDs (comma separated)" -AsSecureString | ConvertFrom-SecureString -AsPlainText | Split-String -Delimiter ','
    }

    foreach ($vm_id in $vm_ids) {
        $vm_id = $vm_id.Trim()
        $vm = Get-VMByID -vm_id $vm_id -vm_lists $vm_lists

        if ($null -ne $vm) {
            if ($vm in $vm_lists[0]) {
                $vm_type = "VMware"
            } elseif ($vm in $vm_lists[1]) {
                $vm_type = "Nutanix"
            } elseif ($vm in $vm_lists[2]) {
                $vm_type = "Hyper-V"
            } else {
                $vm_type = "Unknown"
            }

            Write-Host "`nProcessing VM ID: $vm_id"
            Write-Host "VM Type: $vm_type"
            Write-Host "VM Name: $($vm.name)"

            $sla_id, $sla_name = Get-SLADomain -access_token $access_token -rubrik_base_url $rubrik_base_url -vm_id $vm_id -vm_type $vm_type

            if ($null -ne $sla_id -and $null -ne $sla_name -and $vm_type -ne "Unknown") {
                Take-Snapshot -access_token $access_token -rubrik_base_url $rubrik_base_url -vm_id $vm_id -vm_sla_id $sla_id -vm_type $vm_type -vm_name $vm.name -sla_name $sla_name
            } else {
                Write-Host "Unable to determine the SLA Domain ID or unsupported VM type." -ForegroundColor Red
            }
        } else {
            Write-Host "VM ID '$vm_id' not found or is invalid." -ForegroundColor Red
        }
    }
}

Main