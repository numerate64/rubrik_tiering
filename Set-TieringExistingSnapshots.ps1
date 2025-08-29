<#
.SYNOPSIS
    Set tiering on existing NAS DA Snapshots
.NOTES
    Created: November - 2024
    Updated: August 2025
    Author: Rubrik PSO
.LINK
    Rubrik Automation Page: developer.rubrik.com
.EXAMPLE
    Example
#>
#Requires -Version 7.0
[cmdletbinding()]
param(
    [Parameter(Mandatory = $true)][string]$serviceAccountFile,
    [Parameter(Mandatory = $true)][string]$clusterName,
    [string]$archivalLocationName,
    [string]$nasShareName,
    [string]$retentionSLAOverride
)

function Write-Log() {
    param (
        $message,
        [switch]$isError,
        [switch]$isSuccess,
        [switch]$isWarning
    )
    $color = 'Blue'
    if($isError){
        $message = 'ERROR: ' + $message
        $color = 'red'
    } elseif($isSuccess){
        $message = 'SUCCESS: ' + $message
        $color = 'green'
    } elseif($isWarning){
        $message = 'WARNING: ' + $message
        $color = 'yellow'
    }
    $message = "$(get-date) $message"
    Write-Host("$message$($PSStyle.Reset)") -BackgroundColor $color
    $message | out-file Set-TieringExistingSnapshots_log.txt -append
    if($isError){
        exit
    }   
}

Connect-Rsc

$serviceAccountJson = Get-Content $serviceAccountFile | convertfrom-json

$token=$RscConnectionClient.AccessToken

#Create connection object for all subsequent calls with bearer token
$connection = @{
    headers = @{
        'Content-Type'  = 'application/json';
        'Accept'        = 'application/json';
        'Authorization' = 'Bearer ' + $token;
    }
    endpoint = $serviceAccountJson.access_token_uri.Replace('/api/client_token', '/api/graphql')
}
#End brokering to RSC
Write-Log -message 'Authentication to RSC succeeded'
$global:connection = $connection
$rsc=$connection

function Get-Nasshare([object]$cluster, [string]$sharename) {
    # Create an RscQuery object for:
    # API Domain:    Nas
    # API Operation: Shares
    
    $query = New-RscQueryNas -Operation Shares -AddField Nodes.PrimaryFileset.SnapshotConnection,Nodes.DescendantConnection.Nodes.SlaAssignment,Nodes.Cluster.id,Nodes.PrimaryFileset.ConfiguredSlaDomain
    
    # OPTIONAL
    $query.Var.first = 50
    # OPTIONAL
    $query.Var.filter = @(
        @{
            # OPTIONAL
            field = "CLUSTER_ID" # Call [Enum]::GetValues([RubrikSecurityCloud.Types.HierarchyFilterField]) for enum values.
            # OPTIONAL
            texts = @($cluster.id)
        }
        @{
            # OPTIONAL
            field = "NAME" # Call [Enum]::GetValues([RubrikSecurityCloud.Types.HierarchyFilterField]) for enum values.
            # OPTIONAL
            texts = @($sharename)
        }
    )
    
    # Execute the query
    
    $result = $query | Invoke-Rsc
    
    return $result.nodes
}

function Get-Nasshares([object]$cluster) {
    $payload = @{
        query = 'query NasShares($filter: [Filter!]) {
                nasShares(filter: $filter) {
                    nodes {
                    id
                    name
                    shareType
                    primaryFileset {
                        id
                    }
                    effectiveSlaDomain {
                        name
                        id
                        
                    }
                    }
                }
            }'
        variables = @{
            filter = @(
                @{
                    field = "CLUSTER_ID"
                    texts = $cluster.id
                }
            )            
        }
    }
    $response = (Invoke-RestMethod -Method POST -Uri $rsc.endpoint -Body $($payload | ConvertTo-JSON -Depth 100) -Headers $rsc.headers).data.nasShares.nodes
    return $response
}

function Get-ArchivalLocations() {
    $payload = @{
        query = 'query Nodes($filter: [TargetFilterInput!]) {
            targets(filter: $filter) {
                nodes {
                id
                name
                }
            }
        }'
        variables = @{

        }
    }
    $response = (Invoke-RestMethod -Method POST -Uri $rsc.endpoint -Body $($payload | ConvertTo-JSON -Depth 100) -Headers $rsc.headers).data.targets.nodes
    return $response
}

function Set-ObjectTiering([string]$clusterUuid, [string]$archivalLocationId, [string[]] $objectid) {
    $payload = @{
        query = 'mutation BulkTierExistingSnapshots($input: BulkTierExistingSnapshotsInput!) {
            bulkTierExistingSnapshots(input: $input) {
                endTime
                id
                nodeId
                progress
                startTime
                status
                error {
                message
                }
            }
        }'
        variables = @{
            input = @{
                clusterUuid = $clusterUuid
                objectTierInfo = @{
                    objectIds = @($objectId)
                }
            }
        }
    }
    $response = (Invoke-RestMethod -Method POST -Uri $rsc.endpoint -Body $($payload | ConvertTo-JSON -Depth 100) -Headers $rsc.headers)
    return $response
}

function Set-SnapshotRetention([string]$slaId,[string]$snapshotId){
    # Create an RscQuery object for:
    # API Domain:    Sla
    # API Operation: AssignRetentionToSnapshots
    
    $query = New-RscMutationSla -Operation AssignRetentionToSnapshots
    
    # OPTIONAL
    $query.Var.globalSlaOptionalFid = $slaId
    # REQUIRED
    $query.Var.globalSlaAssignType = "PROTECT_WITH_SLA_ID"
    # REQUIRED
    $query.Var.snapshotFids = @($snapshotId)
    # OPTIONAL
    $query.Var.userNote = $someString
    
    # Execute the query
    
    $result = $query | Invoke-Rsc
    
    return $result
}

function get-sla([String]$SLAName){
$payload = @{
        query = 'query QuerySlaDomains($first: Int,$after: String,$last: Int,$before: String,$sortBy: SlaQuerySortByField,$sortOrder: SortOrder,$filter: [GlobalSlaFilterInput!],$contextFilter: ContextFilterTypeEnum,$contextFilterInput:
            [ContextFilterInputField!],$shouldShowSyncStatus: Boolean,$shouldShowProtectedObjectCount: Boolean,$shouldShowUpgradeInfo: Boolean,$showRemoteSlas: Boolean,$shouldShowPausedClusters: Boolean) {
            slaDomains   (
                first: $first
                after: $after
                last: $last
                before: $before
                sortBy: $sortBy  
                    sortOrder: $sortOrder
                filter: $filter
                contextFilter: $contextFilter
                contextFilterInput: $contextFilterInput
                shouldShowSyncStatus: $shouldShowSyncStatus
                shouldShowProtectedObjectCount: $shouldShowProtectedObjectCount
                shouldShowUpgradeInfo: $shouldShowUpgradeInfo
                showRemoteSlas: $showRemoteSlas
                shouldShowPausedClusters: $shouldShowPausedClusters
            )   {
                nodes {
                ... on ClusterSlaDomain {
                    cdmId
                    fid
                    id
                    name
                    polarisManagedId
                    archivalSpecs {
                    archivalLocationType
                    frequencies
                    thresholdUnit
                    archivalLocationId
                    archivalLocationName
                    threshold
                    archivalTieringSpec {
                        coldStorageClass
                        isInstantTieringEnabled
                        minAccessibleDurationInSeconds
                        shouldTierExistingSnapshots
                    }
                    }
                }
                ... on GlobalSlaReply {
                    clusterUuid
                    description
                    id
                    isArchived
                    isDefault
                    isReadOnly
                    isRetentionLockedSla
                    name
                    stateVersion
                    version
                    objectTypes
                    snapshotSchedule{
                    minute{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    hourly{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    daily{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    weekly{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    monthly{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    yearly{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    quarterly{
                        basicSchedule{
                        frequency
                        retention
                        retentionUnit
                        }
                    }
                    }
                    archivalSpecs {
                    archivalLocationToClusterMapping{
                        cluster{
                        id
                        name
                        }
                        location{
                        id
                        name
                        targetType
                        }
                    }
                    frequencies
                    thresholdUnit
                    threshold
                    archivalTieringSpec {
                        coldStorageClass
                        isInstantTieringEnabled
                        minAccessibleDurationInSeconds
                        shouldTierExistingSnapshots
                        __typename
                    }
                    __typename
                    }
                    __typename
                }
                __typename
                }
                count
                pageInfo
                {
                endCursor
                hasNextPage
                hasPreviousPage
                startCursor
                __typename
                }
                __typename
            }
            __typename
            }'
        variables = @{
            filter = @(
                @{
                    field="NAME"
                    text=$SLAName
                }
            )
            first=50
        }
    } 
    $response = (Invoke-Rsc -GqlQuery $payload.query -Var $payload.variables)
    return $response.nodes

}

$thisCluster=Get-RscCluster -Name $clusterName
if($null -eq $thisCluster){
    Write-Log -isError ('Could not find cluster, check spelling and permissions')
} else {
    Write-Log ('Found cluster {0} with id {1}' -f $thisCluster.name, $thiscluster.id)
}

# Add check if the share name param is set, if not discover all passthrough shares and iterate

if($nasShareName){
    $thisShare = Get-Nasshare -cluster $thisCluster -sharename $nasShareName
    foreach ($share in $thisShare){
        if ($share.cluster.id -eq $thisCluster.id){
            $thisShare=$share
            break
        }
        else{
            $thisShare=$null
        }
    }
    if($null -eq $thisShare){
        Write-Log -isError ('Could not find share, check spelling and permissions')
    } else {
        Write-Log ('Found share {0} with id {1}' -f $thisshare.name, $thisshare.id)
    }
    
    $thisArchive = Get-ArchivalLocations | Where-Object name -eq $archivalLocationName
    if($null -eq $thisArchive){
        Write-Log -isError ('Could not find Archival location, check spelling and permissions')
    } else {
        Write-Log ('Found Archival Location {0} with id {1}' -f $thisArchive.name, $thisArchive.id)
    }
    # Check how many filesets are found on the share
    if ($thisShare.DescendantConnection.nodes.count -gt 1){
        Write-Log -isWarning "There are {0} filesets found on share {1}, please narrow to 1." -f $thisShare.DescendantConnection.nodes.count, $thisshare.name
    }
    elseif ($thisShare.DescendantConnection.nodes.count -lt 1) {
        Write-Log -isError "There are {0} filesets found on share {1}, no objects to tier." -f $thisShare.DescendantConnection.nodes.count, $thisshare.name
    }

    # Check if primary fileset is protected
    if ($thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Name -ne "DO_NOT_PROTECT"){
        Write-Log ("Primary Fileset has SLA named {0} with ID {1}" -f $thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Name, $thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Id)
        $existingSLAID=$thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Id
        $existingSLAName=$thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Name
        Write-Log "Updating SLA to DO_NOT_PROTECT and marking existing snapshots as KEEP_FOREVER."
        Protect-RscWorkload -AssignmentType DO_NOT_PROTECT -ExistingSnapshotAction "KEEP_FOREVER" -Id $thisShare.primaryFileset.id
        $protectionUpdated=$true
    }
    else {
        Write-Log ("Primary Fileset has already been marked as 'DO_NOT_PROTECT'")
        $existingSLAID=$thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Id
        $existingSLAName=$thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Name
        Write-Log "Marking existing snapshots as KEEP_FOREVER in order to ensure existing snaps are able to tier."
        Protect-RscWorkload -AssignmentType DO_NOT_PROTECT -ExistingSnapshotAction "KEEP_FOREVER" -Id $thisShare.primaryFileset.id
        $protectionUpdated=$false
    }

    # Add step to check tiering level of SLA and update to use Glacier Deep Archive
    $slaAlreadyTiering=$false
    $sla=(get-sla -slaName $existingSLAName | where name -eq $existingSLAName)
    if ($null -eq $sla.ArchivalSpecs.ArchivalTieringSpec){
        $message="SLA {0} has an Archive location in {1}, but has no tiering spec set." -f $sla.name,$sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.TargetType
        Write-Log $message
        Write-Log "Updating SLA to tier to AWS_GLACIER_DEEP_ARCHIVE"
            $archivalSpec = New-RscSlaArchivalSpecs -ArchivalThreshold 0 -ArchivalThresholdUnit MINUTES -Frequencies @('DAYS') -InstantTiering -ColdStorageClass AWS_GLACIER_DEEP_ARCHIVE -LocationIds @($sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.Id) -ClusterUuids @($thisCluster.id)
            Set-RscSla -Sla $sla -ArchivalSpecs @($archivalSpec) -UserNote "Temp update to include archive tiering" -ObjectTypes @($sla.objectTypes)`
            -MinuteSchedule $(if($null -ne $sla.snapshotSchedule.Minute) {(New-Object -TypeName RubrikSecurityCloud.Types.MinuteSnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Minute.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Minute.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Minute.BasicSchedule.retentionUnit })})}) `
            -DailySchedule $(if($null -ne $sla.snapshotSchedule.Daily) {(New-Object -TypeName RubrikSecurityCloud.Types.DailySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Daily.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Daily.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Daily.BasicSchedule.retentionUnit })}) })`
            -HourlySchedule $(if($null -ne $sla.snapshotSchedule.Hourly) {(New-Object -TypeName RubrikSecurityCloud.Types.HourlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Hourly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Hourly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Hourly.BasicSchedule.retentionUnit })}) })`
            -WeeklySchedule $(if($null -ne $sla.snapshotSchedule.Weekly) {(New-Object -TypeName RubrikSecurityCloud.Types.WeeklySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Weekly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Weekly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Weekly.BasicSchedule.retentionUnit })}) })`
            -MonthlySchedule $(if($null -ne $sla.snapshotSchedule.Monthly) {(New-Object -TypeName RubrikSecurityCloud.Types.MonthlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Monthly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Monthly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Monthly.BasicSchedule.retentionUnit })}) })`
            -QuarterlySchedule $(if($null -ne $sla.snapshotSchedule.Quarterly) {(New-Object -TypeName RubrikSecurityCloud.Types.QuarterlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Quarterly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Quarterly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Quarterly.BasicSchedule.retentionUnit })}) })`
            -YearlySchedule $(if($null -ne $sla.snapshotSchedule.Yearly) {(New-Object -TypeName RubrikSecurityCloud.Types.YearlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Yearly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Yearly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Yearly.BasicSchedule.retentionUnit })}) })
    }
    else {
        $message="SLA {0} has an Archive location in {1}, and tiering spec set to tier to {2} with InstantTiering set to {3}." -f $sla.name,$sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.TargetType,$sla.ArchivalSpecs.ArchivalTieringSpec.coldStorageClass,$sla.ArchivalSpecs.ArchivalTieringSpec.isInstantTieringEnabled
        $slaAlreadyTiering=$true
        Write-Log $message
    }
    $message="SLA {0} is set to archive after {1} {2}." -f $sla.name,$sla.ArchivalSpecs.threshold, $sla.ArchivalSpecs.thresholdUnit
    Write-Log $message

    Write-Log('Starting tier job of {0}' -f $thisShare.name)
    Set-ObjectTiering -clusterUuid $thisCluster.id -archivalLocationId $thisArchive.id -objectid $thisShare.DescendantConnection.Nodes.id

    # Add step to check tiering level of SLA and update back to Standard
    $sla=(get-sla -slaName $existingSLAName | where name -eq $existingSLAName)
    if (!$slaAlreadyTiering){
        $message="Updating SLA {0} back to non-tiering." -f $sla.name
        Write-Log $message
        $archivalSpec = New-RscSlaArchivalSpecs -ArchivalThreshold 0 -ArchivalThresholdUnit MINUTES -Frequencies @('DAYS') -LocationIds @($sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.Id) -ClusterUuids @($thisCluster.id)
        Set-RscSla -Sla $sla -ArchivalSpecs @($archivalSpec) -UserNote "Temp update to remove archive tiering" -ObjectTypes @($sla.objectTypes)`
            -MinuteSchedule $(if($null -ne $sla.snapshotSchedule.Minute) {(New-Object -TypeName RubrikSecurityCloud.Types.MinuteSnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Minute.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Minute.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Minute.BasicSchedule.retentionUnit })})}) `
            -DailySchedule $(if($null -ne $sla.snapshotSchedule.Daily) {(New-Object -TypeName RubrikSecurityCloud.Types.DailySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Daily.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Daily.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Daily.BasicSchedule.retentionUnit })}) })`
            -HourlySchedule $(if($null -ne $sla.snapshotSchedule.Hourly) {(New-Object -TypeName RubrikSecurityCloud.Types.HourlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Hourly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Hourly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Hourly.BasicSchedule.retentionUnit })}) })`
            -WeeklySchedule $(if($null -ne $sla.snapshotSchedule.Weekly) {(New-Object -TypeName RubrikSecurityCloud.Types.WeeklySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Weekly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Weekly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Weekly.BasicSchedule.retentionUnit })}) })`
            -MonthlySchedule $(if($null -ne $sla.snapshotSchedule.Monthly) {(New-Object -TypeName RubrikSecurityCloud.Types.MonthlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Monthly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Monthly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Monthly.BasicSchedule.retentionUnit })}) })`
            -QuarterlySchedule $(if($null -ne $sla.snapshotSchedule.Quarterly) {(New-Object -TypeName RubrikSecurityCloud.Types.QuarterlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Quarterly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Quarterly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Quarterly.BasicSchedule.retentionUnit })}) })`
            -YearlySchedule $(if($null -ne $sla.snapshotSchedule.Yearly) {(New-Object -TypeName RubrikSecurityCloud.Types.YearlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Yearly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Yearly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Yearly.BasicSchedule.retentionUnit })}) })
    }
    else {
        $message="SLA {0} was tiering before script was run, leaving it as tiering." -f $sla.name
        Write-Log $message
    }

    if ($protectionUpdated){
        Write-Log "Reverting SLA on object and retention on snapshots back to '$existingSLAName'."
        Protect-RscWorkload -AssignmentType PROTECT_WITH_SLA_ID -ShouldApplyToExistingSnapshots -ShouldApplyToNonPolicySnapshots -Id $thisShare.primaryFileset.id -Sla $(Get-RscSla -Name $existingSLAName | where name -eq $existingSLAName)
        <#
        foreach ($snap in $thisShare.PrimaryFileset.SnapshotConnection.Nodes){
            $snapRetention=(Set-SnapshotRetention -slaId $existingSLAID -snapshotId $snap.id)
            Write-Host $snapRetention | ConvertTo-Json
        }
        #>
    }
} else {
    $statusSet = [System.Collections.ArrayList]::new()
    Write-Log('No nassharename specified iterating')
    #$allNasShares = Get-Nasshares -cluster $thisCluster
    $allNasShares = Get-Nasshare -cluster $thisCluster
    # Filter to only NAS shares with an SLA applied by checking primaryFileset.id
    $allNasShares = $allNasShares | Where-Object { $_.primaryFileset -and $_.primaryFileset.id } | Where-Object {$_.PrimaryFileset.IsPassThrough}
    
    <#
    # Interactive protocol filter with numbered options
    Write-Host "Select which share types you want to see:"
    Write-Host "1. SMB"
    Write-Host "2. NFS"
    Write-Host "3. BOTH"
    $protocolNumber = Read-Host 'Enter the number corresponding to your choice'
    switch ($protocolNumber) {
        '1' { $protocolChoice = 'SMB' }
        '2' { $protocolChoice = 'NFS' }
        '3' { $protocolChoice = 'BOTH' }
        default { Write-Log -isWarning 'Invalid input, showing all shares.'; $protocolChoice = 'BOTH' }
    }
    switch ($protocolChoice.ToUpper()) {
        'SMB'  { $allNasShares = $allNasShares | Where-Object { $_.shareType -eq 'SMB' } }
        'NFS'  { $allNasShares = $allNasShares | Where-Object { $_.shareType -eq 'NFS' } }
        'BOTH' { }
    }
    #>
    Write-Log('Found {0} shares with a primaryFileset (SLA) applied set to use Direct Archive' -f $allNasShares.count)
    $confirm = Read-Host('Proceed with tiering of all shares? y/n')
    if($confirm -eq 'y'){
        $Archive = Get-ArchivalLocations | Where-Object name -eq $archivalLocationName
        Write-Log ('Found Archival Location {0} with id {1}' -f $Archive.name, $Archive.id)
        foreach($share in $allNasShares){
            $errString=""
            Write-Log ('Found share {0} with id {1}' -f $share.name, $share.id)
            try{
                # Check if primary fileset is protected
                if ($share.DescendantConnection.Nodes.EffectiveSlaDomain.Name -ne "DO_NOT_PROTECT"){
                    Write-Log ("Primary Fileset has SLA named {0} with ID {1}" -f $thisShare.DescendantConnection.Nodes.EffectiveSlaDomain.Name, $share.DescendantConnection.Nodes.EffectiveSlaDomain.Id)
                    $existingSLAID=$share.DescendantConnection.Nodes.EffectiveSlaDomain.Id
                    $existingSLAName=$share.DescendantConnection.Nodes.EffectiveSlaDomain.Name
                    Write-Log "Updating SLA to DO_NOT_PROTECT and marking existing snapshots as KEEP_FOREVER."
                    Protect-RscWorkload -AssignmentType DO_NOT_PROTECT -ExistingSnapshotAction "KEEP_FOREVER" -Id $share.primaryFileset.id
                    $protectionUpdated=$true
                }
                else {
                    Write-Log ("Primary Fileset has already been marked as 'DO_NOT_PROTECT'")
                    $existingSLAID=$share.DescendantConnection.Nodes.EffectiveSlaDomain.Id
                    $existingSLAName=$share.DescendantConnection.Nodes.EffectiveSlaDomain.Name
                    Write-Log "Marking existing snapshots as KEEP_FOREVER in order to ensure existing snaps are able to tier."
                    Protect-RscWorkload -AssignmentType DO_NOT_PROTECT -ExistingSnapshotAction "KEEP_FOREVER" -Id $share.primaryFileset.id
                    $protectionUpdated=$false
                }

                # Add step to check tiering level of SLA and update to use Glacier Deep Archive
                $slaAlreadyTiering=$false
                if ($existingSLAName -eq "DO_NOT_PROTECT"){
                    if ($retentionSLAOverride){
                        Write-Log "Share primary fileset is not protected, using override SLA provided."
                        $sla=(get-sla -slaName $retentionSLAOverride | where name -eq $retentionSLAOverride)
                    }
                    else {
                        Write-Log -isWarning "Fileset set is not protected, and there is no override Archive location provided. Skipping this Fileset and share."
                        $errString="Fileset set is not protected, and there is no override Archive location provided. Skipping this Fileset and share."
                        $status="Skipped"
                        $thisObject = [PSCustomObject]@{
                            Name = $share.Name
                            Id = $share.Id
                            TieringStarted = $status
                            Message=$errString
                        }
                        $statusSet.add($thisObject) | Out-Null
                        continue
                    }
                }
                else {
                    $sla=(get-sla -slaName $existingSLAName | where name -eq $existingSLAName)
                }
                if ($null -eq $sla.ArchivalSpecs.ArchivalTieringSpec){
                    $message="SLA {0} has an Archive location in {1}, but has no tiering spec set." -f $sla.name,$sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.TargetType
                    Write-Log $message
                    Write-Log "Updating SLA to tier to AWS_GLACIER_DEEP_ARCHIVE"
                        $archivalSpec = New-RscSlaArchivalSpecs -ArchivalThreshold 0 -ArchivalThresholdUnit MINUTES -Frequencies @('DAYS') -InstantTiering -ColdStorageClass AWS_GLACIER_DEEP_ARCHIVE -LocationIds @($sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.Id) -ClusterUuids @($thisCluster.id)
                        Set-RscSla -Sla $sla -ArchivalSpecs @($archivalSpec) -UserNote "Temp update to include archive tiering" -ObjectTypes @($sla.objectTypes)`
                        -MinuteSchedule $(if($null -ne $sla.snapshotSchedule.Minute) {(New-Object -TypeName RubrikSecurityCloud.Types.MinuteSnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Minute.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Minute.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Minute.BasicSchedule.retentionUnit })})}) `
                        -DailySchedule $(if($null -ne $sla.snapshotSchedule.Daily) {(New-Object -TypeName RubrikSecurityCloud.Types.DailySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Daily.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Daily.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Daily.BasicSchedule.retentionUnit })}) })`
                        -HourlySchedule $(if($null -ne $sla.snapshotSchedule.Hourly) {(New-Object -TypeName RubrikSecurityCloud.Types.HourlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Hourly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Hourly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Hourly.BasicSchedule.retentionUnit })}) })`
                        -WeeklySchedule $(if($null -ne $sla.snapshotSchedule.Weekly) {(New-Object -TypeName RubrikSecurityCloud.Types.WeeklySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Weekly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Weekly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Weekly.BasicSchedule.retentionUnit })}) })`
                        -MonthlySchedule $(if($null -ne $sla.snapshotSchedule.Monthly) {(New-Object -TypeName RubrikSecurityCloud.Types.MonthlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Monthly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Monthly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Monthly.BasicSchedule.retentionUnit })}) })`
                        -QuarterlySchedule $(if($null -ne $sla.snapshotSchedule.Quarterly) {(New-Object -TypeName RubrikSecurityCloud.Types.QuarterlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Quarterly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Quarterly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Quarterly.BasicSchedule.retentionUnit })}) })`
                        -YearlySchedule $(if($null -ne $sla.snapshotSchedule.Yearly) {(New-Object -TypeName RubrikSecurityCloud.Types.YearlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Yearly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Yearly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Yearly.BasicSchedule.retentionUnit })}) })
                }
                else {
                    $message="SLA {0} has an Archive location in {1}, and tiering spec set to tier to {2} with InstantTiering set to {3}." -f $sla.name,$sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.TargetType,$sla.ArchivalSpecs.ArchivalTieringSpec.coldStorageClass,$sla.ArchivalSpecs.ArchivalTieringSpec.isInstantTieringEnabled
                    $slaAlreadyTiering=$true
                    Write-Log $message
                }
                $message="SLA {0} is set to archive after {1} {2}." -f $sla.name,$sla.ArchivalSpecs.threshold, $sla.ArchivalSpecs.thresholdUnit
                Write-Log $message

                Write-Log('Starting tier job of {0}' -f $thisShare.name)
                $operation = Set-ObjectTiering -clusterUuid $thisCluster.id -archivalLocationId $thisArchive.id -objectid $share.DescendantConnection.Nodes.id

                $status = "Started"

                # Add step to check tiering level of SLA and update back to Standard
                if (!$slaAlreadyTiering){
                    $message="Updating SLA {0} back to non-tiering." -f $sla.name
                    Write-Log $message
                    $archivalSpec = New-RscSlaArchivalSpecs -ArchivalThreshold 0 -ArchivalThresholdUnit MINUTES -Frequencies @('DAYS') -LocationIds @($sla.ArchivalSpecs.ArchivalLocationToClusterMapping.Location.Id) -ClusterUuids @($thisCluster.id)
                    Set-RscSla -Sla $sla -ArchivalSpecs @($archivalSpec) -UserNote "Temp update to remove archive tiering" -ObjectTypes @($sla.objectTypes)`
                        -MinuteSchedule $(if($null -ne $sla.snapshotSchedule.Minute) {(New-Object -TypeName RubrikSecurityCloud.Types.MinuteSnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Minute.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Minute.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Minute.BasicSchedule.retentionUnit })})}) `
                        -DailySchedule $(if($null -ne $sla.snapshotSchedule.Daily) {(New-Object -TypeName RubrikSecurityCloud.Types.DailySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Daily.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Daily.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Daily.BasicSchedule.retentionUnit })}) })`
                        -HourlySchedule $(if($null -ne $sla.snapshotSchedule.Hourly) {(New-Object -TypeName RubrikSecurityCloud.Types.HourlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Hourly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Hourly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Hourly.BasicSchedule.retentionUnit })}) })`
                        -WeeklySchedule $(if($null -ne $sla.snapshotSchedule.Weekly) {(New-Object -TypeName RubrikSecurityCloud.Types.WeeklySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Weekly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Weekly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Weekly.BasicSchedule.retentionUnit })}) })`
                        -MonthlySchedule $(if($null -ne $sla.snapshotSchedule.Monthly) {(New-Object -TypeName RubrikSecurityCloud.Types.MonthlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Monthly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Monthly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Monthly.BasicSchedule.retentionUnit })}) })`
                        -QuarterlySchedule $(if($null -ne $sla.snapshotSchedule.Quarterly) {(New-Object -TypeName RubrikSecurityCloud.Types.QuarterlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Quarterly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Quarterly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Quarterly.BasicSchedule.retentionUnit })}) })`
                        -YearlySchedule $(if($null -ne $sla.snapshotSchedule.Yearly) {(New-Object -TypeName RubrikSecurityCloud.Types.YearlySnapshotScheduleInput -Property @{BasicSchedule=(New-Object -TypeName RubrikSecurityCloud.Types.BasicSnapshotScheduleInput -Property @{ Frequency=$sla.snapshotSchedule.Yearly.BasicSchedule.Frequency;Retention=$sla.snapshotSchedule.Yearly.BasicSchedule.retention;RetentionUnit=$sla.snapshotSchedule.Yearly.BasicSchedule.retentionUnit })}) })
                    $status+=" + SLA reverted"
                }
                else {
                    $message="SLA {0} was tiering before script was run, leaving it as tiering." -f $sla.name
                    Write-Log $message
                }

                if ($protectionUpdated){
                    Write-Log "Reverting SLA on object and retention on snapshots back to '$existingSLAName'."
                    Protect-RscWorkload -AssignmentType PROTECT_WITH_SLA_ID -ShouldApplyToExistingSnapshots -ShouldApplyToNonPolicySnapshots -Id $share.primaryFileset.id -Sla $(Get-RscSla -Name $existingSLAName | where name -eq $existingSLAName)
                    $status+=" + Retention reverted"
                }
            }
            catch {
                $status = "Failed"
                $errString = $_.Exception
            }
            if($operation.errors){
                $status = "Failed"
                $errString = $operation.errors.message
            }
            if($null -eq $operation.errors){
                Write-Log -isSuccess('{0} tiering started' -f $share.name)
            } else {
                Write-Log -isWarning('{0} tiering failed' -f $share.name)
            }
            $thisObject = [PSCustomObject]@{
                Name = $share.Name
                Id = $share.Id
                TieringStarted = $status
                Message=$errString
            }
            $statusSet.add($thisObject) | Out-Null
        }
    Write-Host('Statuses:')
    $statusSet | Format-Table |Out-String| Write-Host
    } else {
        $allNasShares | Select-Object id, name, shareType, primaryFileset
    }
}
Disconnect-Rsc