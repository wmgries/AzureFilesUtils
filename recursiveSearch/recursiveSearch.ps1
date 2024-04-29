<#
    .SYNOPSIS
    recursiveSearch.ps1 is a PowerShell script to search a file share and/or its
    snapshots for files or folders matching a particular name.

    .DESCRIPTION
    recursiveSearch.ps1 is a PowerShell script to search a file share and/or its
    snapshots for files or folders matching a particular name.

    .PARAMETER ResourceGroupName
    The name of the resource group containing the target storage account/file 
    share.

    .PARAMETER StorageAccountName
    The name of the storage account containing the target file share.

    .PARAMETER FileShareName
    The name of the file share to run the search one.

    .PARAMETER TargetItemName
    The item (file or directory) to search for.

    .PARAMETER SearchScope
    The scope over which to search; valid options are:
    - FileShare: search only the live file share for the TargetItemName.
    - FileShareSnapshots: search only the file share snapshots for the 
      TargetItemName.
    - Both (default): search both the FileShare and FileShareSnapshots for the 
      TargetItemName.

    .PARAMETER MatchBehavior
    Specify the script behavior when a match is found:
    - End: stop the search immediately and do not continue.
    - ScopeEnd (default): continue with the search through the end of the 
      current scope.
    - Continue: run until all scopes have been searched.

    .INPUTS
    None

    .OUTPUTS
    None

    .EXAMPLE
    .\recursiveSearch.ps1 `
            -ResourceGroupName "testrg" `
            -StorageAccountName "testsa" `
            -FileShareName "testshare" `
            -TargetItemName "testfile.docx"

    .EXAMPLE
    .\recursiveSearch.ps1 `
            -ResourceGroupName "testrg" `
            -StorageAccountName "testsa" `
            -FileShareName "testshare" `
            -TargetItemName "testdirectory" `
            -SearchScope FileShareSnapshots `
            -MatchBehavior Continue

    .EXAMPLE
    Get-AzRmStorageShare `
            -ResourceGroupName "testrg" `
            -StorageAccountName "testsa" `
            -FileShareName "testshare" | `
        .\recursiveSearch.ps1 `
            -TargetItemName "testfile.docx"
    
    .NOTES
    This script assumes you have the following PowerShell modules installed:
    - Az.Accounts
    - Az.Resources
    - Az.Storage

    The simplest way to ensure all dependencies are required is to install the
    Az PowerShell module:

    ```PowerShell
    Install-Module Az
    ```
#>


#requires -Modules Az.Accounts, Az.Resources, Az.Storage
using namespace Azure.Storage.Files.Shares
using namespace Azure.Storage


[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [System.String]$ResourceGroupName,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [System.String]$StorageAccountName,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [Alias("Name")]
    [System.String]$FileShareName,

    [Parameter(Mandatory = $true)]
    [System.String]$TargetItemName,

    [Parameter(Mandatory = $false)]
    [ValidateSet("FileShare", "FileShareSnapshots", "Both")]
    [System.String]$SearchScope = "Both",

    [Parameter(Mandatory = $false)]
    [ValidateSet("End", "ScopeEnd", "Continue")]
    [System.String]$MatchBehavior = "ScopeEnd"
)


# Check to ensure that signed into Azure.
$context = Get-AzContext
if ($null -eq $context) {
    Write-Error `
            -Message ("Azure account not connected to Az PowerShell " + `
                "module. Run Connect-AzAccount and use Set-AzContext to " + `
                "select the appropriate Azure subscription.") `
            -ErrorAction Stop
} 

$account = $context | Select-Object -ExpandProperty Account
Write-Verbose -Message "Azure account ($($account.Id)) connected."

$subscription = $context | Select-Object -ExpandProperty Subscription
Write-Verbose `
        -Message ("Selected subscription $($subscription.Name) " + `
            "($($subscription.Id)).")


# Check to ensure that the resource group exists.
$resourceGroup = Get-AzResourceGroup | `
    Where-Object { $_.ResourceGroupName -eq $ResourceGroupName }
if ($null -eq $resourceGroup) {
    Write-Error `
            -Message ("Resource group $ResourceGroupName not found " + `
                "in subscription $($subscription.Name) " + `
                "($($subscription.Id)).") `
            -ErrorAction Stop
}

Write-Verbose -Message "Resource group $ResourceGroupName confirmed to exist."


# Check to ensure that the storage account exists.
$storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName | `
    Where-Object { $_.StorageAccountName -eq $StorageAccountName }
if ($null -eq $storageAccount) {
    Write-Error `
            -Message ("Specified storage account $StorageAccountName not " + `
                "found in resource group $ResourceGroupName in " + `
                "subscription $($subscription.Name) ($($subscription.Id)).") `
            -ErrorAction Stop
}

Write-Verbose -Message "Storage account $StorageAccountName confimed to exist."


# Check to ensure that the storage account has shared keys enabled.
if (!$storageAccount.AllowSharedKeyAccess) {
    Write-Error `
            -Message ("This script relies on shared key access, however " + `
                "storage account $StorageAccountName has shared key " + `
                "disabled. Enable shared key to continue.") `
            -ErrorAction Stop
}


# Get the shared key account key for direct use later
$key = $null
try {
    $key = $storageAccount | `
        Get-AzStorageAccountKey | `
        Where-Object { $_.KeyName -eq "key1" } | `
        Select-Object -ExpandProperty Value
} catch {
    Write-Error `
        -Message ("Unable to get the enumerate the shared key. This could " + `
            "indicate that that you don't have access to enumerate " + `
            "the shared keys.") `
        -ErrorAction Stop
}

if ($null -eq $key) {
    Write-Error `
        -Message ("Unable to get the enumerate the shared key. This could " + `
            "indicate that that you don't have access to enumerate " + `
            "the shared keys.") `
        -ErrorAction Stop
}

Write-Verbose -Message "Acquired shared key for file/directory enumeration."


# Attempt to enumerate file shares
$fileSharesMatchingName = $null
$fileShare = Get-AzStorageShare `
        -Context $storageAccount.Context `
        -Prefix $FileShareName | `
    Where-Object { $_.Name -eq $FileShareName -and !$_.IsDeleted } | `
    Tee-Object -Variable "fileSharesMatchingName" | `
    Where-Object { !$_.IsSnapshot }

if ($null -eq $fileShare) {
    Write-Error `
            -Message ("Specified file share $FileShareName not found in " + `
                "storage account $StorageAccountName.") `
            -ErrorAction Stop
}

Write-Verbose -Message "File share $FileShareName confirmed to exist."

$targetScopes = @()

if ($SearchScope -eq "FileShare" -or $SearchScope -eq "Both") {
    $targetScopes += $fileShare
}

if ($SearchScope -eq "FileShareSnapshots" -or $SearchScope -eq "Both") {
    $fileShareSnapshots = $fileSharesMatchingName | `
        Where-Object { $_.IsSnapshot } | `
        Sort-Object -Property SnapshotTime -Descending
    
    if ($SearchScope -eq "FileShareSnapshots") {
        if ($null -eq $fileShareSnapshots) {
            Write-Error `
                    -Message ("Specified file share $FileShareName doesn't " + `
                        "have file share snapshots.") `
                    -ErrorAction Stop
        }
    }

    if ($null -ne $fileShareSnapshots) {
        $targetScopes = $targetScopes + $fileShareSnapshots
    }
}


# Enumerate files in file share snapshots
$sharedKeyCred = [StorageSharedKeyCredential]::new("wgriesmfsressa2", $key)

foreach ($targetScope in $targetScopes) {
    $endpointUri = $targetScope.ShareClient.Uri
    Write-Verbose `
            -Message ("Enumerating files and directories " + `
                "for: $($endpointUri.ToString())")

    $queue = [System.Collections.Generic.Queue[System.String]]::new()
    $queue.Enqueue("/")

    $shouldBreak = $false

    while ($queue.Count -gt 0) {
        $dirPath = $queue.Dequeue()

        $uriBuilder = [System.UriBuilder]::new($endpointUri)
        $uriBuilder.Path += $dirPath
        $currentUri = $uriBuilder.Uri

        Write-Debug `
                -Message ("Enumerating files of directory: " + `
                    $currentUri.ToString());

        $dirClient = [ShareDirectoryClient]::new(
            $currentUri, 
            $sharedKeyCred
        )

        $dirEnum = $dirClient.GetFilesAndDirectories()
        foreach ($dirItem in $dirEnum) {
            $path = "$dirPath$($dirItem.Name)"
            Write-Debug -Message "Item: $path"

            if ($dirItem.Name -eq $TargetItemName) {
                $obj = [PSCustomObject]@{
                    ResourceGroupName = $ResourceGroupName
                    StorageAccountName = $StorageAccountName
                    FileShareName = $FileShareName
                    SnapshotTime = $targetScope.SnapshotTime
                    Path = $path
                }

                Write-Output -InputObject $obj
                if ($MatchBehavior -eq "End") {
                    $shouldBreak = $true
                    $queue.Clear()
                    break
                } elseif ($MatchBehavior -eq "ScopeEnd") {
                    $shouldBreak = $true
                }
            }

            if ($dirItem.IsDirectory) {
                $queue.Enqueue("$path/")
            }
        }
    }

    if ($shouldBreak) {
        break
    }
}
