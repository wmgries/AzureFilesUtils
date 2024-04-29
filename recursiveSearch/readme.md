# Recursive search
The [recursiveSearch.ps1](./recursiveSearch.ps1) PowerShell script is a quick 
utility to search a file share and/or its snapshots for files or folders with a 
particular name.

```PowerShell
# Example 1: Search for testfile.docx with default behavior
.\recursiveSearch.ps1 `
        -ResourceGroupName "testrg" `
        -StorageAccountName "testsa" `
        -FileShareName "testshare" `
        -TargetItemName "testfile.docx"

# Example 2: Search only the file share snapshots for testdirectory and find all
# occurrences.
.\recursiveSearch.ps1 `
        -ResourceGroupName "testrg" `
        -StorageAccountName "testsa" `
        -FileShareName "testshare" `
        -TargetItemName "testdirectory" `
        -SearchScope FileShareSnapshots `
        -MatchBehavior Continue

# Example 3: Pipe the output of Get-AzRmStorageShare to the recursiveSearch.ps1
# script and find testfile.docx using default script behavior.
Get-AzRmStorageShare `
        -ResourceGroupName "testrg" `
        -StorageAccountName "testsa" `
        -FileShareName "testshare" | `
    .\recursiveSearch.ps1 `
        -TargetItemName "testfile.docx"
```
    