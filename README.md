# CDrive

*Features*
1. Support Azure Blob Service:
   Create/List/Remove container
   Create/Delete virtual directory hierarchy
   List/View/Delete blobs
   List blobs in under a virtual directory
   Create PageBlob of desired size
   Create AppendBlob, and append blocks
   Upload local file as BlockBlob
   Download BlockBlob/PageBlob/AppendBlob
2. <TBD>

*Usage*
1. Open PowerShell command  prompt windows
2. Run:  
   Import-module .\cdrive.psd1
   New-PSDrive -name x -psprovider CDrive -root /
   Note: if x: is occupied, you can use another drive label.
3. Map Azure Blob/Table/Queue/File services to a directory under the drive. e.g.
   cd x:
   New-Item [myBlob] -type AzureBlob -value https://[yourAccount].blob.core.windows.net/?account=[yourAccount]`&key=[yourAccountKey]
   New-Item [myQueue] -type AzureBlob -value https://[yourAccount].queue.core.windows.net/?account=[yourAccount]`&key=[yourAccountKey]
   New-Item [myFile] -type AzureBlob -value https://[yourAccount].file.core.windows.net/?account=[yourAccount]`&key=[yourAccountKey]
   New-Item [myTable] -type AzureBlob -value https://[yourAccount].table.core.windows.net/?account=[yourAccount]`&key=[yourAccountKey]
   Note: you'll need to replace those placeholders wrapped in brackets, and you can map multiple services under the root.
