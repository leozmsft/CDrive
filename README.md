# CDrive

`PullRequests are welcome`


## Features
* Support Azure, AzureChina, Emulator, etc. modes
* Support Azure Blob Service:
 * Create/List/Remove containers
 * Create/Delete virtual directory hierarchy
 * List/View/Delete blobs
 * List blobs under a virtual directory
 * Create PageBlob of desired size
 * Create AppendBlob, and append blocks
 * Upload local file as BlockBlob
 * Download BlockBlob/PageBlob/AppendBlob
 * Create policy
 * Create Container/Blob SAS Token with or without policy
 * Make Container public/private
 * Async copy blobs on server side, query copy status, and cancel the copy
 * Show blob etag
 * Show blob properties and metadata
 * Edit blob metadata
* Support Azure Queue Service
 * Create/List/Remove queues
 * List/View/Create/Remove queue messages
* Support Azure Table Service
 * ...
* Support Azure File Service
 * Create/List/Remove shares
 * Create/Delete directories
 * List/View/Delete files
 * List files under a directory
 * Create files with specified size
 * Upload/Download files from/to local


## Usage

1. Open PowerShell command prompt windows

2. Run:  
    `Import-module .\cdrive.psd1`

    `New-PSDrive -name x -psprovider CDrive -root /`

	Note: if x: is occupied, you can use another drive label.
3. Map Azure Blob/Table/Queue/File services to directories under the drive. e.g.

	`cd x:`

	`New-Item [myBlob] -type AzureBlob -value https://[yourAccount].blob.core.windows.net/?account=[yourAccount]``&key=[yourAccountKey]`

	`New-Item [myTable] -type AzureTable -value https://[yourAccount].table.core.windows.net/?account=[yourAccount]``&key=[yourAccountKey]`

	`New-Item [myQueue] -type AzureQueue -value https://[yourAccount].queue.core.windows.net/?account=[yourAccount]``&key=[yourAccountKey]`

	`New-Item [myFile] -type AzureFile -value https://[yourAccount].file.core.windows.net/?account=[yourAccount]``&key=[yourAccountKey]`


	Note: you'll need to replace those placeholders wrapped in brackets, and you can map multiple services under the root.
