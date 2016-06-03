@{

ModuleToProcess = '.\CDrive.dll'
ModuleVersion = '0.1.0'

# ID used to uniquely identify this module
GUID = '66e1d012-3aa1-4b0b-a614-330c1fcedb8c'

# Author of this module
Author = 'Leo Zhou'

# Minimum version of the Windows PowerShell engine required by this module
PowerShellVersion = '3.0'

# Minimum version of the .NET Framework required by this module
DotNetFrameworkVersion = '4.0'

# Minimum version of the common language runtime (CLR) required by this module
CLRVersion='4.0'

# Format files (.ps1xml) to be loaded when importing this module
FormatsToProcess = @(
    'config\AzureFile.format.ps1xml',
	'config\AzureBlob.format.ps1xml',
	'config\AzureTable.format.ps1xml',
	'config\AzureQueue.format.ps1xml',
	'config\general.format.ps1xml'
)

TypesToProcess = @(
	'config\AzureFile.types.ps1xml', 
	'config\AzureTable.types.ps1xml', 
	'config\AzureQueue.types.ps1xml', 
	'config\AzureBlob.types.ps1xml',
	'config\Local.types.ps1xml')

# Functions to export from this module
FunctionsToExport = '*'

# Cmdlets to export from this module
CmdletsToExport = '*'

# Variables to export from this module
VariablesToExport = '*'
}
