namespace CDrive
{
    public enum PathType
    {
        AzureFileRoot,
        AzureFileDirectory,
        AzureFile,

        AzureBlobRoot,
        AzureBlobDirectory,
        AzureBlobBlock,
        AzureBlobPage,
        AzureBlobAppend,

        AzureTableRoot,
        AzureTableQuery,

        AzureQueueRoot,
        AzureQueueQuery,

        Invalid,
        Unknown
    }
}