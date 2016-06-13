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

        LocalDirectory,
        LocalFile,

        Container,
        Item,
        Any,

        Invalid,
        Unknown
    }
}