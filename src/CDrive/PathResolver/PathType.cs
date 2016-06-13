namespace CDrive
{
    public enum PathType
    {
        Root,

        AzureFileRoot,
        AzureFileDirectory,
        AzureFile,

        AzureBlobRoot,
        AzureBlobQuery,

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