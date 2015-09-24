using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;

namespace CDrive
{
    public class AzureTablePathResolveResult
    {
        public AzureTablePathResolveResult()
        {
            this.PathType = PathType.Invalid;
        }
        public PathType PathType { get; set; }

        public CloudTable Table { get; set; }

        public TableQuery Query { get; set; }

        public List<string> Parts { get; set; }

        public bool Exists()
        {
            switch (PathType)
            {
                case CDrive.PathType.AzureTableQuery:
                    return this.Table.Exists();
                case CDrive.PathType.AzureTableRoot:
                    return true;
                default:
                    return false;
            }
        }
    }
}
