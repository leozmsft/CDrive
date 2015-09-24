using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Collections.Generic;

namespace CDrive
{
    public class AzureQueuePathResolveResult
    {
        public AzureQueuePathResolveResult()
        {
            this.PathType = PathType.Invalid;
        }
        public PathType PathType { get; set; }

        public CloudQueue Queue { get; set; }

        public List<string> Parts { get; set; }
        public bool Exists()
        {
            switch (PathType)
            {
                case CDrive.PathType.AzureQueueQuery:
                    return this.Queue.Exists();
                case CDrive.PathType.AzureQueueRoot:
                    return true;
                default:
                    return false;
            }
        }
    }
}
