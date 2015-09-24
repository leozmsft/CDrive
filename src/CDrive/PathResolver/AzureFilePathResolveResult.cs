using Microsoft.WindowsAzure.Storage.File;
using System.Collections.Generic;

namespace CDrive
{
    public class AzureFilePathResolveResult
    {
        public AzureFilePathResolveResult()
        {
            this.PathType = PathType.Invalid;
        }
        public PathType PathType { get; set; }


        public CloudFileShare Share { get; set; }
        public CloudFileDirectory Directory { get; set; }
        public CloudFile File { get; set; }
        public CloudFileDirectory RootDirectory { get; set;}

        public List<string> Parts { get; set; }
        public bool Exists()
        {
            switch (PathType)
            {
                case CDrive.PathType.AzureFile:
                    return this.File.Exists();
                case CDrive.PathType.AzureFileDirectory:
                    return this.Directory.Exists();
                case CDrive.PathType.AzureFileRoot:
                    return true;
                default:
                    return false;
            }
        }
    }
}
