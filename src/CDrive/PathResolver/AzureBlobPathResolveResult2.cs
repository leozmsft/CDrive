using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;
using System.Collections.Generic;
using System.Linq;

namespace CDrive
{
    public class AzureBlobPathResolveResult2
    {
        public AzureBlobPathResolveResult2()
        {
            this.PathType = PathType.Invalid;
        }
        public PathType PathType { get; set; }

        public CloudBlobContainer Container { get; set; }

        public BlobQuery BlobQuery { get; set; }

        public List<string> Parts { get; set; }
        public bool Exists()
        {
            return true;
        }
    }
}
