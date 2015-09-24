using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;
using System.Collections.Generic;
using System.Linq;

namespace CDrive
{
    public class AzureBlobPathResolveResult
    {
        public AzureBlobPathResolveResult()
        {
            this.PathType = PathType.Invalid;
        }
        public PathType PathType { get; set; }

        public CloudBlobDirectory Directory { get; set; }
        public CloudBlobDirectory RootDirectory { get; set; }
        public CloudBlobContainer Container { get; set; }
        public ICloudBlob Blob { get; set; }
        public List<string> Parts { get; set; }
        public bool Exists()
        {
            switch (PathType)
            {
                case PathType.AzureBlobRoot:
                    return true;
                case PathType.AzureBlobDirectory:
                    if (this.Parts.Count == 1)
                    {
                        //check container
                        return this.Container.Exists();
                    }

                    return this.Directory.ListBlobsSegmented(true, BlobListingDetails.None, 1, null, null, null).Results.Count() > 0;
                case PathType.AzureBlobBlock:
                case PathType.AzureBlobAppend:
                case PathType.AzureBlobPage:
                    var e = this.Container.Exists();
                    if (!e)
                    {
                        return false;
                    }

                    return this.Blob.Exists();
                default:
                    return false;
            }
        }
    }
}
