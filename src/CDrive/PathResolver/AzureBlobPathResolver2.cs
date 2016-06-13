using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureBlobPathResolver2 : PathResolver
    {
        public static AzureBlobPathResolveResult2 ResolvePath(CloudBlobClient client, string path)
        {
            var result = new AzureBlobPathResolveResult2();
            var parts = SplitPath(path);
            if (!ValidatePath(parts))
            {
                throw new Exception("Path " + path + " is invalid");
            }

            result.Parts = parts;

            if (parts.Count == 0)
            {
                result.PathType = PathType.AzureBlobRoot;
            }

            if (parts.Count == 1)
            {
                result.PathType = PathType.Container;
                result.Container = client.GetContainerReference(parts[0]);
                result.BlobQuery = new BlobQuery();
                return result;
            }

            if (parts.Count > 0)
            {
                result.Container = client.GetContainerReference(parts[0]);
                result.PathType = PathType.AzureBlobQuery;
                result.BlobQuery = new BlobQuery();
            }

            if (result.PathType == PathType.AzureBlobQuery)
            {
                var prefixList = new List<string>();
                var q = result.BlobQuery;

                for (var i = 1; i < parts.Count; ++i)
                {
                    var p = parts[i];
                    var take = 0;
                    if (p.StartsWith("take=") && Int32.TryParse(p.Substring("take=".Length), out take))
                    {
                        q.MaxResult = take;
                        continue;
                    }

                    if (p.StartsWith("details=") && !p.EndsWith("details="))
                    {
                        var details = BlobListingDetails.All;
                        if (Enum.TryParse<BlobListingDetails>(p.Substring("details=".Length), true, out details))
                        {
                            q.BlobListingDetails = details;
                            continue;
                        } else
                        {
                            throw new Exception("Allowed values for details: None, Snapshots, Metadata, UncommittedBlobs, Copy, All");
                        }
                    }

                    prefixList.Add(p);
                }

                q.Prefix = string.Join("/", prefixList);
            }

            return result;
        }

        public static bool ValidatePath(List<string> parts)
        {
            if (parts.Count == 0)
            {
                return true;
            }

            //todo: add more checks here
            return true;
        }
    }
}
