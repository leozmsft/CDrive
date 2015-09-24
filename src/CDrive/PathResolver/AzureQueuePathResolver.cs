using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureQueuePathResolver : PathResolver
    {
        public static AzureQueuePathResolveResult ResolvePath(CloudQueueClient client, string path)
        {
            var result = new AzureQueuePathResolveResult();
            var parts = SplitPath(path);
            if (!ValidatePath(parts))
            {
                throw new Exception("Path " + path + " is invalid");
            }

            result.Parts = parts;

            if (parts.Count == 0)
            {
                result.PathType = PathType.AzureQueueRoot;
            }

            if (parts.Count > 0)
            {
                result.Queue = client.GetQueueReference(parts[0]);
                result.PathType = PathType.AzureQueueQuery;
            }

            return result;
        }

        public static bool ValidatePath(List<string> parts)
        {
            return true;
        }
    }
}
