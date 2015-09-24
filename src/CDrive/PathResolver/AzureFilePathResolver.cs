using Microsoft.WindowsAzure.Storage.File;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureFilePathResolver : PathResolver
    {
        public static AzureFilePathResolveResult ResolvePath(CloudFileClient client, string path, PathType hint = PathType.Unknown, bool skipCheckExistence = true, bool createAncestorDirectories = false)
        {
            var result = new AzureFilePathResolveResult();
            var parts = SplitPath(path);
            if (!ValidatePath(parts))
            {
                throw new Exception("Path " + path + " is invalid");
            }

            result.Parts = parts;

            if (parts.Count == 0)
            {
                result.PathType = PathType.AzureFileRoot;
            }

            if (parts.Count > 0)
            {
                result.Share = client.GetShareReference(parts[0]);
                result.Directory = result.Share.GetRootDirectoryReference();
                result.PathType = PathType.AzureFileDirectory;
                result.RootDirectory = result.Directory;

                if (createAncestorDirectories && parts.Count > 1 && !result.Share.Exists())
                {
                    result.Share.Create();
                }
            }

            if (parts.Count > 1)
            {
                for (var level = 1; level < parts.Count - 1; ++level)
                {
                    //assume it's directory
                    var dir = result.Directory.GetDirectoryReference(parts[level]);

                    if (createAncestorDirectories && !dir.Exists())
                    {
                        dir.Create();
                    }

                    if (result.PathType == PathType.AzureFileDirectory)
                    {
                        result.Directory = dir;
                        result.PathType = PathType.AzureFileDirectory;
                        continue;
                    }
                }

                //last element
                if (hint == PathType.AzureFileDirectory || hint == PathType.Unknown)
                {
                    //assume it's directory first
                    var dir = result.Directory.GetDirectoryReference(parts.Last());
                    if (result.PathType == PathType.AzureFileDirectory && (skipCheckExistence || dir.Exists()))
                    {
                        result.Directory = dir;
                        result.PathType = PathType.AzureFileDirectory;
                        return result;
                    }

                }

                //2. assume it's a file
                if (hint == PathType.AzureFile || hint == PathType.Unknown)
                {
                    var file = result.Directory.GetFileReference(parts.Last());
                    if (result.PathType == PathType.AzureFileDirectory && (skipCheckExistence || file.Exists()))
                    {
                        result.File = file;
                        result.PathType = PathType.AzureFile;
                        return result;
                    }
                }

                result.PathType = PathType.Unknown;
            }

            if (result.PathType == PathType.AzureFileDirectory && hint == PathType.AzureFile)
            {
                result.PathType = PathType.Invalid;
            }

            return result;
        }

        public static bool ValidatePath(List<string> parts)
        {
            if (parts.Count == 0)
            {
                return true;
            }

            if (!Regex.Match(parts[0], SharePattern).Success)
            {
                return false;
            }

            for (var i = 1; i < parts.Count; ++i)
            {
                if (!Regex.Match(parts[i], FilePattern).Success)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
