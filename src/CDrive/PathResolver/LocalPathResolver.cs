using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CDrive
{
    //public class LocalPathResolver : PathResolver
    //{
    //    public static LocalPathResolveResult ResolvePath(string driveLabel, string path, PathType hint = PathType.Unknown, bool skipCheckExistence = true, bool createAncestorDirectories = false)
    //    {
    //        var result = new LocalPathResolveResult();
    //        var parts = SplitPath(path);
    //        if (!ValidatePath(parts))
    //        {
    //            throw new Exception("Path " + path + " is invalid");
    //        }

    //        result.Parts = parts;

    //        if (parts.Count == 0)
    //        {
    //            result.PathType = PathType.LocalDirectory;
    //        }

    //        var temp = driveLabel;
    //        if (parts.Count > 0)
    //        {
    //            result.PathType = PathType.LocalDirectory;
    //            temp = Path.Combine(temp, parts[0]);
    //        }

    //        if (parts.Count > 1)
    //        {
    //            for (var level = 1; level < parts.Count - 1; ++level)
    //            {
    //                //assume it's directory
    //                temp = Path.Combine(temp, parts[level]);

    //                if (createAncestorDirectories && !Directory.Exists(temp))
    //                {
    //                    try
    //                    {
    //                        Directory.CreateDirectory(temp);
    //                    }
    //                    catch (Exception e)
    //                    {
    //                        throw e;
    //                    }
    //                }
    //            }

    //            //last element
    //            if (hint == PathType.LocalDirectory || hint == PathType.Unknown)
    //            {
    //                //assume it's directory first
    //                temp = Path.Combine(temp, parts.Last());
    //                if (result.PathType == PathType.LocalDirectory && (skipCheckExistence || Directory.Exists(temp)))
    //                {
    //                    result.PathType = PathType.LocalDirectory;
    //                    result.FullPath = temp;
    //                    return result;
    //                }
    //            }

    //            //2. assume it's a file
    //            if (hint == PathType.LocalFile || hint == PathType.Unknown)
    //            {
    //                temp = Path.Combine(temp, parts.Last());
    //                if (result.PathType == PathType.LocalFile && (skipCheckExistence || File.Exists(temp)))
    //                {
    //                    result.PathType = PathType.LocalFile;
    //                    result.FullPath = temp;
    //                    return result;
    //                }
    //            }

    //            result.PathType = PathType.Unknown;
    //        }

    //        if (result.PathType  != hint)
    //        {
    //            result.PathType = PathType.Invalid;
    //        }

    //        return result;
    //    }

    //    public static bool ValidatePath(List<string> parts)
    //    {
    //        return true;
    //    }
    //}
}
