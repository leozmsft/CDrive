using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Collections;

namespace CDrive
{
    public class LocalDriveInfo : AbstractDriveInfo
    {
        public string Endpoint { get; set; }

        public LocalDriveInfo(string path, string name)
        {
            this.Endpoint = path;
            if (!this.Endpoint.EndsWith("\\"))
            {
                this.Endpoint += "\\";
            }
            this.Name = name;
        }

        public override void GetChildItems(string path, bool recurse)
        {
            var localPath = convertToLocalPath(path);
            try
            {
                if (Directory.Exists(localPath))
                {
                    var info = new DirectoryInfo(localPath);
                    foreach (var c in info.EnumerateDirectories())
                    {
                        this.RootProvider.WriteItemObject(c, path, true);
                    }
                    foreach (var c in info.EnumerateFiles())
                    {
                        this.RootProvider.WriteItemObject(c, path, false);
                    }
                }
                if (File.Exists(localPath))
                {
                    var info = new FileInfo(localPath);
                    this.RootProvider.WriteItemObject(info, path, false);
                }
            }
            catch { }

            if (recurse)
            {
                foreach (var c in Directory.GetDirectories(localPath))
                {
                    GetChildItems(convertToInternalPath(c), recurse);
                }
            }
        }

        public override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            var localPath = convertToLocalPath(path);
            try
            {
                if (Directory.Exists(localPath))
                {
                    var info = new DirectoryInfo(localPath);
                    foreach (var c in info.EnumerateDirectories())
                    {
                        this.RootProvider.WriteItemObject(c.Name, path, true);
                    }
                    foreach (var c in info.EnumerateFiles())
                    {
                        this.RootProvider.WriteItemObject(c.Name, path, false);
                    }
                }
            }
            catch { }
        }

        public override IContentReader GetContentReader(string path)
        {
            var localPath = convertToLocalPath(path);
            if (File.Exists(localPath)){
                return new LocalReader(localPath);
            }
            return null;
        }

        public override void GetProperty(string path, Collection<string> providerSpecificPickList)
        {
            throw new NotImplementedException();
        }

        public override bool HasChildItems(string path)
        {
            var childCount = 0;
            var localPath = convertToLocalPath(path);
            if (Directory.Exists(localPath))
            {
                childCount += Directory.GetDirectories(localPath).Length;
                childCount += Directory.GetFiles(localPath).Length;
            }
            return childCount > 0;
        }

        public override bool IsItemContainer(string path)
        {
            return Directory.Exists(convertToLocalPath(path));
        }

        public override bool IsValidPath(string path)
        {
            throw new NotImplementedException();
        }

        public override bool ItemExists(string path)
        {
            return Directory.Exists(convertToLocalPath(path)) || File.Exists(convertToLocalPath(path));
        }

        public override void NewItem(string path, string type, object newItemValue)
        {
            if (string.Equals(type, "Directory", StringComparison.InvariantCultureIgnoreCase))
            {
                Directory.CreateDirectory(convertToLocalPath(path));
            }
        }

        public override void RemoveItem(string path, bool recurse)
        {
            if (Directory.Exists(convertToLocalPath(path)))
            {
                Directory.Delete(convertToLocalPath(path), recurse);
            }
            else
            {
                File.Delete(convertToLocalPath(path));
            }
            
        }

        public override void SetProperty(string path, PSObject propertyValue)
        {
            throw new NotImplementedException();
        }

        internal string convertToLocalPath(string path)
        {
            return Endpoint + path;
        }

        internal string convertToInternalPath(string path)
        {
            return path.Substring(Endpoint.Length);
        }

        public override Stream CopyFrom(string path)
        {
            return new FileStream(convertToLocalPath(path), FileMode.Open);
        }

        public override Stream CopyTo(string path, string name)
        {
            return new FileStream(Path.Combine(convertToLocalPath(path), name), FileMode.CreateNew);
        }

        public override IList<string> GetChildNamesList(string path, PathType type = PathType.Any)
        {
            var childs = new List<string>();
            var info = new DirectoryInfo(convertToLocalPath(path));
            if (type == PathType.Container || type == PathType.Any)
            {
                childs.AddRange(info.EnumerateDirectories().Select(x => x.Name));
            }
            if (type == PathType.Item || type == PathType.Any)
            {
                childs.AddRange(info.EnumerateFiles().Select(x => x.Name));
            }
            return childs;
        }
    }
}

class LocalReader : IContentReader
{
    private StreamReader reader;

    public LocalReader(string localPath)
    {
        this.reader = new StreamReader(localPath);
    }

    public void Close()
    {
        this.reader.Close();
    }

    public void Dispose()
    {
        this.reader.Dispose();
    }

    public IList Read(long readCount)
    {
        var l = new List<string>();
        while (!this.reader.EndOfStream && l.Count < readCount)
        {
            l.Add(this.reader.ReadLine());
        }
        return l;
    }

    public void Seek(long offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }
}