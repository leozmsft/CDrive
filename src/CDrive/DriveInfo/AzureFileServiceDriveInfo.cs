using CDrive.Util;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.File;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Text;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureFileServiceDriveInfo : AbstractDriveInfo
    {
        public CloudFileClient Client { get; set; }
        public string Endpoint { get; set; }

        public AzureFileServiceDriveInfo(string url, string name)
        {
            var parts = url.Split('?');
            var endpoint = parts[0];
            var dict = ParseValues(parts[1]);
            var accountName = dict["account"];
            var accountKey = dict["key"];

            var cred = new StorageCredentials(accountName, accountKey);
            var account = new CloudStorageAccount(cred, null, null, null, fileStorageUri: new StorageUri(new Uri(endpoint)));
            var client = account.CreateCloudFileClient();

            this.Client = client;
            this.Endpoint = endpoint;
            this.Name = name;
        }

        public override void NewItem(
                            string path,
                            string type,
                            object newItemValue)
        {
            if (string.Equals(type, "Directory", StringComparison.InvariantCultureIgnoreCase))
            {
                this.CreateDirectory(path);
            }
            else if (string.Equals(type, "EmptyFile", StringComparison.InvariantCultureIgnoreCase))
            {
                if (newItemValue != null)
                {
                    var size = 0L;
                    if (long.TryParse(newItemValue.ToString(), out size))
                    {
                        this.CreateEmptyFile(path, size);
                    }
                    else
                    {
                        this.CreateEmptyFile(path, 0);
                    }
                }
            }
            else
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count == 1)
                {
                    this.CreateShare(parts[0]);
                }
                else
                {
                    this.CreateFile(path, newItemValue.ToString());
                }
            }
        }

        public override void GetChildItems(string path, bool recurse)
        {
            var folders = recurse ? new List<string>() : null;

            var items = this.ListItems(path);
            this.HandleItems(items,
                (f) =>
                {
                    f.FetchAttributes();
                    this.RootProvider.WriteItemObject(f, path, true);
                },
                (d) =>
                {
                    this.RootProvider.WriteItemObject(d, path, true);
                    if (recurse)
                    {
                        var p = PathResolver.Combine(path, d.Name);
                        folders.Add(p);
                    }
                },
                (s) =>
                {
                    this.RootProvider.WriteItemObject(s, path, true);
                    if (recurse)
                    {
                        var p = PathResolver.Combine(path, s.Name);
                        folders.Add(p);
                    }
                });

            if (recurse && folders != null)
            {
                foreach (var f in folders)
                {
                    GetChildItems(f, recurse);
                }
            }
        }

        public override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path);
            switch (r.PathType)
            {
                case PathType.AzureFileRoot:
                    var shares = this.ListItems(path);
                    foreach (CloudFileShare s in shares)
                    {
                        this.RootProvider.WriteItemObject(s.Name, path, true);
                    }
                    break;
                case PathType.AzureFileDirectory:
                    var items = r.Directory.ListFilesAndDirectories();
                    var parentPath = PathResolver.Combine(r.Parts);
                    this.HandleItems(items,
                        (f) => this.RootProvider.WriteItemObject(f.Name, PathResolver.Root, false),
                        (d) => this.RootProvider.WriteItemObject(d.Name, PathResolver.Root, true),
                        (s) => { }
                        );
                    break;
                case PathType.AzureFile:
                default:
                    break;
            }
        }

        public override void RemoveItem(string path, bool recurse)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            switch (r.PathType)
            {
                case PathType.AzureFileDirectory:
                    this.DeleteDirectory(r.Directory, recurse);
                    break;
                case PathType.AzureFile:
                    r.File.Delete();
                    break;
                default:
                    break;
            }
        }

        internal IEnumerable<object> ListItems(string path)
        {
            var result = AzureFilePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);

            switch (result.PathType)
            {
                case PathType.AzureFileRoot:
                    return ListShares(this.Client);
                case PathType.AzureFileDirectory:
                    return ListDirectory(result.Directory);
                case PathType.AzureFile:
                    return ListFile(result.File);
                default:
                    return null;
            }
        }

        private IEnumerable<object> ListShares(CloudFileClient client)
        {
            return client.ListShares();
        }

        public void HandleItems(IEnumerable<object> items, Action<CloudFile> fileAction = null, Action<CloudFileDirectory> dirAction = null, Action<CloudFileShare> shareAction = null)
        {
            foreach (var i in items)
            {
                var d = i as CloudFileDirectory;
                if (d != null && dirAction != null)
                {
                    dirAction(d);
                    continue;
                }

                var f = i as CloudFile;
                if (f != null && fileAction != null)
                {
                    fileAction(f);
                    continue;
                }

                var s = i as CloudFileShare;
                if (s != null && shareAction != null)
                {
                    shareAction(s);
                    continue;
                }
            }
        }

        private IEnumerable<IListFileItem> ListFile(CloudFile file)
        {
            return new IListFileItem[] { file };
        }

        private IEnumerable<IListFileItem> ListDirectory(CloudFileDirectory dir)
        {
            var list = dir.ListFilesAndDirectories();
            return list;
        }

        internal void CreateDirectory(string path)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path);

            switch (r.PathType)
            {
                case PathType.AzureFileRoot:
                    return;
                case PathType.AzureFileDirectory:
                    CreateDirectoryAndShare(r.Directory);
                    return;
                case PathType.AzureFile:
                    throw new Exception("File " + path + " already exists.");
                default:
                    return;
            }
        }

        internal void CreateDirectoryAndShare(CloudFileDirectory dir)
        {
            var share = dir.Share;
            if (!share.Exists())
            {
                share.Create();
            }

            CreateParentDirectory(dir, share.GetRootDirectoryReference());
            if (!dir.Exists())
            {
                dir.Create();
            }
        }

        private void CreateParentDirectory(CloudFileDirectory dir, CloudFileDirectory rootDir)
        {
            var p = dir.Parent;
            if (p == null || p.Uri == rootDir.Uri)
            {
                return;
            }

            if (p.Exists())
            {
                return;
            }

            CreateParentDirectory(p, rootDir);

            p.Create();
        }

        internal void CreateEmptyFile(string path, long size)
        {
            var file = GetFile(path);
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            file.Create(size);
        }

        internal void CreateFile(string path, string content)
        {
            var file = GetFile(path);
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            CreateDirectoryAndShare(file.Parent);
            file.UploadText(content);
        }

        public override IContentReader GetContentReader(string path)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, hint: PathType.AzureFile, skipCheckExistence: false);
            if (r.PathType == PathType.AzureFile)
            {
                var reader = new AzureFileReader(GetFile(path));
                return reader;
            }

            return null;
        }

        public CloudFile GetFile(string path)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, hint: PathType.AzureFile);
            if (r.PathType == PathType.AzureFile)
            {
                return r.File;
            }

            return null;
        }

        internal void DeleteDirectory(CloudFileDirectory dir, bool recurse)
        {
            if (dir.Share.GetRootDirectoryReference().Uri == dir.Uri)
            {
                dir.Share.Delete();
                return;
            }

            var items = dir.ListFilesAndDirectories();
            if (recurse)
            {
                HandleItems(items,
                    (f) => f.Delete(),
                    (d) => DeleteDirectory(d, recurse),
                    (s) => s.Delete());

                dir.Delete();
            }
            else
            {
                if (items.Count() == 0)
                {
                    dir.Delete();
                }
                else
                {
                    throw new Exception("The directory is not empty. Please specify -recurse to delete it.");
                }
            }
        }

        internal CloudFileShare CreateShare(string shareName)
        {
            var share = this.Client.GetShareReference(shareName);
            if (!share.Exists())
            {
                share.Create();
                return share;
            }

            throw new Exception("Share " + shareName + " already exists");
        }

        internal void Download(string path, string destination)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            var targetIsDir = Directory.Exists(destination);

            switch (r.PathType)
            {
                case PathType.AzureFile:
                    if (targetIsDir)
                    {
                        destination = PathResolver.Combine(destination, r.Parts.Last());
                    }

                    r.File.DownloadToFile(destination, FileMode.CreateNew);
                    break;
                case PathType.AzureFileDirectory:
                    if (string.IsNullOrEmpty(r.Directory.Name))
                    {
                        //at share level
                        this.DownloadShare(r.Share, destination);
                    }
                    else
                    {
                        DownloadDirectory(r.Directory, destination);
                    }
                    break;
                case PathType.AzureFileRoot:
                    var shares = this.Client.ListShares();
                    foreach (var share in shares)
                    {
                        this.DownloadShare(share, destination);
                    }
                    break;
                default:
                    break;
            }
        }

        private void DownloadShare(CloudFileShare share, string destination)
        {
            destination = PathResolver.Combine(destination, share.Name);
            Directory.CreateDirectory(destination);

            var dir = share.GetRootDirectoryReference();
            var items = dir.ListFilesAndDirectories();

            this.HandleItems(items,
                (f) =>
                {
                    f.DownloadToFile(PathResolver.Combine(destination, f.Name), FileMode.CreateNew);
                },
                (d) =>
                {
                    DownloadDirectory(d, destination);
                },
                (s) => { });
        }

        internal void DownloadDirectory(CloudFileDirectory dir, string destination)
        {
            destination = Path.Combine(destination, dir.Name);
            Directory.CreateDirectory(destination);
            var items = dir.ListFilesAndDirectories();
            this.HandleItems(items,
                (f) =>
                {
                    f.DownloadToFile(PathResolver.Combine(destination, f.Name), FileMode.CreateNew);
                },
                (d) =>
                {
                    DownloadDirectory(d, destination);
                },
                (s) => { });
        }

        internal void Upload(string localPath, string targePath)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, targePath, skipCheckExistence: false);
            var localIsDirectory = Directory.Exists(localPath);
            var local = PathResolver.SplitPath(localPath);
            switch (r.PathType)
            {
                case PathType.AzureFileRoot:
                    if (localIsDirectory)
                    {
                        var share = CreateShare(local.Last());
                        var dir = share.GetRootDirectoryReference();
                        foreach (var f in Directory.GetFiles(localPath))
                        {
                            UploadFile(f, dir);
                        }

                        foreach (var d in Directory.GetDirectories(localPath))
                        {
                            UploadDirectory(d, dir);
                        }
                    }
                    else
                    {
                        throw new Exception("Cannot upload file as file share.");
                    }
                    break;
                case PathType.AzureFileDirectory:
                    if (localIsDirectory)
                    {
                        UploadDirectory(localPath, r.Directory);
                    }
                    else
                    {
                        UploadFile(localPath, r.Directory);
                    }
                    break;
                case PathType.AzureFile:
                default:
                    break;
            }

        }

        private void UploadDirectory(string localPath, CloudFileDirectory dir)
        {
            var localDirName = Path.GetFileName(localPath);
            var subdir = dir.GetDirectoryReference(localDirName);
            subdir.Create();

            foreach (var f in Directory.GetFiles(localPath))
            {
                UploadFile(f, subdir);
            }

            foreach (var d in Directory.GetDirectories(localPath))
            {
                UploadDirectory(d, subdir);
            }
        }

        private void UploadFile(string localFile, CloudFileDirectory dir)
        {
            var file = Path.GetFileName(localFile);
            var f = dir.GetFileReference(file);
            var condition = new AccessCondition();
            f.UploadFromFile(localFile, FileMode.CreateNew);
        }

        public override bool HasChildItems(string path)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, hint: PathType.AzureFileDirectory, skipCheckExistence: false);
            return r.Exists();
        }

        public override bool IsValidPath(string path)
        {
            throw new NotImplementedException();
        }

        public override bool ItemExists(string path)
        {
            if (PathResolver.IsLocalPath(path))
            {
                path = PathResolver.ConvertToRealLocalPath(path);
                return File.Exists(path) || Directory.Exists(path);
            }

            try
            {
                var r = AzureFilePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
                var exists = r.Exists();
                return exists;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override bool IsItemContainer(string path)
        {
            if (PathResolver.IsLocalPath(path))
            {
                return true;
            }

            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return true;
            }

            try
            {
                var r = AzureFilePathResolver.ResolvePath(this.Client, path, hint: PathType.AzureFileDirectory, skipCheckExistence: false);
                return r.Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override void GetProperty(string path, System.Collections.ObjectModel.Collection<string> providerSpecificPickList)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            switch (r.PathType)
            {
                case PathType.AzureFile:
                    r.File.FetchAttributes();
                    this.RootProvider.WriteItemObject(r.File.Properties, path, false);
                    this.RootProvider.WriteItemObject(r.File.Metadata, path, false);
                    break;
                case PathType.AzureFileDirectory:
                    if (r.Parts.Count() == 1)
                    {
                        r.Share.FetchAttributes();
                        this.RootProvider.WriteItemObject(r.Share.Properties, path, true);
                        this.RootProvider.WriteItemObject(r.Share.Metadata, path, true);
                    }
                    else
                    {
                        r.Directory.FetchAttributes();
                        this.RootProvider.WriteItemObject(r.Directory.Properties, path, true);
                    }
                    break;
                default:
                    break;
            }
        }

        public override void SetProperty(string path, PSObject propertyValue)
        {
            var r = AzureFilePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            switch (r.PathType)
            {
                case PathType.AzureFile:
                    r.File.FetchAttributes();
                    MergeProperties(r.File.Metadata, propertyValue.Properties);
                    r.File.SetMetadata();
                    break;
                case PathType.AzureFileDirectory:
                    if (r.Parts.Count() == 1)
                    {
                        r.Share.FetchAttributes();
                        MergeProperties(r.Share.Metadata, propertyValue.Properties);
                        r.Share.SetMetadata();
                    }
                    else
                    {
                        throw new Exception("Setting metadata/properties for directory is not supported");
                    }
                    break;
                default:
                    break;
            }
        }

        private void MergeProperties(IDictionary<string, string> target, PSMemberInfoCollection<PSPropertyInfo> source)
        {
            foreach (var info in source)
            {
                var name = info.Name;
                if (target.ContainsKey(name))
                {
                    target.Remove(name);
                }

                target.Add(name, info.Value.ToString());
            }
        }

        public override Stream CopyFrom(string path)
        {
            throw new NotImplementedException();
        }

        public override Stream CopyTo(string path, string name)
        {
            throw new NotImplementedException();
        }

        public override IList<string> GetChildNamesList(string path, PathType type = PathType.Any)
        {
            throw new NotImplementedException();
        }
    }

    class AzureFileReader : IContentReader
    {
        private CloudFile File { get; set; }
        private long length = 0;
        private const int unit = 1024 * 64; //64KB

        private byte[] buffer = new byte[unit];
        private int bufferSize = 0;
        private int pointer = -1;
        private long fileOffset = 0;
        public AzureFileReader(CloudFile file)
        {
            this.File = file;
            this.File.FetchAttributes();
            length = this.File.Properties.Length;
        }
        public void Close()
        {
        }

        public System.Collections.IList Read(long readCount)
        {
            if (pointer >= length)
            {
                return null;
            }

            if (pointer == -1)
            {
                ReadFile(0);
                pointer = 0;
                fileOffset = 0;
            }

            var l = new List<string>();
            while (l.Count < readCount)
            {
                var line = ReadLine();
                l.Add(line);
            }

            return l;
        }

        private string ReadLine()
        {
            if (pointer == this.bufferSize)
            {
                ReadFile(this.fileOffset + this.bufferSize);
                pointer = 0;
            }

            for (var i = this.pointer; i < this.bufferSize; ++i)
            {
                if (this.buffer[i] == '\n')
                {
                    var s = Encoding.UTF8.GetString(this.buffer, this.pointer, i - this.pointer);
                    this.pointer = i + 1;
                    return s;
                }

                if (i == this.bufferSize - 1)
                {
                    //if it's the end of the file, then force print this line
                    if (i == this.length - 1)
                    {

                        var s = Encoding.UTF8.GetString(this.buffer, this.pointer, i + 1 - this.pointer);
                        this.pointer += i + 1;
                        return s;
                    }
                    else
                    {
                        //if it's just the end of this block, just reload the file and read again
                        ReadFile(this.fileOffset + this.pointer);
                        this.pointer = 0;
                        return ReadLine();
                    }
                }
            }

            return string.Empty;
        }

        private void ReadFile(long start = 0)
        {
            this.bufferSize = unit;
            if (this.bufferSize + start > length)
            {
                this.bufferSize = (int)(length - start);
            }

            if (this.bufferSize <= 0)
            {
                return;
            }

            this.File.DownloadRangeToByteArray(this.buffer, 0, start, this.bufferSize);
            this.fileOffset = start;
        }

        public void Seek(long offset, System.IO.SeekOrigin origin)
        {
            this.pointer = (int)offset;
        }

        public void Dispose()
        {
        }
    }
}
