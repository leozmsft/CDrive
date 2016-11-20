using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureBlobServiceDriveInfo2 : AbstractDriveInfo
    {
        public CloudBlobClient Client { get; set; }
        public string Endpoint { get; set; }

        public AzureBlobServiceDriveInfo2(string url, string name)
        {
            var parts = url.Split('?');
            var endpoint = parts[0];
            var dict = ParseValues(parts[1]);
            var accountName = dict["account"];
            var accountKey = dict["key"];

            var cred = new StorageCredentials(accountName, accountKey);
            var account = new CloudStorageAccount(cred, new StorageUri(new Uri(endpoint)), null, null, null);
            var client = account.CreateCloudBlobClient();

            this.Client = client;
            this.Endpoint = endpoint;
            this.Name = name;
        }

        public override void NewItem(
                            string path,
                            string type,
                            object newItemValue)
        {
            if (string.Equals(type, "PageBlob", StringComparison.InvariantCultureIgnoreCase))
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
            else if (string.Equals(type, "BlockBlob", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count == 1)
                {
                    this.CreateContainerIfNotExists(parts[0]);
                }
                else
                {
                    this.CreateBlockBlob(path, newItemValue.ToString());
                }
            }
            else if (string.Equals(type, "AppendBlob", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count == 1)
                {
                    this.CreateContainerIfNotExists(parts[0]);
                }
                else
                {
                    this.CreateAppendBlob(path, newItemValue.ToString());
                }
            }
        }
        

        public override void GetChildItems(string path, bool recurse)
        {
            var items = this.ListItems(path);
            foreach (var i in items)
            {
                this.RootProvider.WriteItemObject(i, path, true);
            }
        }

        public override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            switch (r.PathType)
            {
                case PathType.AzureBlobRoot:
                    var containers = ListContainers();
                    foreach (var container in containers)
                    {
                        this.RootProvider.WriteItemObject(container, path, true);
                    }
                    break;
                default:
                    break;
            }
        }

        public override void RemoveItem(string path, bool recurse)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            switch (r.PathType)
            {
                case PathType.Container:
                    r.Container.DeleteIfExists();
                    break;
                case PathType.AzureBlobQuery:
                    var files = ListItems(path) as IEnumerable<IListBlobItem>;
                    // TODO

                    break;
                default:
                    break;
            }
        }

        internal IEnumerable<object> ListItems(string path)
        {
            var result = AzureBlobPathResolver2.ResolvePath(this.Client, path);

            switch (result.PathType)
            {
                case PathType.AzureBlobRoot:
                    return ListContainers();
                case PathType.Container:
                case PathType.AzureBlobQuery:
                    return ListFiles(result.Container, result.BlobQuery);
                default:
                    return null;
            }
        }

        private IEnumerable<IListBlobItem> ListFiles(CloudBlobContainer container, BlobQuery blobQuery)
        {
            if (blobQuery.MaxResult == -1)
            {
                return container.ListBlobs(blobQuery.Prefix, true, blobQuery.BlobListingDetails);
            }
            else
            {
                var seg = container.ListBlobsSegmented(blobQuery.Prefix, true, blobQuery.BlobListingDetails, blobQuery.MaxResult, null, null, null);
                return seg.Results;
            }
        }

        private IEnumerable<CloudBlobContainer> ListContainers()
        {
            return this.Client.ListContainers();
        }

        internal CloudBlobContainer CreateContainerIfNotExists(string name)
        {
            var container = this.Client.GetContainerReference(name);
            if (!container.Exists())
            {
                container.Create();
            }

            return container;
        }

        internal void CreateEmptyFile(string path, long size)
        {
            var file = GetPageBlob(path);
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            file.Create(size);
        }

        private CloudPageBlob GetPageBlob(string path)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            return r.Container.GetPageBlobReference(r.BlobQuery.Prefix);
        }

        internal void CreateBlockBlob(string path, string content)
        {
            var file = GetBlockBlob(path);
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            CreateContainerIfNotExists(file.Container.Name);
            file.UploadText(content);
        }

        private CloudBlockBlob GetBlockBlob(string path)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            return r.Container.GetBlockBlobReference(r.BlobQuery.Prefix);
        }

        internal void CreateAppendBlob(string path, string content)
        {
            var file = GetAppendBlob(path);
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            CreateContainerIfNotExists(file.Container.Name);
            if (file.Exists())
            {
                file.AppendText(content);
            }
            else
            {
                file.UploadText(content);
            }
        }

        private CloudAppendBlob GetAppendBlob(string path)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            return r.Container.GetAppendBlobReference(r.BlobQuery.Prefix);
        }

        public override IContentReader GetContentReader(string path)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            var blob = r.Container.GetBlobReference(r.BlobQuery.Prefix);
            if (r.PathType == PathType.AzureBlobQuery)
            {
                var firstFile = this.ListFiles(r.Container, r.BlobQuery).FirstOrDefault();
                if (firstFile != default(IListBlobItem))
                {
                    var reader = new AzureBlobReader(new CloudBlob(firstFile.Uri, this.Client.Credentials));
                    return reader;
                }
            }

            return null;
        }


        public override bool HasChildItems(string path)
        {
            return true;
        }

        public override bool IsValidPath(string path)
        {
            throw new NotImplementedException();
        }

        public override bool ItemExists(string path)
        {
            return true;
        }

        public override bool IsItemContainer(string path)
        {
            return true;
        }

        public override void GetProperty(string path, System.Collections.ObjectModel.Collection<string> providerSpecificPickList)
        {
            
        }

        public override void SetProperty(string path, PSObject propertyValue)
        {
            
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
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path);
            var files = this.ListFiles(r.Container, r.BlobQuery);
            if (files.Count() > 0)
            {
                var blob = new CloudBlob(files.First().Uri, this.Client.Credentials);
                return blob.OpenRead();
            }

            return null;
        }

        public override Stream CopyTo(string path, string name)
        {
            var r = AzureBlobPathResolver2.ResolvePath(this.Client, path + PathResolver.DirSeparator + name);
            if (r.PathType == PathType.AzureBlobQuery)
            {
                var prefix = r.BlobQuery.Prefix;
                var blob = new CloudBlockBlob(new Uri(r.Container.Uri.ToString() + "/" + prefix), this.Client.Credentials);
                return blob.OpenWrite();
            }

            return null;
        }

        public override IList<string> GetChildNamesList(string path, PathType type)
        {
            throw new NotImplementedException();
        }
    }
}
