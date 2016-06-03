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
    public class AzureBlobServiceDriveInfo : AbstractDriveInfo
    {
        public CloudBlobClient Client { get; set; }
        public string Endpoint { get; set; }

        public AzureBlobServiceDriveInfo(string url, string name)
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
            if (string.Equals(type, "Directory", StringComparison.InvariantCultureIgnoreCase))
            {
                this.CreateDirectory(path);
            }
            else if (string.Equals(type, "PageBlob", StringComparison.InvariantCultureIgnoreCase))
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
            else if (string.Equals(type, "RandomPages", StringComparison.InvariantCultureIgnoreCase))
            {
                //fill page blob with random data, each page data is 512Byte, and count is required
                //e.g. ni PageBlob -type RandomPages -value <count>
                if (newItemValue != null)
                {
                    var size = 0L;
                    if (long.TryParse(newItemValue.ToString(), out size))
                    {
                        this.FillDataInPageBlob(path, size);
                    }
                    else
                    {
                        this.RootProvider.WriteWarning("Value is required.");
                    }
                }

            }
            else if (string.Equals(type, "ListPages", StringComparison.InvariantCultureIgnoreCase))
            {
                //List page ranges in page blob
                //e.g. ni pageBlob -type ListPages
                this.ListPageRanges(path);
            }
            else if (string.Equals(type, "ContainerSAStoken", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 0)
                {
                    var containerName = parts[0];
                    var container = this.Client.GetContainerReference(containerName);
                    var policyName = string.Empty;
                    var policy = CreateBlobPolicy(newItemValue as string, ref policyName);

                    if (policyName != string.Empty) //policy-based SAStoken
                    {
                        var token = container.GetSharedAccessSignature(policy, policyName);
                        this.RootProvider.WriteItemObject(token, path, false);
                    }
                    else
                    {
                        var token = container.GetSharedAccessSignature(policy);
                        this.RootProvider.WriteItemObject(token, path, false);
                    }
                }
            }
            else if (string.Equals(type, "BlobSAStoken", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 1)
                {
                    var containerName = parts[0];
                    var container = this.Client.GetContainerReference(containerName);
                    var blob = container.GetBlobReference(PathResolver.GetSubpath(path));
                    var policyName = string.Empty;
                    var policy = CreateBlobPolicy(newItemValue as string, ref policyName);

                    if (policyName != string.Empty) //policy-based SAStoken
                    {
                        var token = blob.GetSharedAccessSignature(policy, policyName);
                        this.RootProvider.WriteItemObject(blob.StorageUri.PrimaryUri.ToString() + token, path, false);
                    }
                    else
                    {
                        var token = blob.GetSharedAccessSignature(policy);
                        this.RootProvider.WriteItemObject(blob.StorageUri.PrimaryUri.ToString() + token, path, false);
                    }
                }
            }
            else if (string.Equals(type, "Policy", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 0)
                {
                    var containerName = parts[0];
                    var container = this.Client.GetContainerReference(containerName);
                    var policyName = parts.Last();
                    var policyPlaceHolder = string.Empty;
                    var policy = CreateBlobPolicy(newItemValue as string, ref policyPlaceHolder);

                    var permissions = container.GetPermissions();
                    if (permissions.SharedAccessPolicies.ContainsKey(policyName))
                    {
                        if (!this.RootProvider.ShouldContinue(string.Format("Should continue to update existing policy {0}?", policyName), "Policy existed"))
                        {
                            this.RootProvider.WriteWarning("Cancelled");
                            return;
                        }
                        else
                        {
                            permissions.SharedAccessPolicies[policyName] = policy;
                        }
                    }
                    else
                    {
                        permissions.SharedAccessPolicies.Add(policyName, policy);
                    }

                    this.RootProvider.WriteWarning(string.Format("Policy {0} updated or added.", policyName));
                }

                return;
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
            else if (string.Equals(type, "Permission", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 0)
                {
                    switch (newItemValue.ToString().ToLowerInvariant())
                    {
                        case "publiccontainer":
                            this.SetContainerPermission(containerName: parts[0], toBePublic: true);
                            this.RootProvider.WriteWarning(string.Format("Done setting container {0} to be public", parts[0]));
                            break;
                        case "privatecontainer":
                            this.SetContainerPermission(containerName: parts[0], toBePublic: false);
                            this.RootProvider.WriteWarning(string.Format("Done setting container {0} to be private", parts[0]));
                            break;
                        default:
                            this.RootProvider.WriteWarning("Invalid value. Supported values: PublicContainer, PrivateContainer");
                            break;

                    }
                }
                else
                {
                    this.RootProvider.WriteWarning("Please do this operation in a container.");
                }
            }
            else if (string.Equals(type, "AsyncCopy", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 1)
                {
                    if (newItemValue != null && newItemValue.ToString().Length > 0)
                    {
                        this.AsyncCopy(path, newItemValue.ToString());
                        this.RootProvider.WriteWarning("Started Async copy");
                    }
                    else
                    {
                        this.RootProvider.WriteWarning("Must specify the source url in -value.");
                    }

                }
                else
                {
                    this.RootProvider.WriteWarning("Please do this operation in a container and specify the target blob name.");
                }
            }
            else if (string.Equals(type, "CopyStatus", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 1)
                {
                    this.ShowCopyStatus(path);
                }
                else
                {
                    this.RootProvider.WriteWarning("Please do this operation in a container and specify the target blob name.");
                }
            }
            else if (string.Equals(type, "CancelCopy", StringComparison.InvariantCultureIgnoreCase))
            {
                var parts = PathResolver.SplitPath(path);
                if (parts.Count > 1)
                {
                    if (newItemValue != null && newItemValue.ToString().Length > 0)
                    {
                        this.CancelCopy(path, newItemValue.ToString());
                    }
                    else
                    {
                        this.RootProvider.WriteWarning("Must specify the copy ID in -value.");
                    }

                }
                else
                {
                    this.RootProvider.WriteWarning("Please do this operation in a container and specify the target blob name.");
                }
            }
            else if (string.Equals(type, "Etag", StringComparison.InvariantCultureIgnoreCase))
            {
                this.ShowEtag(path);
            }
            else
            {
                this.RootProvider.WriteWarning("No operation type is specified by <-type>.");
                this.RootProvider.WriteWarning("Supported operation type: ");
                this.RootProvider.WriteWarning("\tDirectory:            Create directory <-path>");
                this.RootProvider.WriteWarning("\tPageBlob:             Create page blob <-path> with size <-value>");
                this.RootProvider.WriteWarning("\tRandomPages:          Fill page blob <-path> with size <-value> using random data");
                this.RootProvider.WriteWarning("\tListPages:            List page ranges in page blob <-path>");
                this.RootProvider.WriteWarning("\tBlockBlob:            Create block blob <-path> with contents <-value>");
                this.RootProvider.WriteWarning("\tAppendBlob:           Create append blob <-path> with contents <-value>");
                this.RootProvider.WriteWarning("\tContainerSAStoken:    Expected <-value>: start=<days>;expiry=<days>;policy=<policy>;p=rwdl");
                this.RootProvider.WriteWarning("\tBlobSAStoken:         Expected <-value>: start=<days>;expiry=<days>;policy=<policy>;p=rwdl");
                this.RootProvider.WriteWarning("\tPolicy:               Expected <-value>: start=<days>;expiry=<days>;policy=<policy>;p=rwdl");
                this.RootProvider.WriteWarning("\tPermission:           Supported <-value>: PublicContainer, PrivateContainer");
                this.RootProvider.WriteWarning("\tAsyncCopy:            AsyncCopy blob from url <-value> to <-path>");
                this.RootProvider.WriteWarning("\tCopyStatus:           Show copy status of blob <-path>");
                this.RootProvider.WriteWarning("\tCancelCopy:           Specify the destBlob in <-path> and the copy ID in <-value>.");
                this.RootProvider.WriteWarning("\tEtag:                 Show the Etag of the blob <-path> ");
            }
        }

        private void ListPageRanges(string path)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count > 1)
            {
                var blob = this.Client.GetContainerReference(parts[0]).GetPageBlobReference(PathResolver.GetSubpath(path));
                if (!blob.Exists())
                {
                    this.RootProvider.WriteWarning("PageBlob " + path + " does not exist.");
                    return;
                }

                blob.FetchAttributes();
                var totalLength = blob.Properties.Length;

                var count = 0L;
                var offset = 0L;
                var length = 4 * 1024 * 1024L; //4MB
                while (true)
                {
                    PageRange page = null;
                    var round = 0L;

                    length = (offset + length > totalLength) ? (totalLength - offset) : length;
                    foreach (var r in blob.GetPageRanges(offset, length)) {
                        page = r;
                        round++;
                        this.RootProvider.WriteWarning(string.Format("[{3}]\t[{0} - {1}] {2}", r.StartOffset, r.EndOffset, r.EndOffset - r.StartOffset + 1, count++));
                    }

                    if (offset + length >= totalLength)
                    {
                        //reach the end
                        break;
                    }

                    //1. move offset
                    offset += length;

                    //2. calculate next length
                    if (round < 200)
                    {
                        length *= 2;
                    }
                    else if (round > 500)
                    {
                        length /= 2;
                    }
                }
            }
            else
            {
                this.RootProvider.WriteWarning("Please specify the page blob path.");
            }
        }

        private void FillDataInPageBlob(string path, long count)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count > 1)
            {
                var blob = this.Client.GetContainerReference(parts[0]).GetPageBlobReference(PathResolver.GetSubpath(path));
                if (!blob.Exists())
                {
                    this.RootProvider.WriteWarning("PageBlob " + path + " does not exist.");
                    return;
                }

                blob.FetchAttributes();
                var total = blob.Properties.Length / 512;
                var data = new byte[512];
                var random = new Random();
                random.NextBytes(data);

                this.RootProvider.WriteWarning("Start writing pages...");
                var tasks = new Task[count];

                for (var i = 0; i < count; ++i) {
                    var p = (long)(random.NextDouble() * total);

                    var task = blob.WritePagesAsync(new MemoryStream(data), p * 512, null);
                    tasks[i] = task;
                }

                this.RootProvider.WriteWarning("Waiting writing pages...");
                Task.WaitAll(tasks);
                this.RootProvider.WriteWarning("Completed writing pages...");
            }
            else
            {
                this.RootProvider.WriteWarning("Please specify the page blob path.");
            }
        }

        private void ShowEtag(string path)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count > 1)
            {
                var blob = this.Client.GetContainerReference(parts[0]).GetBlobReference(PathResolver.GetSubpath(path));
                blob.FetchAttributes();
                this.RootProvider.WriteItemObject(blob.Properties.ETag, path, false);
            }
            else
            {
                this.RootProvider.WriteWarning("Please specify the target blob.");
            }
        }

        private void CancelCopy(string path, string copyId)
        {
            var parts = PathResolver.SplitPath(path);
            var container = this.Client.GetContainerReference(parts[0]);
            var destBlob = container.GetBlobReference(PathResolver.GetSubpath(path));
            destBlob.AbortCopy(copyId);
        }

        private void ShowCopyStatus(string path)
        {
            var parts = PathResolver.SplitPath(path);
            var container = this.Client.GetContainerReference(parts[0]);
            var destBlob = container.GetBlobReference(PathResolver.GetSubpath(path));
            destBlob.FetchAttributes();
            var state = destBlob.CopyState;
            if (state != null)
            {
                this.RootProvider.WriteWarning(string.Format("Copy is {0}\r\nId: {1}\r\nBytes Copied: {2}/{3}",
                   state.Status.ToString(),
                   state.CopyId,
                   state.BytesCopied,
                   state.TotalBytes));
            } else
            {
                this.RootProvider.WriteWarning(string.Format("CopyStatus is null"));
            }
        }

        private void AsyncCopy(string path, string url)
        {
            var parts = PathResolver.SplitPath(path);
            var container = this.Client.GetContainerReference(parts[0]);
            var destBlob = container.GetBlobReference(PathResolver.GetSubpath(path));
            var copyId = destBlob.StartCopy(new Uri(url));
            this.RootProvider.WriteItemObject(new { CopyId = copyId }, path, false);
        }

        private void SetContainerPermission(string containerName, bool toBePublic)
        {
            var container = this.Client.GetContainerReference(containerName);
            var permissions = container.GetPermissions();
            permissions.PublicAccess = toBePublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off;
            container.SetPermissions(permissions);
        }

        private SharedAccessBlobPolicy CreateBlobPolicy(string permissions, ref string policyName)
        {
            if (permissions == null)
            {
                throw new Exception("Value should be set. Expected: start=<days>;expiry=<days>;policy=<policy>;p=rwdl");
            }

            ///permissions: start=0;expiry=30;policy=hello;p=rwdl
            ///
            var set = permissions.Split(';');
            var policy = new SharedAccessBlobPolicy();
            foreach (var s in set)
            {
                var p = s.Split('=');
                switch (p[0].ToLowerInvariant())
                {
                    case "expiry":
                        policy.SharedAccessExpiryTime = DateTime.Now.AddDays(Convert.ToInt32(p[1]));
                        break;
                    case "start":
                        policy.SharedAccessStartTime = DateTime.Now.AddDays(Convert.ToInt32(p[1]));
                        break;
                    case "policy":
                        policyName = p[1];
                        break;
                    case "p":
                        for (var i = 0; i < p[1].Length; ++i)
                        {
                            switch (Char.ToLowerInvariant(p[1][i]))
                            {
                                case 'r':
                                    policy.Permissions |= SharedAccessBlobPermissions.Read;
                                    break;
                                case 'w':
                                    policy.Permissions |= SharedAccessBlobPermissions.Write;
                                    break;
                                case 'd':
                                    policy.Permissions |= SharedAccessBlobPermissions.Delete;
                                    break;
                                case 'l':
                                    policy.Permissions |= SharedAccessBlobPermissions.List;
                                    break;
                            }
                        }
                        break;
                    default:
                        throw new Exception("Unknown parameter: " + p[0] + ". Expected: start=<days>;expiry=<days>;policy=<policy>;p=rwdl");
                }
            }

            return policy;
        }

        public override void GetChildItems(string path, bool recurse)
        {
            var folders = recurse ? new List<string>() : null;

            var items = this.ListItems(path);
            this.HandleItems(items,
                (b) =>
                {
                    this.RootProvider.WriteItemObject(b, path, true);
                },
                (d) =>
                {
                    this.RootProvider.WriteItemObject(d, path, true);
                    if (recurse)
                    {
                        var name = PathResolver.SplitPath(d.Prefix).Last();
                        var p = PathResolver.Combine(path, name);
                        folders.Add(p);
                    }
                },
                (c) =>
                {
                    this.RootProvider.WriteItemObject(c, path, true);
                    if (recurse)
                    {
                        var p = PathResolver.Combine(path, c.Name);
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
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path);
            switch (r.PathType)
            {
                case PathType.AzureBlobRoot:
                    var shares = this.ListItems(path);
                    foreach (CloudBlobContainer s in shares)
                    {
                        this.RootProvider.WriteItemObject(s.Name, path, true);
                    }
                    break;
                case PathType.AzureBlobDirectory:
                    ListAndHandle(r.Directory,
                        blobAction: b => this.RootProvider.WriteItemObject(b.Name, b.Parent.Uri.ToString(), false),
                        dirAction: d => this.RootProvider.WriteItemObject(d.Prefix, d.Parent.Uri.ToString(), false)
                        );
                    break;
                case PathType.AzureBlobBlock:
                default:
                    break;
            }
        }

        public override void RemoveItem(string path, bool recurse)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            switch (r.PathType)
            {
                case PathType.AzureBlobDirectory:
                    if (r.Parts.Count == 1)
                    {
                        r.Container.Delete();
                        return;
                    }

                    this.DeleteDirectory(r.Directory, recurse);
                    break;
                case PathType.AzureBlobBlock:
                case PathType.AzureBlobPage:
                case PathType.AzureBlobAppend:
                    r.Blob.Delete();
                    break;
                default:
                    break;
            }
        }

        internal IEnumerable<object> ListItems(string path)
        {
            var result = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);

            switch (result.PathType)
            {
                case PathType.AzureBlobRoot:
                    return ListContainers(this.Client);
                case PathType.AzureBlobDirectory:
                    return ListDirectory(result.Directory);
                case PathType.AzureBlobBlock:
                    return ListBlob(result.Blob);
                default:
                    return null;
            }
        }

        private IEnumerable<object> ListContainers(CloudBlobClient client)
        {
            return client.ListContainers();
        }

        public bool IsDirEmpty(CloudBlobDirectory dir)
        {
            var r = dir.ListBlobsSegmented(true, BlobListingDetails.None, 1, null, null, null);
            return r.Results.Count() == 0;
        }

        public void ListAndHandle(CloudBlobDirectory dir, 
            bool flatBlobListing = false,
            Action<ICloudBlob> blobAction = null, 
            Action<CloudBlobDirectory> dirAction = null,
            Action<CloudBlobContainer> containerAction = null)
        {
            BlobContinuationToken token = null;
            while (true)
            {
                var r = dir.ListBlobsSegmented(flatBlobListing, BlobListingDetails.None, 10, token, null, null);
                token = r.ContinuationToken;
                var blobs = r.Results;
                this.HandleItems(blobs, blobAction, dirAction, containerAction, dir.Prefix);

                if (token == null)
                {
                    break;
                }
            }
        }

        public void HandleItems(IEnumerable<object> items, 
            Action<ICloudBlob> blobAction = null, 
            Action<CloudBlobDirectory> dirAction = null, 
            Action<CloudBlobContainer> containerAction = null,
            string excludedName = null)
        {
            if (items == null)
            {
                return;
            }

            foreach (var i in items)
            {
                var d = i as CloudBlobDirectory;
                if (d != null)
                {
                    dirAction(d);
                    continue;
                }

                var f = i as ICloudBlob;
                if (f != null)
                {
                    if (f.Name != excludedName)
                    {
                        blobAction(f);
                    }
                    continue;
                }

                var s = i as CloudBlobContainer;
                if (s != null)
                {
                    containerAction(s);
                    continue;
                }
            }
        }

        private IEnumerable<IListBlobItem> ListBlob(ICloudBlob file)
        {
            return new IListBlobItem[] { file };
        }

        private IEnumerable<object> ListDirectory(CloudBlobDirectory dir)
        {
            var list = new List<object>();
            ListAndHandle(dir,
                blobAction: b => list.Add(b),
                dirAction: d => list.Add(d));
            return list;
        }

        internal void CreateDirectory(string path)
        {
            var parts = PathResolver.SplitPath(path);

            
            if (parts.Count > 0)
            {
                var container = CreateContainerIfNotExists(parts[0]);

                if (parts.Count > 1)
                {
                    var dirObj = container.GetPageBlobReference(PathResolver.GetSubpath(path) + PathResolver.DirSeparator);
                    dirObj.Create(0);
                }
            }
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
            var file = GetBlob(path, PathType.AzureBlobPage) as CloudPageBlob;
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            file.Create(size);
        }

        internal void CreateBlockBlob(string path, string content)
        {
            var file = GetBlob(path, PathType.AzureBlobBlock) as CloudBlockBlob;
            if (file == null)
            {
                throw new Exception("Path " + path + " is not a valid file path.");
            }

            CreateContainerIfNotExists(file.Container.Name);
            file.UploadText(content);
        }

        internal void CreateAppendBlob(string path, string content)
        {
            var file = GetBlob(path, PathType.AzureBlobAppend) as CloudAppendBlob;
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

        public override IContentReader GetContentReader(string path)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            if (r.PathType == PathType.AzureBlobBlock 
                || r.PathType == PathType.AzureBlobPage
                || r.PathType == PathType.AzureBlobAppend)
            {
                var reader = new AzureBlobReader(GetBlob(path, r.PathType));
                return reader;
            }

            return null;
        }

        public ICloudBlob GetBlob(string path, PathType expectedType)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, hint: expectedType);
            if (r.PathType == expectedType)
            {
                return r.Blob;
            }

            return null;
        }

        internal void DeleteDirectory(CloudBlobDirectory dir, bool recurse)
        {
            if (recurse)
            {
                ListAndHandle(dir,
                    flatBlobListing: true,
                    blobAction: (b) => b.Delete());

                //query if directory blob exists, and delete it
                var blobs = dir.ListBlobs();
                HandleItems(blobs,
                    blobAction: (b) => b.Delete());
            }
            else
            {
                if (!IsDirEmpty(dir))
                {
                    throw new Exception("The directory is not empty. Please specify -recurse to delete it.");
                }
            }
        }

        public override bool HasChildItems(string path)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, hint: PathType.AzureBlobDirectory, skipCheckExistence: false);
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
                var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
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
                var r = AzureBlobPathResolver.ResolvePath(this.Client, path, hint: PathType.AzureBlobDirectory, skipCheckExistence: false);
                return r.Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override void GetProperty(string path, System.Collections.ObjectModel.Collection<string> providerSpecificPickList)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            switch (r.PathType)
            {
                case PathType.AzureBlobBlock:
                    r.Blob.FetchAttributes();
                    this.RootProvider.WriteItemObject(r.Blob.Properties, path, false);
                    this.RootProvider.WriteItemObject(r.Blob.Metadata, path, false);
                    break;
                case PathType.AzureBlobDirectory:
                    if (r.Directory == r.RootDirectory)
                    {
                        r.Container.FetchAttributes();
                        this.RootProvider.WriteItemObject(r.Container.Properties, path, true);
                        this.RootProvider.WriteItemObject(r.Container.Metadata, path, true);
                    }
                    else
                    {
                        //none to show
                    }
                    break;
                default:
                    break;
            }
        }

        public override void SetProperty(string path, PSObject propertyValue)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            switch (r.PathType)
            {
                case PathType.AzureBlobBlock:
                    r.Blob.FetchAttributes();
                    MergeProperties(r.Blob.Metadata, propertyValue.Properties);
                    r.Blob.SetMetadata();
                    break;
                case PathType.AzureBlobDirectory:
                    if (r.Parts.Count() == 1)
                    {
                        r.Container.FetchAttributes();
                        MergeProperties(r.Container.Metadata, propertyValue.Properties);
                        r.Container.SetMetadata();
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
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            MemoryStream target = new MemoryStream();
            r.Blob.DownloadToStream(target);
            return target;
        }

        public override void CopyTo(string path, string name, Stream stream)
        {
            var r = AzureBlobPathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            if (r.PathType == PathType.AzureBlobDirectory)
            {
                var blob = r.Directory.GetBlockBlobReference(name);
                blob.UploadFromStream(stream);
            }
        }

        public override IList<string> GetChildNamesList(string path, PathType type = PathType.Any)
        {
            var childs = new List<string>();
            var items = ListItems(path);

            if (type == PathType.Container || type == PathType.Any)
            {
                HandleItems(items, x => { } , x => childs.Add(x.Prefix.Trim('/').Split('/').Last()), x => childs.Add(x.Name));
            }
            if (type == PathType.Item || type == PathType.Any)
            {
                HandleItems(items, x => childs.Add(x.Name.Trim('/').Split('/').Last()), x => { }, x => { });
            }
            return childs;
        }
    }

    class AzureBlobReader : IContentReader
    {
        private ICloudBlob File { get; set; }
        private long length = 0;
        private const int unit = 1024 * 64; //64KB

        private byte[] buffer = new byte[unit];
        private int bufferSize = 0;
        private int pointer = -1;
        private long fileOffset = 0;

        public AzureBlobReader(ICloudBlob file)
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
            if (this.fileOffset + this.pointer >= length)
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
                    if (this.fileOffset + i == this.length - 1) {

                        var s = Encoding.UTF8.GetString(this.buffer, this.pointer, i + 1 - this.pointer);
                        this.pointer = i + 1;
                        return s;
                    } else {
                        //if it's just the end of this block, just read from the pointer
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
