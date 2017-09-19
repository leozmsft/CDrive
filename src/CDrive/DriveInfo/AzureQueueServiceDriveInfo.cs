using CDrive.Util;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
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
    public class AzureQueueServiceDriveInfo : AbstractDriveInfo
    {
        public CloudQueueClient Client { get; set; }
        public string Endpoint { get; set; }

        public AzureQueueServiceDriveInfo(string url, string name)
        {
            var parts = url.Split('?');
            var endpoint = parts[0];
            var dict = ParseValues(parts[1]);
            var accountName = dict["account"];
            var accountKey = dict["key"];

            var cred = new StorageCredentials(accountName, accountKey);
            var account = new CloudStorageAccount(cred, null, queueStorageUri: new StorageUri(new Uri(endpoint)), tableStorageUri: null, fileStorageUri: null);
            var client = account.CreateCloudQueueClient();

            this.Client = client;
            this.Endpoint = endpoint;
            this.Name = name;
        }

        public override void NewItem(
                            string path,
                            string type,
                            object newItemValue)
        {
            var r = AzureQueuePathResolver.ResolvePath(this.Client, path);
            if (r.Parts.Count == 1) {
                var q = this.Client.GetQueueReference(r.Parts[0]);
                q.CreateIfNotExists();
            } else if (r.Parts.Count == 2) {
                var content = r.Parts[1];
                if (newItemValue != null) {
                    content = newItemValue as string;
                }

                r.Queue.AddMessage(new CloudQueueMessage(content));
            }
        }

        public override void GetChildItems(string path, bool recurse)
        {
            //recurse is not supported for Queue service.

            var items = this.ListItems(path);
            foreach (var item in items)
            {
                this.RootProvider.WriteItemObject(item, path, true);
            }
        }

        public override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            var r = AzureQueuePathResolver.ResolvePath(this.Client, path);
            switch (r.PathType)
            {
                case PathType.AzureQueueRoot:
                    var Queues = ListQueues();
                    foreach (var Queue in Queues)
                    {
                        this.RootProvider.WriteItemObject(Queue.Name, path, true);
                    }
                    break;
                default:
                    break;
            }
        }

        public override void RemoveItem(string path, bool recurse)
        {
            var r = AzureQueuePathResolver.ResolvePath(this.Client, path);
            if (r.Parts.Count == 1)
            {
                var q = this.Client.GetQueueReference(r.Parts[0]);
                q.DeleteIfExists();
            }
            else if (r.Parts.Count == 2)
            {
                var id = r.Parts[1];
                var stack = new Stack<CloudQueueMessage>();
                while (true)
                {
                    var m = r.Queue.GetMessage();
                    if (m == null)
                    {
                        break;
                    }

                    stack.Push(m);
                    r.Queue.DeleteMessage(m);

                    if (m.Id == id)
                    {
                        stack.Pop();
                        break;
                    }
                }
                
                while (stack.Count > 0) 
                {
                    var m = stack.Pop();
                    if (m == null) {
                        break;;
                    }

                    r.Queue.AddMessage(m);
                }
            }
        }

        internal IEnumerable<object> ListItems(string path)
        {
            var result = AzureQueuePathResolver.ResolvePath(this.Client, path);

            switch (result.PathType)
            {
                case PathType.AzureQueueRoot:
                    return ListQueues();
                case PathType.AzureQueueQuery:
                    return ListMessages(result.Queue);
                default:
                    return null;
            }
        }

        private IEnumerable<object> ListMessages(CloudQueue cloudQueue)
        {
            var count = (cloudQueue.ApproximateMessageCount ?? 0) + 10;
            return cloudQueue.PeekMessages(count);
        }

        private IEnumerable<CloudQueue> ListQueues()
        {
            return this.Client.ListQueues();
        }

        public override IContentReader GetContentReader(string path)
        {
            return null;
        }

        public override bool HasChildItems(string path)
        {
            var r = AzureQueuePathResolver.ResolvePath(this.Client, path);
            return r.Exists();
        }

        public override bool IsValidPath(string path)
        {
            return true;
        }

        public override bool ItemExists(string path)
        {
            try
            {
                var r = AzureQueuePathResolver.ResolvePath(this.Client, path);
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
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return true;
            }

            try
            {
                var r = AzureQueuePathResolver.ResolvePath(this.Client, path);
                return r.Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override void GetProperty(string path, System.Collections.ObjectModel.Collection<string> providerSpecificPickList)
        {
            //var r = AzureQueuePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            //switch (r.PathType)
            //{
            //    case PathType.AzureFile:
            //        r.File.FetchAttributes();
            //        this.RootProvider.WriteItemObject(r.File.Properties, path, false);
            //        this.RootProvider.WriteItemObject(r.File.Metadata, path, false);
            //        break;
            //    case PathType.AzureFileDirectory:
            //        if (r.Parts.Count() == 1)
            //        {
            //            r.Share.FetchAttributes();
            //            this.RootProvider.WriteItemObject(r.Share.Properties, path, true);
            //            this.RootProvider.WriteItemObject(r.Share.Metadata, path, true);
            //        }
            //        else
            //        {
            //            r.Directory.FetchAttributes();
            //            this.RootProvider.WriteItemObject(r.Directory.Properties, path, true);
            //        }
            //        break;
            //    default:
            //        break;
            //}
        }

        public override void SetProperty(string path, PSObject propertyValue)
        {
            //var r = AzureQueuePathResolver.ResolvePath(this.Client, path, skipCheckExistence: false);
            //switch (r.PathType)
            //{
            //    case PathType.AzureFile:
            //        r.File.FetchAttributes();
            //        MergeProperties(r.File.Metadata, propertyValue.Properties);
            //        r.File.SetMetadata();
            //        break;
            //    case PathType.AzureFileDirectory:
            //        if (r.Parts.Count() == 1)
            //        {
            //            r.Share.FetchAttributes();
            //            MergeProperties(r.Share.Metadata, propertyValue.Properties);
            //            r.Share.SetMetadata();
            //        }
            //        else
            //        {
            //            throw new Exception("Setting metadata/properties for directory is not supported");
            //        }
            //        break;
            //    default:
            //        break;
            //}
        }

        public override Tuple<PathType, Stream> CopyFrom(string path)
        {
            throw new NotImplementedException();
        }

        public override Stream CopyTo(string path, string name, PathType sourcePathType)
        {
            throw new NotImplementedException();
        }

        public override IList<string> GetChildNamesList(string path, PathType type = PathType.Any)
        {
            throw new NotImplementedException();
        }
    }
}
