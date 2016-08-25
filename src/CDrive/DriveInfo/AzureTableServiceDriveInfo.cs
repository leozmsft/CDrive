using CDrive.Util;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Text;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureTableServiceDriveInfo : AbstractDriveInfo
    {
        public CloudTableClient Client { get; set; }
        public string Endpoint { get; set; }

        public AzureTableServiceDriveInfo(string url, string name)
        {
            var parts = url.Split('?');
            var endpoint = parts[0];
            var dict = ParseValues(parts[1]);
            var accountName = dict["account"];
            var accountKey = dict["key"];

            var cred = new StorageCredentials(accountName, accountKey);
            var account = new CloudStorageAccount(cred, null, null, tableStorageUri: new StorageUri(new Uri(endpoint)), fileStorageUri: null);
            var client = account.CreateCloudTableClient();

            this.Client = client;
            this.Endpoint = endpoint;
            this.Name = name;
        }

        /// <summary>
        /// It creates a table or a table entity, or update table entities
        /// </summary>
        public override void NewItem(
                            string path,
                            string type,
                            object newItemValue)
        {
            var r = AzureTablePathResolver.ResolvePath(this.Client, path);
            if (r.Parts.Count == 0)
            {
                this.RootProvider.WriteWarning("Nothing to create.");
                return;
            }

            if (type == null)
            {
                type = string.Empty;
            }

            switch (type.ToLowerInvariant())
            {
                case "entity":
                    {
                        var keys = newItemValue as string;
                        if (keys == null || !keys.Contains("#"))
                        {
                            this.RootProvider.WriteWarning("It requires value formatted as <PartitionKey>#<RowKey>");
                            return;
                        }

                        var parts = keys.Split('#');
                        var pk = parts[0];
                        var rk = parts[1];

                        var e = new TableEntity(pk, rk);
                        var o = TableOperation.Insert(e);
                        r.Table.Execute(o);
                        break;
                    }
                case "insertentity":
                    CreateEntity(r, newItemValue, TableOperation.Insert);
                    break;
                case "replaceentity":
                    CreateEntity(r, newItemValue, TableOperation.Replace);
                    break;
                case "mergeentity":
                    CreateEntity(r, newItemValue, TableOperation.Merge);
                    break;
                case "insertorreplaceentity":
                    CreateEntity(r, newItemValue, TableOperation.InsertOrReplace);
                    break;
                case "insertormergeentity":
                    CreateEntity(r, newItemValue, TableOperation.InsertOrMerge);
                    break;
                case "sastoken":
                    {
                        var parts = PathResolver.SplitPath(path);
                        if (parts.Count > 0)
                        {
                            var tableName = parts[0];
                            var table = this.Client.GetTableReference(tableName);
                            var policyName = string.Empty;
                            var policy = CreateTablePermission(newItemValue as string, ref policyName);

                            if (policyName != string.Empty) //policy-based SAStoken
                            {
                                var token = table.GetSharedAccessSignature(policy, policyName);
                                this.RootProvider.WriteItemObject(token, path, false);
                            }
                            else
                            {
                                var token = table.GetSharedAccessSignature(policy);
                                this.RootProvider.WriteItemObject(token, path, false);
                            }
                        }
                    }

                    break;
                case "policy":
                    {
                        var parts = PathResolver.SplitPath(path);
                        if (parts.Count > 0)
                        {
                            var tableName = parts[0];
                            var table = this.Client.GetTableReference(tableName);
                            var policyName = parts.Last();
                            var policyPlaceHolder = string.Empty;
                            var policy = CreateTablePermission(newItemValue as string, ref policyPlaceHolder);

                            var permissions = table.GetPermissions();
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

                            table.SetPermissions(permissions);

                            this.RootProvider.WriteWarning(string.Format("Policy {0} updated or added.", policyName));
                        }
                    }
                    break;
                case "":
                case "directory":
                    if (r.Parts.Count == 1) //create a table
                    {
                        r.Table.Create();
                        this.RootProvider.WriteWarning("Table is created.");
                    }
                    else
                    {
                        UpdateEntities(path, type, newItemValue);
                    }
                    break;
                default:
                    ShowNewItemHelp();
                    break;
            }
        }

        private void ShowNewItemHelp()
        {
            var help = @"New-Item Helper:
Type            Expected Value         Comment
===========     ===============        ==================
<none>          <optional>             To create a table or update entities
Policy          start=<days>;expiry=<days>;policy=<policyName>;p=adqu  Create/Update a policy, policy name in last part of path
sastoken        start=<days>;expiry=<days>;policy=<policyName>;p=adqu  Create an SAS token for the table based on policy or not
entity          <pk>#<rk>              Create an empty entity with <PK> and <RK>
insertEntity    <HashTable> or         <HashTable> must has PartitionKey and RowKey keys, and its properties can be of different
replaceEntity   <DynamicTableEntity>    supported types or string literals
mergeEntity     ...
insertOrReplaceEntity  ...
insertOrMergeEntity    ...
";
            this.RootProvider.WriteItemObject(help, string.Empty, false);
        }

        private void UpdateEntities(string path, string type, object newItemValue)
        {
            //update existing entities
            this.RootProvider.WriteWarning("update existing entities");
            var propertyName = Path.GetFileName(path);
            var entityPath = PathResolver.GetParentPath(path);
            var r = AzureTablePathResolver.ResolvePath(this.Client, entityPath); //override path
            var isNull = newItemValue == null;

            var entities = this.ListEntities(r.Table, r.Query);

            foreach (DynamicTableEntity e in entities)
            {
                if (isNull)
                {
                    //removing the property and save
                    if (e.Properties.ContainsKey(propertyName))
                    {
                        e.Properties.Remove(propertyName);
                        var op = TableOperation.InsertOrReplace(e);
                        r.Table.Execute(op);
                        this.RootProvider.WriteWarning(string.Format("Property {2} is removed from entity {0} # {1}.", e.PartitionKey, e.RowKey, propertyName));
                    }
                    else
                    {
                        this.RootProvider.WriteWarning(string.Format("Skipping entity {0} # {1} is .", e.PartitionKey, e.RowKey));
                    }

                    continue;
                }

                //Type choice:
                //0. if specified explicitly in type
                //1. if there is existing property
                //2. default to string

                var targetType = EdmType.String;

                if (!string.IsNullOrEmpty(type))
                {
                    if (!Enum.TryParse<EdmType>(type, true, out targetType))
                    {
                        throw new Exception("Failed to parse type " + type + ", valid values are:" + string.Join(",", Enum.GetNames(typeof(EdmType))));
                    }
                }
                else if (e.Properties.ContainsKey(propertyName))
                {
                    targetType = e.Properties[propertyName].PropertyType;
                }

                //Check property existence:
                var existing = e.Properties.ContainsKey(propertyName);

                //set value
                switch (targetType)
                {
                    case EdmType.String:
                        if (existing)
                        {
                            e.Properties[propertyName].StringValue = newItemValue.ToString();
                        }
                        else
                        {
                            var value = new EntityProperty(newItemValue.ToString());
                            e.Properties.Add(propertyName, value);
                        }
                        break;
                    case EdmType.Binary:
                        var bytes = newItemValue as byte[];
                        if (existing)
                        {
                            if (bytes != null)
                            {
                                e.Properties[propertyName].BinaryValue = bytes;
                            }
                            else
                            {
                                e.Properties[propertyName].BinaryValue = Utils.FromBase64(newItemValue.ToString());
                            }
                        }
                        else
                        {
                            if (bytes != null)
                            {
                                var value = new EntityProperty(bytes);
                                e.Properties.Add(propertyName, value);
                            }
                            else
                            {
                                var value = new EntityProperty(newItemValue.ToString());
                                e.Properties.Add(propertyName, value);
                            }
                        }
                        break;
                    case EdmType.Boolean:
                        var boolValue = true as bool?;
                        

                        // Check target value:
                        // 1. the input value is a bool
                        // 2. the input value is a string, can be parsed to be a bool
                        if (newItemValue is bool)
                        {
                            boolValue = (bool)newItemValue;
                        }
                        else if (newItemValue is string)
                        {
                            var boolString = newItemValue as string;
                            var tempValue = true;
                            if (Boolean.TryParse(boolString, out tempValue))
                            {
                                boolValue = tempValue;
                            }
                            else
                            {
                                throw new Exception("Failed to parse boolean value " + boolString);
                            }
                        }

                        //set value
                        if (existing)
                        {
                            e.Properties[propertyName].BooleanValue = boolValue;
                        }
                        else
                        {
                            var value = new EntityProperty(boolValue);
                            e.Properties.Add(propertyName, value);
                        }

                        break;
                    case EdmType.DateTime:
                        var dateTime = null as DateTime?;

                        // Check target value:
                        // 1. the input value is a DateTime
                        // 2. the input value is a string, can be parsed to be a DateTime
                        if (newItemValue is DateTime)
                        {
                            dateTime = (DateTime)newItemValue;
                        }
                        else if (newItemValue is string)
                        {
                            var dateTimeString = newItemValue as string;
                            var tempDateTime = DateTime.Now;
                            if (DateTime.TryParse(dateTimeString, out tempDateTime))
                            {
                                dateTime = tempDateTime;
                            }
                            else
                            {
                                throw new Exception("Failed to parse DateTime value " + dateTimeString);
                            }
                        }

                        //set value
                        if (existing)
                        {
                            e.Properties[propertyName].DateTime = dateTime;
                        }
                        else
                        {
                            var value = new EntityProperty(dateTime);
                            e.Properties.Add(propertyName, value);
                        }

                        break;
                    case EdmType.Double:
                        var doubleValue = null as double?;
                        
                        if (newItemValue is double)
                        {
                            doubleValue = (double)newItemValue;
                        }
                        else if (newItemValue is string)
                        {
                            var doubleString = newItemValue as string;
                            var tempValue = 0.0;
                            if (Double.TryParse(doubleString, out tempValue))
                            {
                                doubleValue = tempValue;
                            }
                            else
                            {
                                throw new Exception("Failed to parse double value " + doubleString);
                            }
                        }

                        //set value
                        if (existing)
                        {
                            e.Properties[propertyName].DoubleValue = doubleValue;
                        }
                        else
                        {
                            var value = new EntityProperty(doubleValue);
                            e.Properties.Add(propertyName, value);
                        }

                        break;
                    case EdmType.Guid:
                        var guidValue = null as Guid?;

                        if (newItemValue is Guid)
                        {
                            guidValue = (Guid)newItemValue;
                        }
                        else
                        {
                            var guidString = newItemValue as string;
                            var tempValue = Guid.NewGuid();
                            if (Guid.TryParse(guidString, out tempValue))
                            {
                                guidValue = tempValue;
                            }
                            else
                            {
                                throw new Exception("Failed to parse Guid value " + guidString);
                            }
                        }

                        //set value
                        if (existing)
                        {
                            e.Properties[propertyName].GuidValue = guidValue;
                        }
                        else
                        {
                            var value = new EntityProperty(guidValue);
                            e.Properties.Add(propertyName, value);
                        }
                        break;
                    case EdmType.Int32:
                        var int32Value = null as int?;

                        if (newItemValue is int)
                        {
                            int32Value = (int)newItemValue;
                        }
                        else
                        {
                            var int32String = newItemValue as string;
                            var tempValue = 0;
                            if (int.TryParse(int32String, out tempValue))
                            {
                                int32Value = tempValue;
                            }
                            else
                            {
                                throw new Exception("Failed to parse int32 value " + int32String);
                            }
                        }

                        //set value
                        if (existing)
                        {
                            e.Properties[propertyName].Int32Value = int32Value;
                        }
                        else
                        {
                            var value = new EntityProperty(int32Value);
                            e.Properties.Add(propertyName, value);
                        }
                        break;
                    case EdmType.Int64:
                        var int64Value = null as Int64?;

                        if (newItemValue is Int64)
                        {
                            int64Value = (Int64)newItemValue;
                        }
                        else
                        {
                            var int64String = newItemValue as string;
                            var tempValue = (Int64)0;
                            if (Int64.TryParse(int64String, out tempValue))
                            {
                                int64Value = tempValue;
                            }
                            else
                            {
                                throw new Exception("Failed to parse int64 value " + int64String);
                            }
                        }

                        //set value
                        if (existing)
                        {
                            e.Properties[propertyName].Int64Value = int64Value;
                        }
                        else
                        {
                            var value = new EntityProperty(int64Value);
                            e.Properties.Add(propertyName, value);
                        }
                        break;
                } //end of switch

                var o = TableOperation.Merge(e);
                r.Table.Execute(o);
                this.RootProvider.WriteWarning(string.Format("Entity {0} # {1} is merged.", e.PartitionKey, e.RowKey));
            }
        }

        private void CreateEntity(AzureTablePathResolveResult r, object obj, Func<ITableEntity, TableOperation> func)
        {
            DynamicTableEntity e = null;
            if (obj is Hashtable)
            {
                e = ConvertToTableEntity(obj as Hashtable);
            }
            else if (obj is PSObject)
            {
                var bo = ((PSObject)obj).BaseObject;
                if (bo is Hashtable) {
                    e = ConvertToTableEntity(bo as Hashtable);
                }
                else if (bo is DynamicTableEntity) {
                    e = bo as DynamicTableEntity;
                }
            }
            else if (obj is DynamicTableEntity)
            {
                e = obj as DynamicTableEntity;
            }


            if (e == null)
            {
                throw new Exception("Unknown object caught.");
            }

            var o = func(e);
            r.Table.Execute(o);
            this.RootProvider.WriteWarning(string.Format("Entity {0} # {1} is added.", e.PartitionKey, e.RowKey));
        }

        private DynamicTableEntity ConvertToTableEntity(Hashtable hashtable)
        {
            var e = new DynamicTableEntity();
            foreach (string key in hashtable.Keys)
            {
                switch (key.ToLowerInvariant())
                {
                    case "partitionkey":
                        e.PartitionKey = hashtable[key] as string;
                        break;
                    case "rowkey":
                        e.RowKey = hashtable[key] as string;
                        break;
                    default:
                        AddEntityProperty(e, key, hashtable[key]);
                        break;

                }
            }

            return e;
        }

        private void AddEntityProperty(DynamicTableEntity e, string key, object o)
        {
            if (o == null)
            {
                this.RootProvider.WriteWarning(string.Format("Key {0} is skipped as it's null.", key));
                return;
            }

            var s = o as string;
            if (s == null)
            {
                if (o is PSObject)
                {
                    o = (o as PSObject).BaseObject;
                }

                if (o is DateTime)
                {
                    e.Properties.Add(key, new EntityProperty((DateTime)o));
                }
                else if (o is int)
                {
                    e.Properties.Add(key, new EntityProperty((int)o));
                }
                else if (o is long)
                {
                    e.Properties.Add(key, new EntityProperty((long)o));
                }
                else if (o is bool)
                {
                    e.Properties.Add(key, new EntityProperty((bool)o));
                }
                else if (o is Guid)
                {
                    e.Properties.Add(key, new EntityProperty((Guid)o));
                }
                else if (o is double)
                {
                    e.Properties.Add(key, new EntityProperty((double)o));
                }
                else if (o is byte[])
                {
                    e.Properties.Add(key, new EntityProperty((byte[])o));
                }
            }
            else
            {
                if (s.StartsWith("datetime."))
                {
                    var dt = DateTime.Parse(s.Substring("datetime.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }

                else if (s.StartsWith("int."))
                {
                    var dt = Convert.ToInt32(s.Substring("int.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }

                else if (s.StartsWith("int64."))
                {
                    var dt = Convert.ToInt64(s.Substring("int64.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }

                else if (s.StartsWith("boolean."))
                {
                    var dt = Convert.ToBoolean(s.Substring("boolean.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }

                else if (s.StartsWith("guid."))
                {
                    var dt = Convert.ToBoolean(s.Substring("guid.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }

                else if (s.StartsWith("double."))
                {
                    var dt = Convert.ToBoolean(s.Substring("double.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }
                else if (s.StartsWith("binary."))
                {
                    var dt = Convert.FromBase64String(s.Substring("binary.".Length));
                    e.Properties.Add(key, new EntityProperty(dt));
                }
                else
                {
                    e.Properties.Add(key, new EntityProperty(s));
                }
            }
        }

        private SharedAccessTablePolicy CreateTablePermission(string permissions, ref string policyName)
        {
            ///permissions: start=0;expiry=30;policy=hello;p=adqu
            ///
            var set = permissions.Split(';');
            var policy = new SharedAccessTablePolicy();
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
                                case 'a':
                                    policy.Permissions |= SharedAccessTablePermissions.Add;
                                    break;
                                case 'd':
                                    policy.Permissions |= SharedAccessTablePermissions.Delete;
                                    break;
                                case 'q':
                                    policy.Permissions |= SharedAccessTablePermissions.Query;
                                    break;
                                case 'u':
                                    policy.Permissions |= SharedAccessTablePermissions.Update;
                                    break;
                            }
                        }
                        break;
                    default:
                        throw new Exception("Unknown parameter: " + p[0] + ". Expected: start=<days>;expiry=<days>;policy=<policyName>;p=adqu");
                }
            }

            return policy;
        }

        public override void GetChildItems(string path, bool recurse)
        {
            var items = this.ListItems(path);
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                foreach (dynamic item in items)
                {
                    var to = new TableObject()
                    {
                        Name = item.Name,
                        StorageUri = item.StorageUri
                    };

                    this.RootProvider.WriteItemObject(to, path, true);
                   
                }
            }
            else
            {
                foreach (var item in items)
                {
                    this.RootProvider.WriteItemObject(item, path, true);
                }
            }
        }

        public override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            var r = AzureTablePathResolver.ResolvePath(this.Client, path);
            switch (r.PathType)
            {
                case PathType.AzureTableRoot:
                    var tables = ListTables();
                    foreach (var table in tables)
                    {
                        this.RootProvider.WriteItemObject(table.Name, path, true);
                    }
                    break;
                default:
                    break;
            }
        }

        public override void RemoveItem(string path, bool recurse)
        {
            var r = AzureTablePathResolver.ResolvePath(this.Client, path);
            if (r.Parts.Count == 1) //removing table
            {
                var table = this.Client.GetTableReference(r.Parts.Last());
                table.Delete();
                return;
            }
            else if (r.PathType != PathType.Invalid && r.Parts.Count > 1)
            {
                var entities = ListEntities(r.Table, r.Query);

                foreach (var e in entities)
                {
                    var o = TableOperation.Delete(e);
                    r.Table.Execute(o);
                    this.RootProvider.WriteWarning(string.Format("Entity {0} # {1} is removed.", e.PartitionKey, e.RowKey));
                }
                return;
            }

            if (r.PathType == PathType.Invalid)
            {
                var propertyName = Path.GetFileName(path);
                var entityPath = PathResolver.GetParentPath(path);
                r = AzureTablePathResolver.ResolvePath(this.Client, entityPath); //override path

                var entities = ListEntities(r.Table, r.Query);

                foreach (var e in entities)
                {
                    if (e.Properties.ContainsKey(propertyName))
                    {
                        e.Properties.Remove(propertyName);
                        var o = TableOperation.Replace(e);
                        r.Table.Execute(o);
                        this.RootProvider.WriteWarning(string.Format("Entity {0} # {1} is updated.", e.PartitionKey, e.RowKey));
                    }
                }
                return;
            }

            this.RootProvider.WriteWarning("Nothing to do");
        }

        internal IEnumerable<object> ListItems(string path)
        {
            var result = AzureTablePathResolver.ResolvePath(this.Client, path);

            switch (result.PathType)
            {
                case PathType.AzureTableRoot:
                    return ListTables();
                case PathType.AzureTableQuery:
                    return ListEntities(result.Table, result.Query);
                default:
                    return null;
            }
        }

        private IEnumerable<DynamicTableEntity> ListEntities(CloudTable cloudTable, TableQuery query)
        {
            var list = cloudTable.ExecuteQuery(query);
            return list;
        }

        private IEnumerable<CloudTable> ListTables()
        {
            return this.Client.ListTables();
        }


        public override IContentReader GetContentReader(string path)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                //TODO: if it is listing tables, no display
                this.RootProvider.WriteWarning("Cannot use cat to display tables. Please enter a specific table.");
                return null;
            }

            var items = this.ListItems(path);
            return new AzureTableReader(items);
        }



        public override bool HasChildItems(string path)
        {
            var r = AzureTablePathResolver.ResolvePath(this.Client, path);
            return r.Exists();
        }

        public override bool IsValidPath(string path)
        {
            return true;
        }

        public override bool ItemExists(string path)
        {
            return true;
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
                var r = AzureTablePathResolver.ResolvePath(this.Client, path);
                return r.Exists();
            }
            catch (Exception)
            {
                return false;
            }
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

        internal void CopyTo(string path, AzureTableServiceDriveInfo target, string copyPath, bool deleteOriginal = false)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                this.RootProvider.WriteWarning("Not supported to copy multiple tables.");
                return;
            }

            var targetRef = AzureTablePathResolver.ResolvePath(target.Client, copyPath);
            if (targetRef.PathType == PathType.AzureTableRoot)
            {
                this.RootProvider.WriteWarning("Must specify the target table.");
                return;
            }

            var items = this.ListItems(path);
            foreach (ITableEntity i in items)
            {
                var o = TableOperation.InsertOrReplace(i);
                targetRef.Table.Execute(o);
                this.RootProvider.WriteWarning(string.Format("Entity {0} # {1} is inserted", i.PartitionKey, i.RowKey));
            }

            if (deleteOriginal)
            {
                var sourceRef = AzureTablePathResolver.ResolvePath(this.Client, path);
                foreach (ITableEntity i in items)
                {
                    var o = TableOperation.Delete(i);
                    sourceRef.Table.Execute(o);
                    this.RootProvider.WriteWarning(string.Format("Source Entity {0} # {1} is deleted", i.PartitionKey, i.RowKey));
                }
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


    class AzureTableReader : IContentReader
    {
        private List<string> buffer = new List<string>();
        private int pointer = 0;

        public AzureTableReader(IEnumerable<object> items)
        {
            InitBuffer(items);
        }

        private void InitBuffer(IEnumerable<object> items)
        {
            foreach (DynamicTableEntity item in items)
            {
                buffer.Add("===============================================================================");
                buffer.Add("PartitionKey: " + item.PartitionKey);
                buffer.Add("      RowKey: " + item.RowKey);
                buffer.Add("   Timestamp: " + item.Timestamp);
                buffer.Add(" ");

                if (item.Properties.Count == 0)
                {
                    continue;
                }

                var maxLength = item.Properties.Max(p => p.Key.Length);

                foreach (var p in item.Properties.OrderBy(a => a.Key))
                {
                    var displayKey = p.Key.PadRight(maxLength);
                    switch (p.Value.PropertyType)
                    {
                        case EdmType.String:
                            buffer.Add(displayKey + " : " + p.Value.StringValue);
                            break;
                        case EdmType.Binary:
                            buffer.Add(displayKey + " :[binary] " + p.Value.BinaryValue.Length);
                            break;
                        case EdmType.Boolean:
                            buffer.Add(displayKey + " :[boolean] " + p.Value.BooleanValue);
                            break;
                        case EdmType.DateTime:
                            buffer.Add(displayKey + " :[datetime] " + (p.Value.DateTime.HasValue ? p.Value.DateTime.Value.ToString("yyyy-MM-dd HH:mm:ss.fff") : "null"));
                            break;
                        case EdmType.Double:
                            buffer.Add(displayKey + " :[double] " + p.Value.DoubleValue);
                            break;
                        case EdmType.Guid:
                            buffer.Add(displayKey + " :[guid] " + p.Value.GuidValue);
                            break;
                        case EdmType.Int32:
                            buffer.Add(displayKey + " :[int] " + p.Value.Int32Value);
                            break;
                        case EdmType.Int64:
                            buffer.Add(displayKey + " :[int64] " + p.Value.Int64Value);
                            break;
                    }
                }
            }
        }
        public void Close()
        {
        }

        public System.Collections.IList Read(long readCount)
        {
            if (pointer >= buffer.Count)
            {
                return null;
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
            if (this.pointer < this.buffer.Count)
            {
                return this.buffer[this.pointer++];
            }

            return string.Empty;
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
