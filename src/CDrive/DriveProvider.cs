using System;
using System.Collections.Generic;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace CDrive
{
    [CmdletProvider("CDrive", ProviderCapabilities.None)]
    public class DriveProvider : NavigationCmdletProvider, IContentCmdletProvider, IPropertyCmdletProvider
    {
        #region DriveCmdlet
        protected override PSDriveInfo NewDrive(PSDriveInfo drive)
        {
            // Check if the drive object is null.
            if (drive == null)
            {
                WriteError(new ErrorRecord(
                           new ArgumentNullException("drive"),
                           "NullDrive",
                           ErrorCategory.InvalidArgument,
                           null));

                return null;
            }

            var d = new PSDriveInfo(name: drive.Name, provider: drive.Provider, root: PathResolver.Root, description: null, credential: null);
            return d;
        }

        protected override PSDriveInfo RemoveDrive(PSDriveInfo drive)
        {
            if (drive == null)
            {
                WriteError(new ErrorRecord(
                   new ArgumentNullException("drive"),
                   "NullDrive",
                   ErrorCategory.InvalidArgument,
                   drive));

                return null;
            }

            return drive;
        }

        #endregion

        #region ContainerCmdletProvider
        protected override void GetChildItems(string path, bool recurse)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0) //if listing root, then list all PathResolver.Drives
            {
                foreach (var pair in PathResolver.Drives)
                {
                    WriteItemObject(pair.Value, path, true);
                    if (recurse)
                    {
                        pair.Value.GetChildItems(PathResolver.GetSubpath(path), recurse);
                    }
                }

                return;
            }
            else
            {
                var drive = GetDrive(parts[0]);
                if (drive == null)
                {
                    return;
                }

                drive.GetChildItems(PathResolver.GetSubpath(path), recurse);
            }
        }

        protected override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                foreach (var pair in PathResolver.Drives)
                {
                    WriteItemObject(pair.Key, "/", true);
                }
            }
            else
            {
                var drive = GetDrive(parts[0]);
                if (drive == null)
                {
                    return;
                }

                drive.GetChildNames(PathResolver.GetSubpath(path), returnContainers);
            }
        }

        protected override bool HasChildItems(string path)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return PathResolver.Drives.Count > 0;
            }

            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return false;
            }

            return drive.HasChildItems(PathResolver.GetSubpath(path));
        }

        protected override void NewItem(
                                    string path,
                                    string type,
                                    object newItemValue)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return;
            }

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                drive = DriveFactory.CreateInstance(type, newItemValue, parts[0]);
                if (drive == null)
                {
                    WriteError(new ErrorRecord(
                        new Exception("Unregcognized type"),
                        "NewItem type",
                        ErrorCategory.InvalidArgument,
                        ""));
                    return;
                }
                else
                {
                    PathResolver.Drives.Add(parts[0], drive);
                }
            }
            else
            {
                if (parts.Count > 1)
                {
                    //delegate it if needed
                    drive.NewItem(PathResolver.GetSubpath(path), type, newItemValue);
                }
            }
        }

        protected override void CopyItem(string path, string copyPath, bool recurse)
        {
            CopyItemInternal(path, copyPath, recurse);
        }

        protected override void RemoveItem(string path, bool recurse)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return;
            }

            //if the provider is not mounted, then return
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return;
            }

            if (parts.Count == 1)
            {
                //unmount it
                var mountedLabel = parts[0];
                UnmountDrive(parts[0]);
            }
            else
            {
                //delegate it
                drive.RemoveItem(PathResolver.GetSubpath(path), recurse);
            }
        }
        #endregion

        #region ItemCmdlet

        protected override void GetItem(string path)
        {
            GetChildItems(path, false);
        }

        protected override void SetItem(string path, object value)
        {
            throw new NotImplementedException();
        }

        protected override bool IsValidPath(string path)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return true;
            }

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return false;
            }

            //delegate it
            return drive.IsValidPath(PathResolver.GetSubpath(path));
        }

        protected override bool ItemExists(string path)
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

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return false;
            }

            //delegate it
            return drive.ItemExists(PathResolver.GetSubpath(path));
        }
        #endregion

        #region private helper

        private AbstractDriveInfo GetDrive(string mountedLabel)
        {
            var key = PathResolver.Drives.Keys.Where(p => string.Equals(mountedLabel, p, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            if (key != default(string))
            {
                var drive = PathResolver.Drives[key];
                drive.RootProvider = this;
                return drive;
            }

            return null;
        }

        private void UnmountDrive(string mountedLabel)
        {
            var key = PathResolver.Drives.Keys.Where(p => string.Equals(mountedLabel, p, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            if (key != default(string))
            {
                PathResolver.Drives.Remove(key);
            }
        }
        #endregion

        #region navigation
        protected override string GetChildName(string path)
        {
            if (path == this.PSDriveInfo.Root)
            {
                return string.Empty;
            }

            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return this.PSDriveInfo.Root;
            }

            return parts.Last();
        }
        protected override string GetParentPath(string path, string root)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return string.Empty;
            }

            if (parts.Count == 1)
            {
                if (path.StartsWith(PathResolver.Root))
                {
                    return PathResolver.Root;
                }
                else
                {
                    return string.Empty;
                }
            }

            parts.RemoveAt(parts.Count - 1);
            var r = string.Join(PathResolver.DirSeparator, parts.ToArray());
            return r;
        }
        protected override bool IsItemContainer(string path)
        {
            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return true;
            }

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return false;
            }

            if (parts.Count == 1)
            {
                return true;
            }

            //delegate it
            return drive.IsItemContainer(PathResolver.GetSubpath(path));
        }
        protected override string MakePath(string parent, string child)
        {
            var result = PathResolver.Combine(parent, child);
            var parts = PathResolver.SplitPath(result);
            result = PathResolver.Combine(parts);

            if (parent.StartsWith(PathResolver.Root))
            {
                result = PathResolver.Root + result;
            }

            return result;
        }
        protected override void MoveItem(string path, string destination)
        {
            CopyItemInternal(path, destination, true, true);
        }

        protected void CopyItemInternal(string path, string destination, bool recurse, bool deleteOriginal = false, bool exactDest = false)
        {
            var sourceDrive = PathResolver.FindDrive(path);
            var targetDrive = PathResolver.FindDrive(destination);

            if (sourceDrive is AzureTableServiceDriveInfo && targetDrive is AzureTableServiceDriveInfo)
            {
                var sd = sourceDrive as AzureTableServiceDriveInfo;
                var td = targetDrive as AzureTableServiceDriveInfo;
                sd.CopyTo(PathResolver.GetSubpath(path), td, PathResolver.GetSubpath(destination), deleteOriginal);
                return;
            }

            if (sourceDrive.IsItemContainer(PathResolver.GetSubpath(path)))
            {
                if (!recurse)
                {
                    throw new Exception(path + " is a container");
                }

                if (targetDrive.IsItemContainer(PathResolver.GetSubpath(destination)) && !exactDest)
                {
                    destination = MakePath(destination, PathResolver.SplitPath(path).Last());
                }
  
                targetDrive.NewItem(PathResolver.GetSubpath(destination), "Directory", null);
                foreach (var c in sourceDrive.GetChildNamesList(PathResolver.GetSubpath(path), PathType.Item))
                {
                    CopySingleItem(MakePath(path, c), MakePath(destination, c), exactDest: true);
                }
                foreach ( var c in sourceDrive.GetChildNamesList(PathResolver.GetSubpath(path), PathType.Container))
                {
                    CopyItemInternal(MakePath(path, c), MakePath(destination, c), recurse, deleteOriginal, exactDest: true);
                }
            }
            else
            {
                CopySingleItem(path, destination);
            }
        }

        protected void CopySingleItem(string path, string destination, bool exactDest = false)
        {
            var sourceDrive = PathResolver.FindDrive(path);
            var targetDrive = PathResolver.FindDrive(destination);
            var sourcePath = PathResolver.GetSubpath(path);
            string targetDir, targetFile;
            if (targetDrive.IsItemContainer(PathResolver.GetSubpath(destination)) && !exactDest)
            {
                targetDir = PathResolver.GetSubpath(destination);
                targetFile = PathResolver.SplitPath(path).Last();
            }
            else
            {
                targetDir = PathResolver.GetSubpathDirectory(destination);
                targetFile = PathResolver.SplitPath(destination).Last();
            }
            using (var sourceStream = sourceDrive.CopyFrom(sourcePath))
            using (var targetStream = targetDrive.CopyTo(targetDir, targetFile))
            {
                if (sourceStream == null || targetStream == null)
                {
                    return;
                }

                sourceStream.Seek(0, SeekOrigin.Begin);
                sourceStream.CopyTo(targetStream);
            }
        }

        protected override object MoveItemDynamicParameters(string path, string destination)
        {
            return null;
        }
        protected override string NormalizeRelativePath(string path, string basePath)
        {
            if (!string.IsNullOrEmpty(basePath) && path.StartsWith(basePath, StringComparison.InvariantCultureIgnoreCase))
            {
                var pathTrimmed = path.Substring(basePath.Length);
                return PathResolver.NormalizePath(pathTrimmed);
            }

            return PathResolver.NormalizePath(path);
        }
        #endregion

        #region content cmdlets
        public void ClearContent(string path)
        {
            throw new NotImplementedException();
        }

        public object ClearContentDynamicParameters(string path)
        {
            throw new NotImplementedException();
        }

        public IContentReader GetContentReader(string path)
        {
            if (PathResolver.IsLocalPath(path))
            {
                return null;
            }

            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return null;
            }

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return null;
            }

            //delegate it
            return drive.GetContentReader(PathResolver.GetSubpath(path));
        }

        public object GetContentReaderDynamicParameters(string path)
        {
            return null;
        }

        public IContentWriter GetContentWriter(string path)
        {
            throw new NotImplementedException();
        }

        public object GetContentWriterDynamicParameters(string path)
        {
            throw new NotImplementedException();
        }

        #endregion

        public void ClearProperty(string path, System.Collections.ObjectModel.Collection<string> propertyToClear)
        {
            throw new NotImplementedException();
        }

        public object ClearPropertyDynamicParameters(string path, System.Collections.ObjectModel.Collection<string> propertyToClear)
        {
            throw new NotImplementedException();
        }

        #region IPropertyCmdletProvider
        public void GetProperty(string path, System.Collections.ObjectModel.Collection<string> providerSpecificPickList)
        {
            if (PathResolver.IsLocalPath(path))
            {
                return;
            }

            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return;
            }

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return;
            }

            //delegate it
            drive.GetProperty(PathResolver.GetSubpath(path), providerSpecificPickList);
        }

        public object GetPropertyDynamicParameters(string path, System.Collections.ObjectModel.Collection<string> providerSpecificPickList)
        {
            return null;
        }

        public void SetProperty(string path, PSObject propertyValue)
        {
            if (PathResolver.IsLocalPath(path))
            {
                return;
            }

            var parts = PathResolver.SplitPath(path);
            if (parts.Count == 0)
            {
                return;
            }

            //if the provider is not mounted, then create it first
            var drive = GetDrive(parts[0]);
            if (drive == null)
            {
                return;
            }

            //delegate it
            drive.SetProperty(PathResolver.GetSubpath(path), propertyValue);
        }

        public object SetPropertyDynamicParameters(string path, PSObject propertyValue)
        {
            return null;
        }
        #endregion

    }
}
