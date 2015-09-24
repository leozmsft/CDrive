using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CDrive.Util
{
    public class TableObject
    {
        public string Name { get; set; }
        public StorageUri StorageUri { get; set; }
    }
}
