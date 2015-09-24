using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CDrive.Util
{
    public class Constants
    {
        public const int KB = 1024;
        public const int MB = 1024 * KB;
        public const int GB = 1024 * MB;
        public const long TB = 1024L * GB;
        public const int BlockSize = 4 * 1024 * 1024; //default block size: 4MB
        public static int Parallalism = 8 * Environment.ProcessorCount;
    }
}
