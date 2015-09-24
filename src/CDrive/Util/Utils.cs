using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CDrive.Util
{
    public static class Utils
    {

        public static IEnumerable<string> GetBlockIdArray(int length)
        {
            for (var i = 0; i < length; ++i)
            {
                yield return GetBlockId(i);
            }
        }

        public static string GetBlockId(int id)
        {
            return Convert.ToBase64String(Encoding.ASCII.GetBytes(string.Format("{0:d10}", id)));
        }

        public static byte[] FromBase64(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return new byte[0];
            }

            return Convert.FromBase64String(s);
        }

        public static string ToBase64(byte[] bytes)
        {
            if (bytes == null)
            {
                return string.Empty;
            }

            return Convert.ToBase64String(bytes);
        }
    }
}
