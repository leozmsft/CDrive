using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace CDrive.Util
{
    public class AzureUtils
    {
        private static Dictionary<char, SharedAccessAccountPermissions> permissionDict = new Dictionary<char, SharedAccessAccountPermissions>()
            {
                { 'r', SharedAccessAccountPermissions.Read },
                { 'w', SharedAccessAccountPermissions.Write },
                { 'd', SharedAccessAccountPermissions.Delete },
                { 'l', SharedAccessAccountPermissions.List},
                { 'a', SharedAccessAccountPermissions.Add },
                { 'c', SharedAccessAccountPermissions.Create },
                { 'p', SharedAccessAccountPermissions.ProcessMessages },
                { 'u', SharedAccessAccountPermissions.Update },
            };

        private static Dictionary<char, SharedAccessAccountResourceTypes> resourceTypeDict = new Dictionary<char, SharedAccessAccountResourceTypes>()
        {
            { 'c', SharedAccessAccountResourceTypes.Container },
            { 'o', SharedAccessAccountResourceTypes.Object },
            { 's', SharedAccessAccountResourceTypes.Service },
        };

        private static Dictionary<char, SharedAccessAccountServices> serviceTypeDict = new Dictionary<char, SharedAccessAccountServices>()
        {
            { 'b', SharedAccessAccountServices.Blob },
            { 'q', SharedAccessAccountServices.Queue },
            { 't', SharedAccessAccountServices.Table },
            { 'f', SharedAccessAccountServices.File },
        };

        public static SharedAccessAccountPolicy ParseAccountSAS(string permissions)
        {
            var expected = "start=<days>;expiry=<days>;p=acdlprwu;protocol=http|https;resources=cos;services=bqtf;IP=<IP1>[-<IP2>]";
            if (permissions == null)
            {
                throw new Exception("Value should be set. Expected: " + expected);
            }

            ///permissions: start=0;expiry=30;policy=hello;p=rwdl
            ///
            var set = permissions.Split(';');
            var policy = new SharedAccessAccountPolicy();

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
                    case "p":
                        for (var i = 0; i < p[1].Length; ++i)
                        {
                            policy.Permissions |= permissionDict[Char.ToLowerInvariant(p[1][i])];
                        }
                        break;
                    case "resources":
                        for (var i = 0; i < p[1].Length; ++i)
                        {
                            policy.ResourceTypes |= resourceTypeDict[Char.ToLowerInvariant(p[1][i])];
                        }
                        break;
                    case "services":
                        for (var i = 0; i < p[1].Length; ++i)
                        {
                            policy.Services |= serviceTypeDict[Char.ToLowerInvariant(p[1][i])];
                        }
                        break;
                    case "protocol":
                        if ("https".Equals(p[1], StringComparison.InvariantCultureIgnoreCase))
                        {
                            policy.Protocols = SharedAccessProtocol.HttpsOnly;
                        }
                        else
                        {
                            policy.Protocols = SharedAccessProtocol.HttpsOrHttp;
                        }
                        break;
                    case "ip":
                        var parts = p[1].Split('-');
                        if (parts.Length == 1)
                        {
                            policy.IPAddressOrRange = new IPAddressOrRange(p[1]);
                        }
                        else if (parts.Length == 2)
                        {
                            policy.IPAddressOrRange = new IPAddressOrRange(parts[0], parts[1]);
                        }
                        else
                        {
                            throw new Exception("Unknown IP parameter: " + p[1] + ". Expected: " + expected);
                        }
                        break;
                    default:
                        throw new Exception("Unknown parameter: " + p[0] + ". Expected: " + expected);
                }
            }

            return policy;
        }
    }
}
