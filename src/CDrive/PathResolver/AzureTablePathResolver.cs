using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CDrive
{
    public class AzureTablePathResolver : PathResolver
    {
        public static AzureTablePathResolveResult ResolvePath(CloudTableClient client, string path)
        {
            var result = new AzureTablePathResolveResult();
            var parts = SplitPath(path);
            if (!ValidatePath(parts))
            {
                throw new Exception("Path " + path + " is invalid");
            }

            result.Parts = parts;

            if (parts.Count == 0)
            {
                result.PathType = PathType.AzureTableRoot;
            }

            if (parts.Count > 0)
            {
                result.Table = client.GetTableReference(parts[0]);
                result.PathType = PathType.AzureTableQuery;
                result.Query = new TableQuery();
            }

            if (result.PathType == PathType.AzureTableQuery)
            {
                var partitionKeySpecified = false;
                var rowKeySpecified = false;

                var filters = new List<string>();
                var selects = new List<string>();
                var q = result.Query;
                for (var i = 1; i < parts.Count; ++i)
                {
                    var p = parts[i];
                    var take = 0;
                    if (p.StartsWith("take=") && Int32.TryParse(p.Substring("take=".Length), out take))
                    {
                        q.TakeCount = take;
                    }
                    else if (p.StartsWith("select=") && !p.EndsWith("select="))
                    {
                        selects.Add(p.Substring("select=".Length));
                    }
                    else if (p.Contains(".ge="))
                    {
                        var index = p.IndexOf(".ge=");
                        filters.Add(string.Format("{0} ge {1}", p.Substring(0, index), ConvertToQuery(p.Substring(index + ".ge=".Length))));
                    }
                    else if (p.Contains(".le="))
                    {
                        var index = p.IndexOf(".le=");
                        filters.Add(string.Format("{0} le {1}", p.Substring(0, index), ConvertToQuery(p.Substring(index + ".le=".Length))));
                    }
                    else if (p.Contains(".gt="))
                    {
                        var index = p.IndexOf(".gt=");
                        filters.Add(string.Format("{0} gt {1}", p.Substring(0, index), ConvertToQuery(p.Substring(index + ".gt=".Length))));
                    }
                    else if (p.Contains(".lt="))
                    {
                        var index = p.IndexOf(".lt=");
                        filters.Add(string.Format("{0} lt {1}", p.Substring(0, index), ConvertToQuery(p.Substring(index + ".lt=".Length))));
                    }
                    else if (p.Trim('=').Contains("="))
                    {
                        var ps = p.Split(new char[] { '=' }, 2);
                        filters.Add(string.Format("{0} eq {1}", ps[0], ConvertToQuery(ps[1])));
                    }
                    else
                    {
                        if (!partitionKeySpecified)
                        {
                            filters.Add("PartitionKey eq '" + p + "'");
                            partitionKeySpecified = true;
                        }
                        else if (!rowKeySpecified)
                        {
                            filters.Add("RowKey eq '" + p + "'");
                            rowKeySpecified = true;
                        }
                        else
                        {
                            result.PathType = PathType.Invalid;
                        }
                    }
                }

                q.FilterString = string.Join(" and ", filters.ToArray());
                q.SelectColumns = selects;
            }

            return result;
        }

        public static bool ValidatePath(List<string> parts)
        {
            return true;
        }

        public static string ConvertToQuery(string s)
        {
            if (s.StartsWith("datetime."))
            {
                return string.Format("datetime'{0}'", s.Substring("datetime.".Length).Replace('.', ':'));
            }

            else if (s.StartsWith("int."))
            {
                return string.Format("{0}", s.Substring("int.".Length));
            }

            else if (s.StartsWith("int64."))
            {
                return string.Format("{0}", s.Substring("int64.".Length));
            }

            else if (s.StartsWith("boolean."))
            {
                return string.Format("{0}", s.Substring("boolean.".Length));
            }

            else if (s.StartsWith("guid."))
            {
                return string.Format("guid'{0}'", s.Substring("guid.".Length));
            }

            else if (s.StartsWith("double."))
            {
                return string.Format("{0}", s.Substring("double.".Length));
            }
            return '\'' + s + '\'';
        }
    }
}
