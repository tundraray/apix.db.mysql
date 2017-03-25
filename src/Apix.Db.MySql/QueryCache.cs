using System;
using System.Collections.Generic;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// Simple SQL query strings cache
    /// </summary>
    /// <remarks>Not thread-safety</remarks>
	internal static class QueryCache
    {
        private static readonly Object Obj = new Object();

        private static readonly Dictionary<string, string> Cache = new Dictionary<string, string>();
        /// <summary>
        /// Get SQL query string
        /// </summary>
        /// <param name="type">Repository type</param>
        /// <param name="queryType">Query type</param>
        /// <param name="tableName">Repository table name</param>
        /// <returns>Stored SQL query string</returns>
	    public static string GetQuery(Type type, string queryType, string tableName)
        {
            string query;
            Cache.TryGetValue(GetKey(type, queryType, tableName), out query);
            return query;
        }

        private static string GetKey(Type type, string queryType, string tableName)
        {
            return type.Name + "_" + queryType + "_" + tableName;
        }
        /// <summary>
        /// Add SQL query string
        /// </summary>
        /// <param name="type">Repository type</param>
        /// <param name="queryType">Query type</param>
        /// <param name="tableName">Repository table name</param>
        /// <param name="query">Storing SQL query string</param>
	    public static void AddQuery(Type type, string queryType, string tableName, string query)
        {
            string key = GetKey(type, queryType, tableName);

            if (Cache.ContainsKey(key))
                return;

            lock (Obj)
            {
                if (!Cache.ContainsKey(key))
                    Cache.Add(key, query);
            }
        }
    }
}