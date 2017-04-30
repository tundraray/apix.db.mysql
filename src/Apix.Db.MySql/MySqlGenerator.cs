using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Apix.Extensions;
using Dapper;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// MySql generator
    /// </summary>
    public static class MySqlGenerator
    {
        #region Cache

        private static readonly PropertyCache GlobalPropertiesCache = new PropertyCache();
        /// <summary>
        /// Get entity properties
        /// </summary>
        /// <param name="entityType"></param>
        /// <returns></returns>
        public static PropertyInfo[] GetOrAdd(TypeInfo entityType)
        {
            var properties = GlobalPropertiesCache.GetOrAdd(entityType.AsType().TypeHandle, key =>
                   (from p in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    where !p.IsNotDatabaseField() && p.GetSetMethod(true) != null && p.GetGetMethod(true) != null
                    select p).ToDictionary(p => p.Name));

            return properties.Values.ToArray();
        }
        private static readonly ConcurrentDictionary<string, string> Cache = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// Get MySql query string
        /// </summary>
        /// <param name="type"></param>
        /// <param name="queryType"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static string GetQuery(TypeInfo type, string queryType, string tableName)
        {
            string query;
            Cache.TryGetValue(GetKey(type, queryType, tableName), out query);
            return query;
        }

        private static string GetKey(TypeInfo type, string queryType, string tableName)
        {
            return string.Join("_", type.AssemblyQualifiedName, queryType, tableName);
        }
        /// <summary>
        /// Add query string
        /// </summary>
        /// <param name="type"></param>
        /// <param name="queryType"></param>
        /// <param name="tableName"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static string AddQuery(TypeInfo type, string queryType, string tableName, string query)
        {
            var key = GetKey(type, queryType, tableName);
            return Cache.GetOrAdd(key, k => query);
        }

        #endregion

        #region Insert

        /// <summary>
        /// MySql INSERT
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static string InsertQuery<T>()
        {
            var type = typeof(T).GetTypeInfo();
            var tableName = type.GetTableName();
            return GetQuery(type, SqlQueryType.Insert, tableName) ??
                   AddQuery(type, SqlQueryType.Insert, tableName, GenerateInsertQuery(type, tableName));
        }

        private static string GenerateInsertQuery(TypeInfo type, string tableName)
        {
            var properties = GetOrAdd(type).Where(p => !p.IsDatabaseAutoIncrement()).ToArray();

            var columnNames = new StringBuilder();
            var columnValues = new StringBuilder();
            var propertiesCount = properties.Length;

            for (var i = 0; i < propertiesCount; i++)
            {
                if (i > 0)
                {
                    columnNames.Append(",");
                    columnValues.Append(",");
                }

                columnNames.Append($"{properties[i].GetDatabaseFieldName()}");

                columnValues.Append($"@{properties[i].Name}");

            }
            var insertQuery = $"INSERT INTO {tableName} ({columnNames}) VALUES ({columnValues});";


            return insertQuery;
        }

        public static string InsertQueryWithResult<T>()
        {
            var type = typeof(T).GetTypeInfo();
            var tableName = type.GetTableName();
            return GetQuery(type, SqlQueryType.Insert, tableName) ??
                   AddQuery(type, SqlQueryType.Insert, tableName, GenerateInsertQueryWithResult(type, tableName));
        }

        private static string GenerateInsertQueryWithResult(TypeInfo type, string tableName)
        {
            var properties = GetOrAdd(type);

            var columnNames = new StringBuilder();
            var columnValues = new StringBuilder();
            var propertiesCount = properties.Length;

            var selectQuery = SelectAllQuery(type);
            var condition = new StringBuilder();
            var conditionCounter = 0;

            Ensure.Argument.Is(properties.Any(p => p.IsDatabaseAutoIncrement() || p.IsDatabaseIdentity()));
            for (var i = 0; i < propertiesCount; i++)
            {
                if (i > 0)
                {
                    columnNames.Append(",");
                    columnValues.Append(",");
                }

                columnNames.Append($"{properties[i].GetDatabaseFieldName()}");
                columnValues.Append($"@{properties[i].Name}");

                if (conditionCounter > 0)
                {
                    condition.Append(" AND ");
                }

                if (properties[i].IsDatabaseIdentity() || properties[i].IsDatabaseAutoIncrement())
                {
                    var property = (properties[i].IsDatabaseAutoIncrement()
                        ? "SCOPE_IDENTITY()"
                        : "@" + properties[i].Name);
                    condition.Append($"`{properties[i].GetDatabaseFieldName()}` = {property}");
                    conditionCounter++;
                }
            }


            return $"INSERT INTO {tableName} ({columnNames}) VALUES ({columnValues});" +
                   $"{selectQuery} WHERE ";
        }

        #endregion

        #region Update
        /// <summary>
        /// MySql UPDATE
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static string UpdateQuery<T>()
        {
            var type = typeof(T).GetTypeInfo();
            var tableName = type.GetTableName();
            return GetQuery(type, SqlQueryType.Update, tableName)
                ?? AddQuery(type, SqlQueryType.Update, tableName, GenerateUpdateQuery(type, tableName));
        }

        private static string GenerateUpdateQuery(TypeInfo type, string tableName)
        {
            var properties = GetOrAdd(type);
            var updateFields = new StringBuilder();
            var condition = new StringBuilder();
            var conditionCounter = 0;
            for (var i = 0; i < properties.Length; i++)
            {
                if (!properties[i].IsDatabaseIdentity())
                {
                    if (i > 0)
                    {
                        updateFields.Append(",");
                    }
                    updateFields.Append($"`{properties[i].GetDatabaseFieldName()}` = @{properties[i].Name}");
                }
                if (properties[i].IsDatabaseIdentity())
                {
                    if (conditionCounter > 0)
                    {
                        condition.Append(" AND ");
                    }
                    condition.Append($"`{properties[i].GetDatabaseFieldName()}` = @{properties[i].Name}");
                    conditionCounter++;
                }
            }
            return $"UPDATE `{tableName}` SET {updateFields} WHERE {condition}";
        }
        #endregion

        #region Delete

        /// <summary>
        /// MySql DELETE
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static string DeleteQuery<T>()
        {
            var type = typeof(T).GetTypeInfo();
            var tableName = type.GetTableName();
            return GetQuery(type, SqlQueryType.Delete, tableName)
                ?? AddQuery(type, SqlQueryType.Delete, tableName, GenerateDeleteQuery(type, tableName));
        }

        private static string GenerateDeleteQuery(TypeInfo type, string tableName)
        {
            var properties = GetOrAdd(type);
            var condition = new StringBuilder();
            var conditionCounter = 0;
            foreach (var t in properties)
            {
                if (t.IsDatabaseIdentity())
                {
                    if (conditionCounter > 0)
                    {
                        condition.Append(" AND ");
                    }
                    condition.Append($"`{t.GetDatabaseFieldName()}` = @{t.Name}");
                    conditionCounter++;
                }
            }
            return $"DELETE FROM `{tableName}` WHERE {condition}";
        }
        #endregion

        #region Select All

        /// <summary>
        /// MySql SELECT (*)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static string SelectAllQuery<T>()
        {
            var type = typeof(T).GetTypeInfo();
            return SelectAllQuery(type);
        }

        public static string SelectAllQuery(TypeInfo type,string prefix = null)
        {
            var tableName = type.GetTableName();
            return GetQuery(type, SqlQueryType.SelectAll, tableName)
                ?? AddQuery(type, SqlQueryType.SelectAll,$"{tableName}{(prefix.IsNotNullOrTrimEmpty()?"":"_")}{prefix}", GenerateSelectAllQuery(type, tableName, prefix));
        }

        private static string GenerateSelectAllQuery(TypeInfo type, string tableName, string prefix = null)
        {
            var properties = GetOrAdd(type);
            var selectBody = new StringBuilder("SELECT ");
            for (var i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    selectBody.Append(",");
                selectBody.Append($"{prefix}{(prefix.IsNotNullOrTrimEmpty() ? "" : ".")}`{properties[i].GetDatabaseFieldName()}` as `{properties[i].Name}`");
            }
            selectBody.Append($" FROM {tableName} {prefix} ");

            return selectBody.ToString();
        }


        /// <summary>
        /// MySql SELECT (*)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="limit"></param>
        /// <param name="offet"></param>
        /// <returns></returns>
        public static string SelectAllQuery<T>(long limit, ulong offet = 0)
        {
            var type = typeof(T).GetTypeInfo();
            return SelectAllQuery(type, offet, limit);
        }

        public static string SelectAllQuery(TypeInfo type, ulong offet, long limit)
        {
            var tableName = type.GetTableName();
            return GetQuery(type, SqlQueryType.SelectAllWithLimit, tableName)
                ?? AddQuery(type, SqlQueryType.SelectAllWithLimit, tableName, GenerateSelectAllQuery(type, tableName, offet, limit));
        }

        private static string GenerateSelectAllQuery(TypeInfo type, string tableName, ulong offet, long limit)
        {
            var properties = GetOrAdd(type);
            var selectBody = new StringBuilder("SELECT ");
            for (var i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    selectBody.Append(",");
                selectBody.Append($"`{properties[i].GetDatabaseFieldName()}` as `{properties[i].Name}`");
            }
            selectBody.Append($" FROM {tableName} ");
            if (limit > 0)
            {
                selectBody.Append($" LIMIT {limit} OFFSET {offet}");
            }
            return selectBody.ToString();
        }
        #endregion

        #region Select query

        /// <summary>
        /// Gets dynamic SELECT query.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="properties"></param>
        /// <returns></returns>
        public static SqlQueryResult SelectQuery<T>(IDictionary<string, object> properties)
        {
            var whereFields = new StringBuilder();
            var entityProperties = GetOrAdd(typeof(T).GetTypeInfo());
            var parameters = new DynamicParameters();
            foreach (
                var p in
                    properties.Where(
                        p => entityProperties.Any(pr => pr.Name.IsIgnoreCaseEqual(p.Key))))
            {
                whereFields.Append($" AND `{p.Key}` = @{p.Key}");
                parameters.Add(p.Key, p.Value);
            }

            return new SqlQueryResult(string.Concat(SelectAllQuery<T>(), whereFields), parameters);
        }

        /// <summary>
        /// Gets the dynamic SELECT query.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <param name="offet"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public static SqlQueryResult SelectQuery<T>(Expression<Func<T, bool>> expression, ulong offet = 0, long limit = -1)
        {
            var type = typeof(T).GetTypeInfo();
            var properties = GetOrAdd(type);
            var queryProperties = new List<QueryParameter>();
            var body = (BinaryExpression)expression.Body;
            var parameters = new DynamicParameters();
            var builder = new StringBuilder();
            var tableName = type.GetTableName();

            // walk the tree and build up a list of query parameter objects
            // from the left and right branches of the expression tree
            WalkTree(body, ExpressionType.Default, ref queryProperties);

            // convert the query parms into a MySql string and dynamic property object
            builder.Append("SELECT ");
            for (var i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    builder.Append(",");
                builder.Append($"`{properties[i].GetDatabaseFieldName()}` as `{properties[i].Name}`");
            }
            builder.Append($" FROM `{tableName}` WHERE ");
            for (var i = 0; i < queryProperties.Count(); i++)
            {
                var item = queryProperties[i];

                if (!string.IsNullOrEmpty(item.LinkingOperator) && i > 0)
                {
                    builder.Append($"{item.LinkingOperator} `{item.PropertyName}` {item.QueryOperator} @{item.PropertyName} ");
                }
                else
                {
                    builder.Append($"`{item.PropertyName}` {item.QueryOperator} @{item.PropertyName} ");
                }

                parameters.Add(item.PropertyName, item.PropertyValue);
            }
            if (limit > 0)
            {
                builder.Append($" LIMIT {limit} OFFSET {offet}");
            }
            return new SqlQueryResult(builder.ToString().TrimEnd(), parameters);
        }

        /// <summary>
        /// http://stackoverflow.com/questions/33484295/dynamic-queries-in-dapper
        /// </summary>
        /// <param name="body"></param>
        /// <param name="linkingType"></param>
        /// <param name="queryProperties"></param>
        private static void WalkTree(BinaryExpression body, ExpressionType linkingType, ref List<QueryParameter> queryProperties)
        {
            if (body.NodeType != ExpressionType.AndAlso && body.NodeType != ExpressionType.OrElse)
            {
                var propertyName = ((MemberExpression)body.Left).Member.GetDatabaseFieldName();
                var propertyValue = Expression.Lambda(body.Right).Compile().DynamicInvoke();
                var opr = GetOperator(body.NodeType);
                var link = GetOperator(linkingType);

                queryProperties.Add(new QueryParameter(link, propertyName, propertyValue, opr));
            }
            else
            {
                WalkTree((BinaryExpression)body.Left, body.NodeType, ref queryProperties);
                WalkTree((BinaryExpression)body.Right, body.NodeType, ref queryProperties);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="System.NotImplementedException"></exception>
        private static string GetOperator(ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.AndAlso:
                case ExpressionType.And:
                    return "AND";
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return "OR";
                case ExpressionType.Default:
                    return string.Empty;
                default:
                    throw new NotSupportedException(type.ToString());
            }
        }
        #endregion
    }

    internal class QueryParameter
    {
        public string LinkingOperator { get; set; }
        public string PropertyName { get; set; }
        public object PropertyValue { get; set; }
        public string QueryOperator { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="linkingOperator"></param>
        /// <param name="propertyName"></param>
        /// <param name="propertyValue"></param>
        /// <param name="queryOperator"></param>
        internal QueryParameter(string linkingOperator, string propertyName, object propertyValue, string queryOperator)
        {
            LinkingOperator = linkingOperator;
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            QueryOperator = queryOperator;
        }
    }
}