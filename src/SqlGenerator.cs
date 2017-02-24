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
    /// SQL generator
    /// </summary>
    public static class SqlGenerator
    {
        #region Cache

        private static readonly PropertyCache GlobalPropertiesCache = new PropertyCache();
        /// <summary>
        /// Get entity properties
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <returns>List of entity properties excluding marked as <see cref="NotRepositoryFieldAttribute"/></returns>
        public static PropertyInfo[] GetOrAdd(TypeInfo entityType)
        {
            var properties = GlobalPropertiesCache.GetOrAdd(entityType.AsType().TypeHandle, key =>
                                                                    (from p in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                                                     let attr = p.GetCustomAttribute(typeof(NotRepositoryFieldAttribute))
                                                                     where attr == null && p.GetSetMethod(true) != null && p.GetGetMethod(true) != null
                                                                     select p).ToDictionary(p => p.Name));
            return properties.Values.ToArray();
        }
        private static readonly ConcurrentDictionary<string, string> Cache = new ConcurrentDictionary<string, string>();
        /// <summary>
        /// Get SQL query string
        /// </summary>
        /// <param name="type">Repository type</param>
        /// <param name="queryType">Query type</param>
        /// <param name="tableName">Repository table name</param>
        /// <returns>Stored SQL query string</returns>
        public static string GetQuery(TypeInfo type, string queryType, string tableName)
        {
            string query;
            Cache.TryGetValue(GetKey(type, queryType, tableName), out query);
            return query;
        }

        private static string GetKey(TypeInfo type, string queryType, string tableName)
        {
            return string.Join("_", type.Name, queryType, tableName);
        }
        /// <summary>
        /// Add SQL query string
        /// </summary>
        /// <param name="type">Repository type</param>
        /// <param name="queryType">Query type</param>
        /// <param name="tableName">Repository table name</param>
        /// <param name="query">Storing SQL query string</param>
        public static string AddQuery(TypeInfo type, string queryType, string tableName, string query)
        {
            var key = GetKey(type, queryType, tableName);
            return Cache.GetOrAdd(key, k => query);
        }
        #endregion

        #region Insert
        /// <summary>
        /// SQL INSERT
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Table name</param>
        /// <param name="identityName">Identity name</param>
        /// <returns>SQL statement</returns>
        public static string InsertStatement<T>(string tableName, string identityName)
        {
            var type = typeof(T).GetTypeInfo();
            return GetQuery(type, SqlQueryType.Insert, tableName) ??
                   AddQuery(type, SqlQueryType.Insert, tableName, GenerateInsertStatement(type, tableName, identityName));
        }

        private static string GenerateInsertStatement(TypeInfo type, string tableName, string identityName)
        {
            var properties = GetOrAdd(type);
            var insertStatement = $"INSERT INTO `{tableName}`";
            var columnNames = new StringBuilder("(");
            var columnValues = new StringBuilder(" VALUES (");
            var propertiesCount = properties.Length;



            for (var i = 0; i < propertiesCount; i++)
            {
                if (i > 0)
                {
                    columnNames.Append(",");
                    columnValues.Append(",");
                }

                columnNames.Append(properties[i].Name.IsIgnoreCaseEqual(identityName)
                    ? $"{identityName}"
                    : $"{properties[i].Name}");

                columnValues.Append($"@{properties[i].Name}");

                if (i == propertiesCount - 1)
                {
                    columnNames.Append(")");
                    columnValues.Append(")");
                }
            }
            return insertStatement + columnNames + columnValues;
        }
        #endregion

        #region Update
        /// <summary>
        /// SQL UPDATE
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Table name</param>
        /// <param name="identityName">Identity name</param>
        /// <returns>SQL statement</returns>
        public static string UpdateStatement<T>(string tableName, string identityName)
        {
            var type = typeof(T).GetTypeInfo();
            return GetQuery(type, SqlQueryType.Update, tableName)
                ?? AddQuery(type, SqlQueryType.Update, tableName, GenerateUpdateStatement(type, tableName, identityName));
        }

        private static string GenerateUpdateStatement(TypeInfo type, string tableName, string identityName)
        {
            var properties = GetOrAdd(type);
            var updateStatement = $"UPDATE {tableName}";
            var updateFields = new StringBuilder(" SET ");
            var condition = new StringBuilder($" WHERE `{identityName}` = @{identityName}");
            for (var i = 0; i < properties.Length; i++)
            {
                if (!properties[i].Name.IsIgnoreCaseEqual(identityName))
                {
                    if (i > 0)
                    {
                        updateFields.Append(",");
                    }
                    updateFields.Append("`" + properties[i].Name + "` = @" + properties[i].Name);
                }
            }
            return updateStatement + updateFields + condition;
        }
        #endregion

        #region Delete
        /// <summary>
        /// SQL DELETE
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Table name</param>
        /// <param name="identityName">Identity name</param>
        /// <returns>SQL statement</returns>
        public static string DeleteStatement<T>(string tableName, string identityName)
        {
            var type = typeof(T).GetTypeInfo();
            return GetQuery(type, SqlQueryType.Delete, tableName)
                ?? AddQuery(type, SqlQueryType.Delete, tableName, GenerateDeleteStatement(type, tableName, identityName));
        }

        private static string GenerateDeleteStatement(TypeInfo type, string tableName, string identityName)
        {
            return $"DELETE FROM `{tableName}` WHERE `{identityName}` = @{identityName}";
        }
        #endregion

        #region Select by ID
        /// <summary>
        /// SQL DELETE
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Table name</param>
        /// <param name="identityName">Identity name</param>
        /// <returns>SQL statement</returns>
        public static string SelectByIdStatement<T>(string tableName, string identityName)
        {
            var type = typeof(T).GetTypeInfo();
            return GetQuery(type, SqlQueryType.SelectById, tableName)
                ?? AddQuery(type, SqlQueryType.SelectById, tableName, GenerateSelectByIdStatement(type, tableName, identityName));
        }

        private static string GenerateSelectByIdStatement(TypeInfo type, string tableName, string identityName)
        {
            var properties = GetOrAdd(type);
            var selectBody = new StringBuilder("SELECT ");
            for (var i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    selectBody.Append(",");
                selectBody.Append( properties[i].Name );
            }
            selectBody.Append($" FROM `{tableName}`");
            selectBody.Append($" WHERE `{identityName}` = @{identityName}");
            return selectBody.ToString();
        }
        #endregion

        #region Select All
        /// <summary>
        /// SQL SELECT (*)
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Table name</param>
        /// <returns>SQL statement</returns>
        public static string SelectAllStatement<T>(string tableName)
        {
            var type = typeof(T).GetTypeInfo();
            return GetQuery(type, SqlQueryType.SelectAll, tableName)
                ?? AddQuery(type, SqlQueryType.SelectAll, tableName, GenerateSelectAllStatement(type, tableName));
        }

        private static string GenerateSelectAllStatement(TypeInfo type, string tableName)
        {
            var properties = GetOrAdd(type);
            var selectBody = new StringBuilder("SELECT ");
            for (var i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    selectBody.Append(",");
                selectBody.Append($"{properties[i].Name}");
            }
            selectBody.Append($" FROM {tableName} ");
            return selectBody.ToString();
        }
        #endregion

        #region Select query
        /// <summary>
        /// Gets the dynamic SELECT query.
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Name of the table</param>
        /// <param name="properties">List of update properties</param>
        /// <returns></returns>
        public static SqlQueryResult SelectQuery<T>(string tableName, IDictionary<string, object> properties)
        {
            var whereFields = new StringBuilder();
            var entityProperties = GetOrAdd(typeof(T).GetTypeInfo());
            var parameters = new DynamicParameters();
            foreach (
                var p in
                    properties.Where(
                        p => entityProperties.Any(pr => pr.Name.IsIgnoreCaseEqual(p.Key))))
            {
                whereFields.Append($" AND {p.Key} = @{p.Key}");
                parameters.Add(p.Key, p.Value);
            }
            return new SqlQueryResult(string.Concat(SelectAllStatement<T>(tableName), whereFields), parameters);
        }
        /// <summary>
        /// Gets the dynamic SELECT query.
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="expression">The expression.</param>
        /// <returns>A result object with the generated sql and dynamic params.</returns>
        public static SqlQueryResult SelectQuery<T>(string tableName, Expression<Func<T, bool>> expression)
        {
            var properties = GetOrAdd(typeof(T).GetTypeInfo());
            var queryProperties = new List<QueryParameter>();
            var body = (BinaryExpression)expression.Body;
            var parameters = new DynamicParameters();
            var builder = new StringBuilder();

            // walk the tree and build up a list of query parameter objects
            // from the left and right branches of the expression tree
            WalkTree(body, ExpressionType.Default, ref queryProperties);

            // convert the query parms into a SQL string and dynamic property object
            builder.Append("SELECT ");
            for (var i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    builder.Append(",");
                builder.Append($"{properties[i].Name}");
            }
            builder.Append($" FROM {tableName} WHERE ");
            for (var i = 0; i < queryProperties.Count(); i++)
            {
                var item = queryProperties[i];

                if (!string.IsNullOrEmpty(item.LinkingOperator) && i > 0)
                {
                    builder.Append($"{item.LinkingOperator} {item.PropertyName} {item.QueryOperator} @{item.PropertyName} ");
                }
                else
                {
                    builder.Append($"{item.PropertyName} {item.QueryOperator} @{item.PropertyName} ");
                }

                parameters.Add(item.PropertyName, item.PropertyValue);
            }
            return new SqlQueryResult(builder.ToString().TrimEnd(), parameters);
        }
        /// <summary>
        /// Walks the tree.
        /// </summary>
        /// <param name="body">The body.</param>
        /// <param name="linkingType">Type of the linking.</param>
        /// <param name="queryProperties">The query properties.</param>
        private static void WalkTree(BinaryExpression body, ExpressionType linkingType, ref List<QueryParameter> queryProperties)
        {
            if (body.NodeType != ExpressionType.AndAlso && body.NodeType != ExpressionType.OrElse)
            {
                var propertyName = ((MemberExpression)body.Left).Member.Name;
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
        /// Gets the operator.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        /// The expression types SQL server equivalent operator.
        /// </returns>
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

    /// <summary>
    /// Class that models the data structure in coverting the expression tree into SQL and Params.
    /// </summary>
    internal class QueryParameter
    {
        public string LinkingOperator { get; set; }
        public string PropertyName { get; set; }
        public object PropertyValue { get; set; }
        public string QueryOperator { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryParameter" /> class.
        /// </summary>
        /// <param name="linkingOperator">The linking operator.</param>
        /// <param name="propertyName">Name of the property.</param>
        /// <param name="propertyValue">The property value.</param>
        /// <param name="queryOperator">The query operator.</param>
        internal QueryParameter(string linkingOperator, string propertyName, object propertyValue, string queryOperator)
        {
            LinkingOperator = linkingOperator;
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            QueryOperator = queryOperator;
        }
    }
}