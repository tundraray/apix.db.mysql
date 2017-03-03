using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Apix.Extensions;
using Dapper;
using MySql.Data.MySqlClient;

namespace Apix.Db.Mysql
{
    internal static class DataRepositoryCache
    {
        private static readonly PropertyCache GlobalPropertiesCache = new PropertyCache();

        public static PropertyInfo[] GetOrAdd(Type entityType)
        {
            var properties = GlobalPropertiesCache.GetOrAdd(entityType.TypeHandle, key =>
                                                                    (from p in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                                                     let attr = p.GetCustomAttribute(typeof(NotDatabaseFieldAttribute))
                                                                     where attr == null && p.GetSetMethod(true) != null && p.GetGetMethod(true) != null
                                                                     select p).ToDictionary(p => p.Name));
            return properties.Values.ToArray();
        }
    }
    /// <summary>
    /// Base SQL entity repository
    /// </summary>
    public class MysqlDataRepositoryBase<T> : IDisposable
        where T : new()
    {
        #region Privates
        private readonly Lazy<PropertyInfo[]> _propertiesThatExist = new Lazy<PropertyInfo[]>(() => DataRepositoryCache.GetOrAdd(typeof(T)));
        private static readonly object Obj = new object();
        private readonly MySqlConnection _conn;
        #endregion

        #region Fields
        protected string tableName = "";
        private bool _disposed = false;
        private readonly object objectLock = new object();

        #endregion

        #region Constructors

        public MysqlDataRepositoryBase(string conn) : this(new MySqlConnection(conn)) { }

        public MysqlDataRepositoryBase(MySqlConnection conn)
        {
            _conn = conn;
            _conn.Open();
        }

        #endregion
        /// <summary>
        /// Repository entity properties
        /// </summary>
        protected PropertyInfo[] PropertiesThatExist { get { return _propertiesThatExist.Value; } }
        /// <summary>
        /// Repository entity type
        /// </summary>
        protected Type EntityType { get { return typeof(T); } }
        /// <summary>
        /// Main SQL connection
        /// </summary>
        public MySqlConnection Connection => _conn;


        /// <summary>
        /// Entity table id field name
        /// </summary>
        protected virtual string IdentityName
        {
            get { return "id"; }
        }
        /// <summary>
        /// Entity table name
        /// </summary>
        protected virtual string TableName
        {
            get
            {
                return tableName.IsNotNullOrEmpty() ? tableName : EntityType.Name;
            }
        }

        /// <summary>
        /// Create entity
        /// </summary>
        /// <param name="entity">Entity</param>
        /// <param name="userId">Unique user ID who creating entity</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task CreateAsync(T entity, Guid userId)
        {
            await Connection.ExecuteAsync(SqlGenerator.InsertStatement<T>(TableName, IdentityName), entity).ConfigureAwait(false);
        }


        /// <summary>
        /// Update entity
        /// </summary>
        /// <param name="entity">Entity</param>
        /// <param name="userId">Unique user ID who updating entity</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task UpdateAsync(T entity, Guid userId)
        {
            await Connection.ExecuteAsync(SqlGenerator.UpdateStatement<T>(TableName, IdentityName), entity).ConfigureAwait(false);
        }

        /// <summary>
        /// Get all entities
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of stored entities</returns>
        public Task<IEnumerable<T>> GetAllAsync()
        {
            return Connection.QueryAsync<T>(SqlGenerator.SelectAllStatement<T>(TableName));
        }

        public Task<T> GetByCriteriaAsync(Expression<Func<T, bool>> predicate, string tableName = null)
        {
            tableName = tableName.IsNullOrEmpty() ? TableName : tableName;
            var result = SqlGenerator.SelectQuery<T>(tableName, predicate);
            return Connection.QueryFirstOrDefaultAsync<T>(result.Sql, result.Param);
        }

        public Task<IEnumerable<T>> ListByCriteriaAsync(Expression<Func<T, bool>> predicate)
        {
            var result = SqlGenerator.SelectQuery<T>(TableName, predicate);
            return Connection.QueryAsync<T>(result.Sql, result.Param);
        }

        public Task ExecuteAsync(string sql, object predicate)
        {
            return Connection.ExecuteAsync(sql, predicate);
        }


        #region Find

        /// <summary>
        /// Find entities by expression predicate
        /// </summary>
        /// <param name="predicate">Expression predicate</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of entities</returns>
        public Task<IEnumerable<T>> FindAsync(Expression<Func<T, bool>> predicate)
        {
            Ensure.Argument.NotNull(predicate, nameof(predicate));
            var result = SqlGenerator.SelectQuery(TableName, predicate);
            return Connection.QueryAsync<T>(result.Sql, result.Param);
        }
        #endregion

        #region Create Statement
        /// <summary>
        /// Get INSERT statement
        /// </summary>
        /// <param name="type">Entity type</param>
        /// <returns>T-SQL INSERT string</returns>
        protected string GetCreateStatement(Type type)
        {
            string createStatement = QueryCache.GetQuery(type, SqlQueryType.Create, TableName);
            if (createStatement == null)
            {
                createStatement = GenerateCreateStatement(type);
                QueryCache.AddQuery(EntityType, SqlQueryType.Create, TableName, createStatement);
            }

            return createStatement;
        }
        /// <summary>
        /// Get INSERT statement for repository entity
        /// </summary>
        /// <returns>T-SQL INSERT string</returns>
        protected string GetCreateStatement()
        {
            string createStatement = QueryCache.GetQuery(EntityType, SqlQueryType.Create, TableName);
            if (createStatement == null)
            {
                createStatement = GenerateCreateStatement();
                QueryCache.AddQuery(EntityType, SqlQueryType.Create, TableName, createStatement);
            }

            return createStatement;
        }
        /// <summary>
        /// Generate INSERT statement
        /// </summary>
        /// <param name="properties">Entity properties</param>
        /// <returns>T-SQL INSERT string</returns>
        protected virtual string GenerateCreateStatement(PropertyInfo[] properties)
        {
            var insertStatement = "INSERT INTO `" + TableName + "`";
            var columnNames = new StringBuilder("(");
            var columnValues = new StringBuilder(" VALUES (");
            int propertiesCount = properties.Length;

            for (int i = 0; i < propertiesCount; i++)
            {
                if (i > 0)
                {
                    columnNames.Append(",");
                    columnValues.Append(",");
                }

                if (properties[i].Name.IsIgnoreCaseEqual(IdentityName))
                {
                    columnNames.Append("`" + IdentityName + "`");
                }
                else
                {
                    columnNames.Append("`" + properties[i].Name + "`");
                }

                columnValues.Append("@" + properties[i].Name);

                if (i == propertiesCount - 1)
                {
                    columnNames.Append(")");
                    columnValues.Append(")");
                }
            }
            return insertStatement + columnNames + columnValues;
        }
        /// <summary>
        /// Generate INSERT statement
        /// </summary>
        /// <param name="type">Entity type</param>
        /// <returns>T-SQL INSERT string</returns>
        protected virtual string GenerateCreateStatement(Type type)
        {
            var properties = DataRepositoryCache.GetOrAdd(type);
            return GenerateCreateStatement(properties);
        }
        /// <summary>
        /// Generate INSERT statement for repository entity
        /// </summary>
        /// <returns>T-SQL INSERT string</returns>
        protected virtual string GenerateCreateStatement()
        {
            return GenerateCreateStatement(PropertiesThatExist);
        }

        #endregion

        #region Update Statement
        /// <summary>
        /// Get UPDATE statement
        /// </summary>
        /// <returns>T-SQL UPDATE string</returns>
        protected string GetUpdateStatement()
        {
            string updateStatement = QueryCache.GetQuery(EntityType, SqlQueryType.Update, TableName);
            if (updateStatement == null)
            {
                updateStatement = GenerateUpdateStatement();
                QueryCache.AddQuery(EntityType, SqlQueryType.Update, TableName, updateStatement);
            }

            return updateStatement;
        }
        /// <summary>
        /// Generate UPDATE statement
        /// </summary>
        /// <param name="properties">Entity properties</param>
        /// <returns>T-SQL UPDATE string</returns>
        protected virtual string GenerateUpdateStatement(PropertyInfo[] properties)
        {
            var updateStatement = "UPDATE `" + TableName + "`";
            var updateFields = new StringBuilder(" SET ");
            var condition = new StringBuilder(" WHERE `" + IdentityName + "` = @id");
            for (int i = 0; i < properties.Length; i++)
            {
                if (!properties[i].Name.IsIgnoreCaseEqual("id"))
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
        /// <summary>
        /// Generate UPDATE statement
        /// </summary>
        /// <param name="type">Entity type</param>
        /// <returns>T-SQL UPDATE string</returns>
        protected virtual string GenerateUpdateStatement(Type type)
        {
            var properties = DataRepositoryCache.GetOrAdd(type);
            return GenerateUpdateStatement(properties);
        }
        /// <summary>
        /// Generate UPDATE statement for repository entity
        /// </summary>
        /// <returns>T-SQL UPDATE string</returns>
        protected virtual string GenerateUpdateStatement()
        {
            return GenerateUpdateStatement(PropertiesThatExist);
        }

        #endregion

        #region Select All Statement
        /// <summary>
        /// Get SELECT statement
        /// </summary>
        /// <returns>T-SQL SELECT string</returns>
        protected string GetSelectAllStatement()
        {
            var selectAllStatement = QueryCache.GetQuery(EntityType, SqlQueryType.SelectAll, TableName);
            if (selectAllStatement == null)
            {
                selectAllStatement = GenerateSelectAllStatement();
                QueryCache.AddQuery(EntityType, SqlQueryType.SelectAll, TableName, selectAllStatement);
            }

            return selectAllStatement;
        }
        /// <summary>
        /// Generate SELECT statement
        /// </summary>
        /// <param name="properties">Entity properties</param>
        /// <returns>T-SQL SELECT string</returns>
        protected virtual string GenerateSelectAllStatement(PropertyInfo[] properties)
        {
            var selectBody = new StringBuilder("SELECT ");
            for (int i = 0; i < properties.Length; i++)
            {
                if (i > 0)
                    selectBody.Append(",");
                selectBody.Append("`" + properties[i].Name + "`");
            }
            var fromBody = " FROM `" + TableName + "`";
            return selectBody + fromBody;
        }
        /// <summary>
        /// Generate SELECT statement
        /// </summary>
        /// <param name="type">Entity type</param>
        /// <returns>T-SQL SELECT string</returns>
        protected virtual string GenerateSelectAllStatement(Type type)
        {
            var properties = DataRepositoryCache.GetOrAdd(type);
            return GenerateSelectAllStatement(properties);
        }
        /// <summary>
        /// Generate SELECT statement for repository entity
        /// </summary>
        /// <returns>T-SQL SELECT string</returns>
        protected virtual string GenerateSelectAllStatement()
        {
            return GenerateSelectAllStatement(PropertiesThatExist);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            lock (objectLock)
            {
                if (_disposed == false && disposing == true)
                {

                    if (_conn != null)
                    {
                        _conn.Close();
                        _conn.Dispose();
                    }
                    _disposed = true;

                }
            }
        }

        #endregion

    }

    class PropertyContainer
    {
        private readonly Dictionary<string, object> _ids;
        private readonly Dictionary<string, object> _values;

        #region Properties

        internal IEnumerable<string> IdNames
        {
            get { return _ids.Keys; }
        }

        internal IEnumerable<string> ValueNames
        {
            get { return _values.Keys; }
        }

        internal IEnumerable<string> AllNames
        {
            get { return _ids.Keys.Union(_values.Keys); }
        }

        internal IDictionary<string, object> IdPairs
        {
            get { return _ids; }
        }

        internal IDictionary<string, object> ValuePairs
        {
            get { return _values; }
        }

        internal IEnumerable<KeyValuePair<string, object>> AllPairs
        {
            get { return _ids.Concat(_values); }
        }

        #endregion

        #region Constructor

        internal PropertyContainer()
        {
            _ids = new Dictionary<string, object>();
            _values = new Dictionary<string, object>();
        }

        #endregion

        #region Methods

        internal void AddId(string name, object value)
        {
            _ids.Add(name, value);
        }

        internal void AddValue(string name, object value)
        {
            _values.Add(name, value);
        }

        #endregion
    }
}