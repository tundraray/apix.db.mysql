using System;
using Dapper;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// A result object with the generated sql and dynamic params.
    /// </summary>
    public class SqlQueryResult
    {
        /// <summary>
        /// The _result
        /// </summary>
        private readonly Tuple<string, DynamicParameters> _result;
        /// <summary>
        /// Gets the SQL.
        /// </summary>
        /// <value>
        /// The SQL.
        /// </value>
        public string Sql => _result.Item1;
        /// <summary>
        /// Gets the param.
        /// </summary>
        /// <value>
        /// The param.
        /// </value>
        public DynamicParameters Param => _result.Item2;
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlQueryResult" /> class.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <param name="param">The param.</param>
        public SqlQueryResult(string sql, DynamicParameters param)
        {
            _result = new Tuple<string, DynamicParameters>(sql, param);
        }
    }
}