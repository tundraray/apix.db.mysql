using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// Dapper extension methods
    /// </summary>
    public static class DapperHelper
    {
        #region Async methods

        /// <summary>
        /// Execute query stored procedure
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="procedureName"></param>
        /// <param name="procedureParams"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<IEnumerable<T>> ExecuteQueryProcedureAsync<T>(
            this MySqlConnection connection,
            string procedureName,
            object procedureParams = null,
            int commandTimeout = 30,
            CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteQueryAsync(connection,
                (c, ct) => c.QueryAsync<T>(
                    new CommandDefinition(procedureName, procedureParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

 
        /// <summary>
        /// Execute non-query stored procedure
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="procedureName"></param>
        /// <param name="procedureParams"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task ExecuteNonQueryProcedureAsync(
            this MySqlConnection connection,
            string procedureName,
            object procedureParams = null,
            int commandTimeout = 30,
            CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteNonQueryAsync(connection,
                    (c, ct) => c.ExecuteAsync(
                        new CommandDefinition(procedureName, procedureParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Execute non-query MySql script
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="query"></param>
        /// <param name="queryParams"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task ExecuteNonQueryAsync(
            this MySqlConnection connection,
            string query,
            object queryParams = null,
            int commandTimeout = 30,
            CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteNonQueryAsync(connection,
                    (c, ct) => c.ExecuteAsync(
                        new CommandDefinition(query, queryParams, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="func"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task ExecuteNonQueryAsync(
            this MySqlConnection connection,
            Func<MySqlConnection, CancellationToken, Task> func,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                await func(connection, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="queryStatement"></param>
        /// <param name="queryParams"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<IEnumerable<T>> ExecuteQueryAsync<T>(
            this MySqlConnection connection,
            string queryStatement,
            object queryParams = null,
            int commandTimeout = 30,
            CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteQueryAsync(connection,
                (c, ct) => c.QueryAsync<T>(
                    new CommandDefinition(queryStatement, queryParams, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="queryStatement"></param>
        /// <param name="queryParams"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<T> ExecuteQueryFirstOrDefaultAsync<T>(
            this MySqlConnection connection,
            string queryStatement,
            object queryParams = null,
            int commandTimeout = 30,
            CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteQuerWithTransactonAsync(connection,
                (c, ct) => c.QueryFirstOrDefaultAsync<T>(queryStatement, queryParams, commandTimeout: commandTimeout, transaction: ct), cancellationToken);


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="taskFunc"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<IEnumerable<T>> ExecuteQueryAsync<T>(
            this MySqlConnection connection,
            Func<MySqlConnection, CancellationToken, Task<IEnumerable<T>>> taskFunc,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                return await taskFunc(connection, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="taskFunc"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<T> ExecuteQuerWithTransactonAsync<T>(
            this MySqlConnection connection,
            Func<MySqlConnection, IDbTransaction, Task<T>> taskFunc,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                T result;
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                var transaction = connection.BeginTransaction();
                try
                {
                    result = await taskFunc(connection, transaction).ConfigureAwait(false);
                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
                return result;
            }
        }

        public static async Task ExecuteWithTransactonAsync(
            this MySqlConnection connection,
            Func<MySqlConnection, CancellationToken, Task> taskFunc,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                var transaction = connection.BeginTransaction();
                try
                {
                    await taskFunc(connection, cancellationToken).ConfigureAwait(false);
                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        public static Task ExecuteTransactionNonQueryAsync(
            this MySqlConnection connection,
            string query,
            object queryParams = null,
            int commandTimeout = 30,
            CancellationToken cancellationToken = default(CancellationToken))
           => ExecuteWithTransactonAsync(connection,
                   (c, ct) => c.ExecuteAsync(
                       new CommandDefinition(query, queryParams, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        #endregion
    }
}