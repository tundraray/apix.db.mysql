using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// Dapper extension methods
    /// </summary>
    public static class DapperHelper
    {
        #region Async methods

        /// <summary>
        /// Asynchronous execute query stored procedure
        /// </summary>
        /// <typeparam name="T">Output entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="spParams">Stored procedure input parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Output entities</returns>
        public static Task<IEnumerable<T>> ExecuteQueryProcedureAsync<T>(this DbConnection connection, string spName, object spParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteQueryAsync(connection,
                (c, ct) => c.QueryAsync<T>(
                    new CommandDefinition(spName, spParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Asynchronous execute non-query stored procedure with transaction
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="spParams">Stored procedure input parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static Task ExecuteTransactNonQueryProcedureAsync(this DbConnection connection, string spName, object spParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteWithTransactonAsync(connection,
                (c, ct) => c.ExecuteAsync(
                    new CommandDefinition(spName, spParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Asynchronous execute non-query stored procedure
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="spParams">Stored procedure input parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static Task ExecuteNonQueryProcedureAsync(this DbConnection connection, string spName, object spParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteNonQueryAsync(connection,
                    (c, ct) => c.ExecuteAsync(
                        new CommandDefinition(spName, spParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Asynchronous execute non-query SQL script
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="sqlScript">SQL script</param>
        /// <param name="queryParams">SQL script parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static Task ExecuteNonQueryAsync(this DbConnection connection, string sqlScript, object queryParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteNonQueryAsync(connection,
                    (c, ct) => c.ExecuteAsync(
                        new CommandDefinition(sqlScript, queryParams, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Asynchronous execute non-query SQL script with transaction
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="sqlScript">SQL script</param>
        /// <param name="queryParams">SQL script parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// /// <param name="cancellationToken">Cancellation token</param>
        public static Task ExecuteTransactionNonQueryAsync(this DbConnection connection, string sqlScript, object queryParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
           => ExecuteWithTransactonAsync(connection,
                   (c, ct) => c.ExecuteAsync(
                       new CommandDefinition(sqlScript, queryParams, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Asynchronous execute <see cref="Action{DbConnection}"/> with SQL connection
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="taskFunc">Run function</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task ExecuteNonQueryAsync(this DbConnection connection, Func<DbConnection, CancellationToken, Task> taskFunc, CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                await taskFunc(connection, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Asynchronous execute query SQL script
        /// </summary>
        /// <typeparam name="T">Output entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="queryStatement">SQL script</param>
        /// <param name="queryParams">SQL script parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Output entities</returns>
        public static Task<IEnumerable<T>> ExecuteQueryAsync<T>(this DbConnection connection, string queryStatement, object queryParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteQueryAsync(connection,
                (c, ct) => c.QueryAsync<T>(
                    new CommandDefinition(queryStatement, queryParams, commandTimeout: commandTimeout, cancellationToken: ct)), cancellationToken);

        /// <summary>
        /// Asynchronous execute a single-row query SQL script
        /// </summary>
        /// <typeparam name="T">Output entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="queryStatement">SQL script</param>
        /// <param name="queryParams">SQL script parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Output first <see cref="T"/></returns>
        public static Task<T> ExecuteQueryFirstOrDefaultAsync<T>(this DbConnection connection, string queryStatement, object queryParams = null, int commandTimeout = 30, CancellationToken cancellationToken = default(CancellationToken))
            => ExecuteQuerWithTransactonAsync(connection,
                (c, ct) => c.QueryFirstOrDefaultAsync<T>(queryStatement, queryParams, commandTimeout: commandTimeout,transaction: ct), cancellationToken);


        /// <summary>
        /// Asynchronous execute <see cref="Func{DbConnection, Task}"/> with SQL connection
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="taskFunc">Run function</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task<IEnumerable<T>> ExecuteQueryAsync<T>(this DbConnection connection, Func<DbConnection, CancellationToken, Task<IEnumerable<T>>> taskFunc, CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                return await taskFunc(connection, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Safe execute <see cref="Action{DbConnection}"/> with SQL connection inside transaction.
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="taskFunc">Run fucntion</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task ExecuteWithTransactonAsync(this DbConnection connection, Func<DbConnection, CancellationToken, Task> taskFunc, CancellationToken cancellationToken = default(CancellationToken))
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

        /// <summary>
        /// Safe execute <see cref="Action{DbConnection}"/> with SQL connection inside transaction.
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="taskFunc">Run fucntion</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Output first <see cref="T"/></returns>
        public static async Task<T> ExecuteQuerWithTransactonAsync<T>(this DbConnection connection, Func<DbConnection, IDbTransaction, Task<T>> taskFunc, CancellationToken cancellationToken = default(CancellationToken))
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

        /// <summary>
        /// Safe execute multiple action inside transaction 
        /// </summary>
        /// <typeparam name="T">Input entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="entities">Input entities</param>
        /// <param name="action">Entity processing run action</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task TryExecuteBulkWithTransactionAsync<T>(this DbConnection connection, IEnumerable<T> entities, Func<DbConnection, T, Task> action, CancellationToken cancellationToken = default(CancellationToken))
        {
            using (connection)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                var transaction = connection.BeginTransaction();
                try
                {
                    foreach (var entity in entities)
                    {
                        await action(connection, entity);
                    }
                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        
        #endregion
    }
}