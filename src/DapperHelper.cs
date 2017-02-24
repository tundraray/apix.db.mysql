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
        #region Sync methods
        /// <summary>
        /// Execute query stored procedure
        /// </summary>
        /// <typeparam name="T">Output entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="spParams">Stored procedure input parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <returns>Output entities</returns>
        public static IEnumerable<T> ExecuteProcedure<T>(this DbConnection connection, string spName, object spParams = null, int commandTimeout = 30)
        {
            IEnumerable<T> result;
            using (connection)
            {
                connection.Open();
                result = connection.Query<T>(spName, spParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout);
                connection.Close();
            }
            return result;
        }
        /// <summary>
        /// Execute non-query stored procedure
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="spParams">Stored procedure input parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        public static void ExecuteNonQuery(this DbConnection connection, string spName, object spParams = null, int commandTimeout = 30)
        {
            using (connection)
            {
                connection.Open();
                connection.Execute(spName, spParams, commandType: CommandType.StoredProcedure, commandTimeout: commandTimeout);
                connection.Close();
            }
        }
        /// <summary>
        /// Execute non-query SQL script
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="sqlScript">SQL script</param>
        /// <param name="queryParams">SQL script parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        public static void ExecuteScript(this DbConnection connection, string sqlScript, object queryParams = null, int commandTimeout = 30)
        {
            using (connection)
            {
                connection.Open();
                connection.Execute(sqlScript, queryParams, commandTimeout: commandTimeout);
                connection.Close();
            }
        }
        /// <summary>
        /// Execute query SQL script
        /// </summary>
        /// <typeparam name="T">Output entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="queryStatement">SQL script</param>
        /// <param name="queryParams">SQL script parameters</param>
        /// <param name="commandTimeout">Command run time-out</param>
        /// <returns>Output entities</returns>
        public static IEnumerable<T> ExecuteQuery<T>(this DbConnection connection, string queryStatement, object queryParams = null, int commandTimeout = 30)
        {
            IEnumerable<T> result;
            using (connection)
            {
                connection.Open();
                result = connection.Query<T>(queryStatement, queryParams, commandTimeout: commandTimeout);
                connection.Close();
            }
            return result;
        }
        /// <summary>
        /// Execute <see cref="Action{DbConnection}"/> with SQL connection
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="action">Run action</param>
        public static void ExecuteAction(this DbConnection connection, Action<DbConnection> action)
        {
            using (connection)
            {
                connection.Open();
                action(connection);
                connection.Close();
            }
        }
        /// <summary>
        /// Safe execute <see cref="Action{DbConnection}"/> with SQL connection
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="action">Run action</param>
        public static void TryExecuteAction(this DbConnection connection, Action<DbConnection> action)
        {
            using (connection)
            {
                connection.Open();
                try
                {
                    action(connection);
                }
                finally
                {
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// Safe execute <see cref="Action{DbConnection}"/> with SQL connection inside transaction.
        /// </summary>
        /// <param name="connection">SQL connection</param>
        /// <param name="action">Run action</param>
        public static void TryExecuteWithTransacton(this DbConnection connection, Action<DbConnection> action)
        {
            using (connection)
            {
                connection.Open();
                var transaction = connection.BeginTransaction();
                try
                {
                    action(connection);
                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// Safe execute multiple action inside transaction 
        /// </summary>
        /// <typeparam name="T">Input entity type</typeparam>
        /// <param name="connection">SQL connection</param>
        /// <param name="entities">Input entities</param>
        /// <param name="action">Entity processing run action</param>
        public static void TryExecuteBulkWithTransaction<T>(this DbConnection connection, IEnumerable<T> entities, Action<DbConnection, T> action)
        {
            using (connection)
            {
                connection.Open();
                var transaction = connection.BeginTransaction();
                try
                {
                    foreach (var entity in entities)
                    {
                        action(connection, entity);
                    }
                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    connection.Close();
                }
            }
        }
        #endregion

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