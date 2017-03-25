﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// Base SQL entity repository
    /// </summary>
    public abstract class MysqlDataRepositoryBase<T>
        where T : new()
    {
        #region Constructors

        protected MysqlDataRepositoryBase(string conn) : this(new MySqlConnection(conn)) { }

        protected MysqlDataRepositoryBase(MySqlConnection conn)
        {
            Connection = conn;
        }

        #endregion

        #region Properties

        /// <summary>
        /// Main SQL connection
        /// </summary>
        protected MySqlConnection Connection { get; }

        #endregion

        #region Get method 

        public Task<T> GetByQueryAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = SqlGenerator.SelectQuery(predicate);
            return Connection.ExecuteQueryFirstOrDefaultAsync<T>(result.Sql, result.Param, cancellationToken: cancellationToken);
        }

        #endregion

        #region List method 

        /// <summary>
        /// List all entities
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of stored entities</returns>
        public Task<IEnumerable<T>> ListAllAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.ExecuteQueryAsync<T>(SqlGenerator.SelectAllQuery<T>(), cancellationToken: cancellationToken);
        }


        /// <summary>
        /// List all entities with pagination
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of stored entities</returns>
        public Task<IEnumerable<T>> ListAllAsync(long limit, ulong offset, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.ExecuteQueryAsync<T>(SqlGenerator.SelectAllQuery<T>(limit, offset), cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Find entities by expression predicate
        /// </summary>
        /// <param name="predicate">Expression predicate</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of stored entities</returns>
        public Task<IEnumerable<T>> ListByQueryAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.Argument.NotNull(predicate, nameof(predicate));
            var result = SqlGenerator.SelectQuery(predicate);
            return Connection.ExecuteQueryAsync<T>(result.Sql, result.Param, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Find entities by expression predicate with pagination
        /// </summary>
        /// <param name="predicate">Expression predicate</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of stored entities</returns>
        public Task<IEnumerable<T>> ListByQueryAsync(Expression<Func<T, bool>> predicate, long limit, ulong offset, CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.Argument.NotNull(predicate, nameof(predicate));
            var result = SqlGenerator.SelectQuery(predicate, offset,limit);
            return Connection.ExecuteQueryAsync<T>(result.Sql, result.Param, cancellationToken: cancellationToken);
        }

        #endregion

        #region Create method 

        /// <summary>
        /// Create entity
        /// </summary>
        /// <param name="entity">Entity</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public Task CreateAsync(T entity, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.ExecuteNonQueryAsync(SqlGenerator.InsertQuery<T>(), entity, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Create entity
        /// </summary>
        /// <param name="entity">Entity</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public Task<T> CreateAsyncWithResult(T entity, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.ExecuteQueryFirstOrDefaultAsync<T>(SqlGenerator.InsertQuery<T>(), entity, cancellationToken: cancellationToken);
        }

        #endregion

        #region Update method 

        /// <summary>
        /// Update entity
        /// </summary>
        /// <param name="entity">Entity</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public Task UpdateAsync(T entity, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.ExecuteNonQueryAsync(SqlGenerator.UpdateQuery<T>(), entity, cancellationToken: cancellationToken);
        }

        #endregion

        #region Other methods

        public Task ExecuteAsync(string sql, object predicate, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.ExecuteTransactionNonQueryAsync(sql, predicate, cancellationToken: cancellationToken);
        }

        #endregion

    }

}