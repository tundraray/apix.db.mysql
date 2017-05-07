using System;
using MySql.Data.MySqlClient;

namespace Apix.Db.Mysql
{
    public interface IDbConnection : IDisposable
    {
        MySqlConnection Connection { get; }
    }

    public class DbConnection : IDbConnection
    {
        public MySqlConnection Connection { get; }
        public DbConnection(string connectionString)
        {
            Connection = new MySqlConnection(connectionString);
        }
    
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (Connection != null)
                {
                    MySqlConnection.ClearPool(Connection);
                    Connection.Dispose();
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~DbConnection()
        {
            Dispose(false);
        }
    }
}