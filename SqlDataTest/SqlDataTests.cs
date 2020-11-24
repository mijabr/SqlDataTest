using Dapper;
using NUnit.Framework;
using System;
using System.Data;
using System.Transactions;

namespace SqlDataTest
{
    public class SqlDataTests
    {
        public enum SqlLibraryType
        {
            SqlData,
            SqlConnector
        }

        private class TestState : IDisposable
        {
            public IDbConnection Connection { get; }
            public TransactionScope TransactionScope { get; }

            public TestState(SqlLibraryType library)
            {
                Connection = library == SqlLibraryType.SqlData ? new MySql.Data.MySqlClient.MySqlConnection(ConnectionString) : new MySqlConnector.MySqlConnection(ConnectionString);
                TransactionScope = new TransactionScope();
            }

            protected string ConnectionString => "Database=databasename;Data Source=localhost;User Id=username;Password=password";

            private bool disposed = false;

            public void Dispose() => Dispose(true);

            protected virtual void Dispose(bool disposing)
            {
                if (disposed) {
                    return;
                }

                if (disposing) {
                    TransactionScope.Dispose();
                    Connection.Dispose();
                }

                disposed = true;
            }
        }

        private class TestEntity
        {
            public Guid Id { get; set; }
            public Guid? OtherId { get; set; }
        }

        [Test]
        public void Query_ShouldNotHaltAndCatchFire_GivenSqlDataLibraryAndNullValues()
        {
            using(var state = new TestState(SqlLibraryType.SqlData)) {
                state.Connection.Execute("CREATE TABLE IF NOT EXISTS testtable (Id char(36),OtherId char(36))");
                state.Connection.Execute("INSERT INTO testtable (Id,OtherId) VALUES (@Id,@OtherId)", new TestEntity { Id = Guid.NewGuid(), OtherId = null });

                state.Connection.Query<TestEntity>("SELECT * from testtable");
                // Result - Fail
                // System.Data.DataException : Error parsing column 1(OtherId = 24f57342 - 1d02 - 4328 - a31e - 9b9fe533d73d - Object)
                // --- > System.ArgumentOutOfRangeException : Non - negative number required. (Parameter 'count')
                // at Dapper.SqlMapper.ThrowDataException(Exception ex, Int32 index, IDataReader reader, Object value) in / _ / Dapper / SqlMapper.cs:line 3665
                // at Deserialize02848912-6ca6 - 4cdc - 91b0 - 6c4ccbeb89e2(IDataReader)
                // at Dapper.SqlMapper.QueryImpl[T](IDbConnection cnn, CommandDefinition command, Type effectiveType) + MoveNext() in / _ / Dapper / SqlMapper.cs:line 1102
                // at System.Collections.Generic.List`1..ctor(IEnumerable`1 collection)
                // at System.Linq.Enumerable.ToList[TSource](IEnumerable`1 source)
                // at Dapper.SqlMapper.Query[T](IDbConnection cnn, String sql, Object param, IDbTransaction transaction, Boolean buffered, Nullable`1 commandTimeout, Nullable`1 commandType) in / _ / Dapper / SqlMapper.cs:line 725
                // at SqlDataTest.SqlDataTests.Query_ShouldNotHaltAndCatchFire_GivenSqlDataLibraryAndNullValues() in C:\Sandbox\MB\sql - data\SqlDataTest\SqlDataTest\SqlDataTests.cs:line 61
                //   --ArgumentOutOfRangeException
                //   at System.Text.EncodingNLS.GetString(Byte[] bytes, Int32 index, Int32 count)
                // at MySql.Data.MySqlClient.NativeDriver.ReadColumnValue(Int32 index, MySqlField field, IMySqlValue valObject)
                // at MySql.Data.MySqlClient.ResultSet.get_Item(Int32 index)
                // at MySql.Data.MySqlClient.MySqlDataReader.GetFieldValue(Int32 index, Boolean checkNull)
                // at MySql.Data.MySqlClient.MySqlDataReader.GetValue(Int32 i)
                // at MySql.Data.MySqlClient.MySqlDataReader.get_Item(Int32 i)
                // at Deserialize02848912-6ca6 - 4cdc - 91b0 - 6c4ccbeb89e2(IDataReader)
            }
        }

        [Test]
        public void Query_ShouldNotHaltAndCatchFire_GivenSqlConnectorLibraryAndNullValues()
        {
            using (var state = new TestState(SqlLibraryType.SqlConnector)) {
                //state.Connection.Execute("CREATE TABLE IF NOT EXISTS testtable (Id char(36),OtherId char(36))");
                state.Connection.Execute("INSERT INTO testtable (Id,OtherId) VALUES (@Id,@OtherId)", new TestEntity { Id = Guid.NewGuid(), OtherId = null });

                state.Connection.Query<TestEntity>("SELECT * from testtable");
                // Result - success
            }
        }
    }
}
