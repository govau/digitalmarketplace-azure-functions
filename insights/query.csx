#r "System.Data.SqlClient"

using System.Data.SqlClient;

internal class Query {
    protected readonly string _connectionString;

    public Query(string connectionString) {
        _connectionString = connectionString;
    }

    protected async Task<List<T>> ExecuteQueryAsync<T>(string sql, Func<SqlDataReader, T> processRowFunc) where T : class {
        var result = new List<T>();
        using (var connection = new SqlConnection(_connectionString))
        using (var cmd = new SqlCommand(sql, connection)) {
            connection.Open();

            using (var reader = await cmd.ExecuteReaderAsync()) {
                while (reader.Read()) {
                    result.Add(processRowFunc(reader));
                }
            }
        }
        return result;
    }

}