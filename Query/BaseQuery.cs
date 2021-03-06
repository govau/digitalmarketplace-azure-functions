using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Dta.Marketplace.Azure.Functions.Query {
    internal class BaseQuery {
        protected readonly string _connectionString;

        protected BaseQuery(string connectionString) {
            _connectionString = connectionString;
        }

        protected async Task<List<T>> ExecuteReaderAsync<T>(string sql, Func<SqlDataReader, T> processRowFunc) where T : class {
            var result = new List<T>();
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection)) {
                connection.Open();
                cmd.CommandTimeout = 90;

                using (var reader = await cmd.ExecuteReaderAsync()) {
                    while (reader.Read()) {
                        result.Add(processRowFunc(reader));
                    }
                }
            }
            return result;
        }

        protected async Task<int> ExecuteNonQueryAsync(Func<SqlConnection, SqlCommand> createCommand) {
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = createCommand(connection)) {
                connection.Open();
                cmd.CommandTimeout = 90;
                return await cmd.ExecuteNonQueryAsync();
            }
        }

        protected T GetFieldValueOrNull<T>(SqlDataReader reader, int i) {
            if (reader.IsDBNull(i)) {
                return default(T);
            }
            return reader.GetFieldValue<T>(i);
        }

        protected string GetFinancialYearString(DateTime now) {
            if (now.Month > 6) {
                var thisYear = now.ToString("yyyy");
                var nextYear = now.AddYears(1).ToString("yy");
                return $"{thisYear}-{nextYear}";
            } else {
                var lastYear = now.AddYears(-1).ToString("yyyy");
                var thisYear = now.ToString("yy");
                return $"{lastYear}-{thisYear}";
            }
        }
    }
}