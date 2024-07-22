using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Npgsql;

class Program
{
    static async Task Main(string[] args)
    {
        string connectionString = "Host=localhost:5432;Database=Tejas;Username=postgres;Password=Tejas@2012";

        // Sample data to insert
        List<User> users = new List<User>();
        for (int i = 1; i <= 10000000; i++)
        {
            users.Add(new User { Id = i, Name = $"User{i}" });
        }

        // Split data into chunks for multithreading
        int chunkSize = users.Count / Environment.ProcessorCount;
        List<Task> tasks = new List<Task>();
        Console.WriteLine(DateTime.Now.ToString());

        for (int i = 0; i < users.Count; i += chunkSize)
        {
            var chunk = users.GetRange(i, Math.Min(chunkSize, users.Count - i));
            tasks.Add(Task.Run(() => BulkInsert(connectionString, chunk)));
        }

        await Task.WhenAll(tasks);
        Console.WriteLine(DateTime.Now.ToString());
    }

    static async Task BulkInsert(string connectionString, List<User> users)
    {
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            using (var writer = connection.BeginBinaryImport("COPY user_test (id, name) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var row in users)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(row.Id, NpgsqlTypes.NpgsqlDbType.Integer);
                    await writer.WriteAsync(row.Name, NpgsqlTypes.NpgsqlDbType.Text);
                }

                await writer.CompleteAsync();
            }
        }
    }
}
class User
{
    public int Id { get; set; }
    public string Name { get; set; }
}

