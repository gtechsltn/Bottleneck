# Bottleneck

https://docs.google.com/document/d/1SFDPkiXD7TWIpF1bKiAWFetz5XkQBbZphsRsUa6oO_M

## TimeLogger.cs
```
using System;
using System.Configuration;
using System.Diagnostics;

public class TimeLogger : IDisposable
{
    private readonly Stopwatch _stopwatch;
    private readonly string _operation;
    private readonly bool _isEnabled;

    public TimeLogger(string operation)
    {
        _operation = operation;
        _stopwatch = Stopwatch.StartNew();
        _isEnabled = bool.TryParse(ConfigurationManager.AppSettings["EnableTimeLogger"], out var enabled) && enabled;

        if (_isEnabled)
        {
            Console.WriteLine($"[TimeLogger] Started: {_operation}");
        }
    }

    public void Dispose()
    {
        _stopwatch.Stop();

        if (_isEnabled)
        {
            Console.WriteLine($"[TimeLogger] Finished: {_operation} in {_stopwatch.Elapsed.TotalMilliseconds:F2} ms");
        }
    }
}
```

## Nuget
```
Install-Package Dapper
Install-Package Polly
Install-Package log4net
Install-Package CsvHelper
Install-Package Newtonsoft.Json
```

## log4net.config
```
<log4net>
  <root>
    <level value="INFO" />
    <appender-ref ref="RollingFileAppender" />
  </root>

  <appender name="RollingFileAppender" type="log4net.Appender.RollingFileAppender">
    <file value="Logs\\UserReaderWorker.log" />
    <appendToFile value="true" />
    <rollingStyle value="Size" />
    <maxSizeRollBackups value="5" />
    <maximumFileSize value="5MB" />
    <staticLogFileName value="true" />
    <layout type="log4net.Layout.PatternLayout">
      <conversionPattern value="%date [%thread] %-5level %logger - %message%newline" />
    </layout>
  </appender>
</log4net>
```

## T-SQL Code
```
CREATE TABLE Users (
    Id UNIQUEIDENTIFIER PRIMARY KEY,
    Name NVARCHAR(100),
    Email NVARCHAR(100),
    CreatedDate DATETIME,
    IsActive BIT
);

CREATE TYPE dbo.UserType AS TABLE
(
    Id UNIQUEIDENTIFIER PRIMARY KEY,
    Name NVARCHAR(100),
    Email NVARCHAR(100),
    IsActive BIT
);

CREATE PROCEDURE dbo.UpdateUsersBulk
    @Users dbo.UserType READONLY
AS
BEGIN
    UPDATE u
    SET
        u.Name = t.Name,
        u.Email = t.Email,
        u.IsActive = t.IsActive
    FROM Users u
    INNER JOIN @Users t ON u.Id = t.Id;
END
```

## Program.cs
```
using log4net;
using log4net.Config;
using System.IO;

static async Task Main(string[] args)
{
    var logRepository = LogManager.GetRepository(System.Reflection.Assembly.GetEntryAssembly());
    XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));

    var connectionString = "Server=(local);Database=UserDb;Integrated Security=true;";
    using var worker = new UserReaderWorker(connectionString);

    Console.WriteLine("UserReaderWorker running. Press Enter to exit.");
    await Task.Run(() => Console.ReadLine());
}
```

## UserReaderWorker.cs (v1)
```
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using log4net;
using Polly;
using Polly.Retry;

public class UserReaderWorker : IDisposable
{
    private readonly Timer _timer;
    private readonly string _connectionString;
    private readonly ILog _logger;
    private readonly AsyncRetryPolicy _retryPolicy;
    private bool _isRunning = false;
    private bool _disposed = false;

    public UserReaderWorker(string connectionString)
    {
        _connectionString = connectionString;
        _logger = LogManager.GetLogger(typeof(UserReaderWorker));

        _retryPolicy = Policy
            .Handle<SqlException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(2),
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    _logger.Warn($"Retry {retryCount} due to: {exception.Message}");
                });

        _timer = new Timer(OnTimerTickAsync, null, TimeSpan.Zero, TimeSpan.FromSeconds(10));
    }

    private async void OnTimerTickAsync(object state)
    {
        if (_isRunning) return;
        _isRunning = true;

        try
        {
            await _retryPolicy.ExecuteAsync(ReadUsersAsync);
        }
        catch (Exception ex)
        {
            _logger.Error("Unhandled exception in timer tick", ex);
        }
        finally
        {
            _isRunning = false;
        }
    }

    private async Task ReadUsersAsync()
    {
        _logger.Info("Reading users from database...");

        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();

            var users = await connection.QueryAsync<User>(
                "SELECT TOP 100 * FROM Users WHERE IsActive = 1 ORDER BY CreatedDate DESC");

            foreach (var user in users)
            {
                await HandleUserAsync(user);
            }
        }

        _logger.Info("Finished reading users.");

        // Simulate insert/update
        var newUsers = GenerateFakeUsers(1000);
        await BulkInsertUsersAsync(newUsers);

        var updatedUsers = newUsers.Select(u =>
        {
            u.Name += " Updated";
            return u;
        });

        await BulkUpdateUsersAsync(updatedUsers);
    }

    private Task HandleUserAsync(User user)
    {
        _logger.Info($"Processing user {user.Id}: {user.Name}");
        return Task.CompletedTask;
    }

    private async Task BulkInsertUsersAsync(IEnumerable<User> users)
    {
        var dataTable = new DataTable();
        dataTable.Columns.Add("Id", typeof(Guid));
        dataTable.Columns.Add("Name", typeof(string));
        dataTable.Columns.Add("Email", typeof(string));
        dataTable.Columns.Add("CreatedDate", typeof(DateTime));
        dataTable.Columns.Add("IsActive", typeof(bool));

        foreach (var user in users)
        {
            dataTable.Rows.Add(user.Id, user.Name, user.Email, user.CreatedDate, user.IsActive);
        }

        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "Users";
                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }

        _logger.Info($"Bulk inserted {dataTable.Rows.Count} users.");
    }

    private async Task BulkUpdateUsersAsync(IEnumerable<User> users)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(Guid));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Email", typeof(string));
        table.Columns.Add("IsActive", typeof(bool));

        foreach (var user in users)
        {
            table.Rows.Add(user.Id, user.Name, user.Email, user.IsActive);
        }

        var parameter = new SqlParameter("@Users", SqlDbType.Structured)
        {
            TypeName = "dbo.UserType",
            Value = table
        };

        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await connection.ExecuteAsync("dbo.UpdateUsersBulk", new { Users = parameter }, commandType: CommandType.StoredProcedure);
        }

        _logger.Info($"Bulk updated {table.Rows.Count} users.");
    }

    private List<User> GenerateFakeUsers(int count)
    {
        var users = new List<User>();
        for (int i = 0; i < count; i++)
        {
            users.Add(new User
            {
                Id = Guid.NewGuid(),
                Name = $"User {i}",
                Email = $"user{i}@example.com",
                CreatedDate = DateTime.UtcNow,
                IsActive = true
            });
        }
        return users;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _timer?.Dispose();
            _disposed = true;
        }
    }
}
```

## UserReaderWorker.cs (v2)
```
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Dapper;
using log4net;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;

public class UserReaderWorker : IDisposable
{
    private readonly Timer _timer;
    private readonly string _connectionString;
    private readonly ILog _logger;
    private readonly AsyncRetryPolicy _retryPolicy;
    private bool _isRunning = false;
    private bool _disposed = false;

    public UserReaderWorker(string connectionString)
    {
        _connectionString = connectionString;
        _logger = LogManager.GetLogger(typeof(UserReaderWorker));

        _retryPolicy = Policy
            .Handle<SqlException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(2),
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    _logger.Warn($"Retry {retryCount} due to: {exception.Message}");
                });

        _timer = new Timer(OnTimerTickAsync, null, TimeSpan.Zero, TimeSpan.FromSeconds(10));
    }

    private async void OnTimerTickAsync(object state)
    {
        if (_isRunning) return;
        _isRunning = true;

        try
        {
            await _retryPolicy.ExecuteAsync(ReadUsersAsync);
        }
        catch (Exception ex)
        {
            _logger.Error("Unhandled exception in timer tick", ex);
        }
        finally
        {
            _isRunning = false;
        }
    }

    private async Task ReadUsersAsync()
    {
        _logger.Info("Reading users from database...");

        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();

            var users = await connection.QueryAsync<User>(
                "SELECT TOP 100 * FROM Users WHERE IsActive = 1 ORDER BY CreatedDate DESC");

            foreach (var user in users)
            {
                await HandleUserAsync(user);
            }
        }

        _logger.Info("Finished reading users.");

        var newUsers = GenerateFakeUsers(1000);
        await BulkInsertUsersAsync(newUsers);

        var updatedUsers = newUsers.Select(u =>
        {
            u.Name += " Updated";
            return u;
        });

        await BulkUpdateUsersAsync(updatedUsers);

        await ExportUsersToCsvAsync(updatedUsers, "Users_Export.csv");
        await ExportUsersToJsonAsync(updatedUsers, "Users_Export.json");
    }

    private Task HandleUserAsync(User user)
    {
        _logger.Info($"Processing user {user.Id}: {user.Name}");
        return Task.CompletedTask;
    }

    private async Task BulkInsertUsersAsync(IEnumerable<User> users)
    {
        var dataTable = new DataTable();
        dataTable.Columns.Add("Id", typeof(Guid));
        dataTable.Columns.Add("Name", typeof(string));
        dataTable.Columns.Add("Email", typeof(string));
        dataTable.Columns.Add("CreatedDate", typeof(DateTime));
        dataTable.Columns.Add("IsActive", typeof(bool));

        foreach (var user in users)
        {
            dataTable.Rows.Add(user.Id, user.Name, user.Email, user.CreatedDate, user.IsActive);
        }

        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "Users";
                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }

        _logger.Info($"Bulk inserted {dataTable.Rows.Count} users.");
    }

    private async Task BulkUpdateUsersAsync(IEnumerable<User> users)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(Guid));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Email", typeof(string));
        table.Columns.Add("IsActive", typeof(bool));

        foreach (var user in users)
        {
            table.Rows.Add(user.Id, user.Name, user.Email, user.IsActive);
        }

        var parameter = new SqlParameter("@Users", SqlDbType.Structured)
        {
            TypeName = "dbo.UserType",
            Value = table
        };

        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await connection.ExecuteAsync("dbo.UpdateUsersBulk", new { Users = parameter }, commandType: CommandType.StoredProcedure);
        }

        _logger.Info($"Bulk updated {table.Rows.Count} users.");
    }

    private async Task ExportUsersToCsvAsync(IEnumerable<User> users, string filePath)
    {
        using var writer = new StreamWriter(filePath);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

        await csv.WriteRecordsAsync(users);
        _logger.Info($"Exported {users.Count()} users to CSV: {filePath}");
    }

    private async Task ExportUsersToJsonAsync(IEnumerable<User> users, string filePath)
    {
        var json = JsonConvert.SerializeObject(users, Formatting.Indented);
        await File.WriteAllTextAsync(filePath, json);

        _logger.Info($"Exported {users.Count()} users to JSON: {filePath}");
    }

    private List<User> GenerateFakeUsers(int count)
    {
        var users = new List<User>();
        for (int i = 0; i < count; i++)
        {
            users.Add(new User
            {
                Id = Guid.NewGuid(),
                Name = $"User {i}",
                Email = $"user{i}@example.com",
                CreatedDate = DateTime.UtcNow,
                IsActive = true
            });
        }
        return users;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _timer?.Dispose();
            _disposed = true;
        }
    }
}

public class User
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedDate { get; set; }
    public bool IsActive { get; set; }
}
```
