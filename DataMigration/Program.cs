using System;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using Serilog;

class Program
{
    static void Main(string[] args)
    {
        string logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        if (!Directory.Exists(logDirectory))
        {
            Directory.CreateDirectory(logDirectory);
        }

        Log.Logger = new LoggerConfiguration()
        .WriteTo.File($"Logs/log{DateTime.Now.ToString("yyyyMMdd_HHmmss")}.txt")
        .CreateLogger();

        // Set up the logger
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            //builder.AddConsole(); // Add console logging
            builder.AddSerilog();
        });

        ILogger<Program> logger = loggerFactory.CreateLogger<Program>();

        // Build configuration from appsettings.json
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        string sqlServerConnectionString = configuration.GetConnectionString("SqlServer");
        string pgSqlConnectionString = configuration.GetConnectionString("PgSql");

        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        using (var pgConnection = new NpgsqlConnection(pgSqlConnectionString))
        {
            sqlConnection.Open();
            pgConnection.Open();

            // Query to fetch data from SQL Server
            var sqlQuery = @"SELECT 
                                farmer_id, 
                                field_id, 
                                unique_id, 
                                polygon_status, 
                                qc_status, 
                                qc_userid, 
                                polygon_remark, 
                                user_type, 
                                area, 
                                activity_timestamp, 
                                isActive, 
                                coordinates
                             FROM tbl_PolygonQC_Audit";

            // Query to insert data into PostgreSQL
            var pgInsertQuery = @"INSERT INTO public.tblfk_polygon_qc_audit (
                                    farmer_id, field_id, field_no, unique_id, coordinates, area, polygon_status, polygon_remark, 
                                    qc_status, user_type, created_at, created_by, is_active
                                  ) VALUES (
                                    @FarmerId, @FieldId, @FieldNo, @UniqueId, @Coordinates, @Area, @PolygonStatus, @PolygonRemark, 
                                    @QcStatus, @UserType, @CreatedAt, @CreatedBy, @IsActive
                                  )";

            // Fetch data from SQL Server
            var data = sqlConnection.Query(sqlQuery);

            foreach (var row in data)
            {
                try
                {
                    // Transform data if necessary
                    var parameters = new
                    {
                        FarmerId = row.farmer_id,
                        FieldId = (Guid?)null,
                        FieldNo = row.field_id ?? 0,
                        UniqueId = row.unique_id,
                        Coordinates = row.coordinates,
                        Area = row.area,
                        PolygonStatus = row.polygon_status,
                        PolygonRemark = row.polygon_remark,
                        QcStatus = row.qc_status,
                        UserType = row.user_type,
                        CreatedAt = row.activity_timestamp,
                        CreatedBy = row.qc_userid,
                        IsActive = row.isActive == "Y"
                    };

                    // Insert into PostgreSQL
                    pgConnection.Execute(pgInsertQuery, parameters);

                    // Log the successful insert
                    logger.LogInformation($"Successfully inserted record with UniqueId: {row.unique_id}"+
                        $"\nFarmerId: {row.farmer_id}" +
                        $"\nFieldNo: {row.field_id ?? 0}" +
                        $"\nCoordinates: {row.coordinates}");

                }
                catch (Exception ex)
                {
                    // Log the error with the details of the failed record
                    logger.LogError($"Error inserting record with \nUniqueId: {row.unique_id}. " +
                        $"\nFarmerId: {row.farmer_id}" +
                        $"\nFieldNo: {row.field_id ?? 0}" +
                        $"\nCoordinates: {row.coordinates}" +
                        $"\nError: {ex.Message}");
                }
            }

            Console.WriteLine("Data migration completed successfully.");
        }
    }
}
