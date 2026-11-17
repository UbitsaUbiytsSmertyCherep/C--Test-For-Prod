using Google.Protobuf;
using Kafka_Contracts.Contracts;
using Kafka_Worker_Producer.Domain;
using Kafka_Worker_Producer.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Kafka_Worker_Producer.Workers;
using Microsoft.Extensions.Hosting;
public class AlertGeneratorWorker: BackgroundService
{
    private readonly ILogger<AlertGeneratorWorker> _logger;
    private readonly IServiceProvider _serviceProvider;

    public AlertGeneratorWorker(ILogger<AlertGeneratorWorker> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        _logger.LogInformation("[GENERATOR] AlertGeneratorWorker started");
        var counter = 1;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                counter++;
                await Task.Delay(1000, stoppingToken);
                using var scope = _serviceProvider.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);
                try
                {
                    var alert = new Alert()
                    {
                        Id = Guid.NewGuid(),
                        CreatedAtUtc = DateTime.UtcNow,
                        Title = $"Alert Number: {counter}",
                        Severity = new Random().Next(1, 5)
                    };
                    db.Alerts.Add(alert);
                    var protoAlert = new AlertEvent()
                    {
                        AlertId = alert.Id.ToString(),
                        Title = alert.Title,
                        Severity = alert.Severity,
                        TimestampUtc = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                    };
                    var outbox = new OutboxMessage()
                    {
                        Id = Guid.NewGuid(),
                        CreatedAtUtc = DateTime.UtcNow,
                        Topic = "alert-topic",
                        Payload = protoAlert.ToByteArray(),
                        Key = protoAlert.AlertId
                    };
                    db.OutboxMessages.Add(outbox);
                    await db.SaveChangesAsync(stoppingToken);
                    await transaction.CommitAsync(stoppingToken);

                    _logger.LogInformation("[GENERATOR] Saved correctly Alert with Title: {title}", alert.Title);

                }
                catch (Exception ex) when (ex is NpgsqlException or DbUpdateException)
                {
                    await transaction.RollbackAsync(stoppingToken);
                    _logger.LogError("[GENERATOR] Something caused exception During DB operations: {ex}", ex.Message);
                }

            }
        }
        catch (OperationCanceledException e)
        {
            _logger.LogInformation("[GENERATOR] AlertGeneratorWorker stopped");
            
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "[GENERATOR] Critical Error in Background Service");
            throw;
        }
    }
}