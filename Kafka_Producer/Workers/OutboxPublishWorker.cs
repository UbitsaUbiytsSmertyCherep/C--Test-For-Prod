using Confluent.Kafka;
using Kafka_Worker_Producer.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Kafka_Worker_Producer.Workers;

public class OutboxPublishWorker: BackgroundService
{
    private readonly ILogger<OutboxPublishWorker> _logger;
    private readonly IProducer<string, byte[]> _producer;
    private readonly IServiceProvider _serviceProvider;


    public OutboxPublishWorker(
        ILogger<OutboxPublishWorker> logger,
        IProducer<string, byte[]> producer,
        IServiceProvider serviceProvider)
    {
        _logger = logger;
        _producer = producer;
        _serviceProvider = serviceProvider;
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        _logger.LogInformation("[OUTBOX_PUBLISHER] Outbox publish worker starting ...");
       
        
        
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using var scope = _serviceProvider.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);

                try
                {
                    var batch = await db.OutboxMessages
                        .Where(a => a.ProcessedAtUtc==null && a.Error==null)
                        .OrderBy(a => a.CreatedAtUtc)
                        .Take(10)
                        .ToListAsync<OutboxMessage>(stoppingToken);
                    if (batch.Count != 0)
                    {
                        foreach (var item in batch)
                        {
                            
                            try
                            {
                                var res = await _producer.ProduceAsync(
                                    item.Topic,
                                    new Message<string, byte[]>()
                                {
                                    Key = item.Key,
                                    Value = item.Payload
                                }, stoppingToken);
                                
                                
                                item.ProcessedAtUtc = DateTime.UtcNow;
                                _logger.LogInformation("[OUTBOX_PUBLISHER] Processed item with id: {i}",item.Id);
                                
                            }
                            catch (Exception e)
                            {
                                item.Error = e.Message;
                                _logger.LogError(e,
                                    "[OUTBOX_PUBLISHER] Error while processing outbox message: {id}",
                                    item.Id);
                                break;
                            }
                            
                            
                            
                        }

                        await db.SaveChangesAsync(stoppingToken);
                        await transaction.CommitAsync(stoppingToken);
                    }

                    await Task.Delay(1000, stoppingToken);
                }
                catch (Exception e) when (e is NpgsqlException or DbUpdateException)
                {
                    _logger.LogError(e, "[OUTBOX_PUBLISHER] Error during SQL tasks; ");
                    await transaction.RollbackAsync(stoppingToken);
                }
            }
        }
        catch (OperationCanceledException e)
        {
            _logger.LogInformation("[OUTBOX_PUBLISHER] Outbox publish worker stopping ...]");

        }

    }

    public override void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10)); 
        _producer.Dispose();
        base.Dispose();
        
    }
}