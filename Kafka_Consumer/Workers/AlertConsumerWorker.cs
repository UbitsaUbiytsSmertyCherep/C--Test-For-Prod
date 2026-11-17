using System.Data.Common;
using Confluent.Kafka;
using Google.Protobuf;
using Kafka_Contracts.Contracts;
using Kafka_with_Worker.Domain;
using Kafka_with_Worker.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Kafka_with_Worker.Workers;

public class AlertConsumerWorker: BackgroundService
{
    private readonly ILogger<AlertConsumerWorker> _logger;
    private readonly ConsumerConfig _config;
    private readonly string _topicName = "alert-topic";
    private readonly IServiceProvider _serviceProvider;


    public AlertConsumerWorker(ILogger<AlertConsumerWorker> logger, ConsumerConfig config, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _config = config;
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        using var consumer = new ConsumerBuilder<string, byte[]>(_config).Build();
        consumer.Subscribe(_topicName);
        
        _logger.LogInformation("[CONSUMER] Subscribed to {Topic}", _topicName);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {

                try
                {
                    var res = consumer.Consume(stoppingToken);
                    try
                    {
                        var req = AlertEvent.Parser.ParseFrom(res.Message.Value);
                        _logger.LogInformation("[CONSUMER] Recieved: Key:{key}, Value: {value}, Partition:{part}", res.Message.Key,
                            req, res.TopicPartition);
                        using var scope = _serviceProvider.CreateScope();
                        var db = scope.ServiceProvider.GetRequiredService<ConsumerDbContext>();
                        await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);
                        try
                        {
                            db.InboxRecords.Add(new InboxRecord
                            {
                                Id = Guid.Parse(req.AlertId),
                                ReceivedAt = DateTime.UtcNow,
                            });
                            

                            db.AlertsRecieved.Add(new AlertRecieved
                            {
                                Id = Guid.Parse(req.AlertId),
                                Title = req.Title,
                                Severity = req.Severity,
                                CreatedAtUtc = DateTime.UtcNow,
                            });
                            await db.SaveChangesAsync(stoppingToken);
                            
                            await transaction.CommitAsync(stoppingToken);
                            consumer.Commit(res);
                            _logger.LogInformation("[CONSUMER] Data Saved Correctly | Id: {Id}", req.AlertId);
                            
                        }
                        catch (Exception e) when (e is NpgsqlException or DbUpdateException)
                        {
                            _logger.LogError(e, "[CONSUMER] Error occured during the work with db: {Message}", e.Message);
                            await transaction.RollbackAsync(stoppingToken);
                        }
                        
                        
                        await Task.Delay(1000, stoppingToken);
                        
                        
                        
                        _logger.LogInformation("[CONSUMER] Data Commited | Offset: {Offset}", res.Offset);
                    }
                    catch ( InvalidProtocolBufferException e)
                    {
                        _logger.LogError(e, "[CONSUMER] Invalid Protocol Buffer");
                        consumer.Commit(res);
                    }


                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "[CONSUMER] Error occured: {Message}\n\tReason:{Reason}", e.Message, e.Error.Reason);
                }

            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[CONSUMER] Consumer is stopping...");
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("[CONSUMER] Consumer closed softly.");
        }


    }
    
}
