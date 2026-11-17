using System.Data.Common;
using System.Text;
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
    private readonly IProducer<byte[],byte[]> _dlqProducer;


    public AlertConsumerWorker(ILogger<AlertConsumerWorker> logger,
        ConsumerConfig config,
        IServiceProvider serviceProvider,
        IProducer<byte[], byte[]> dlqProducer)
    {
        _logger = logger;
        _config = config;
        _serviceProvider = serviceProvider;
        _dlqProducer = dlqProducer;
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
                        var headers = res.Message.Headers ?? new Headers();
                        headers.Add("from-topic", Encoding.UTF8.GetBytes(_topicName));
                        headers.Add("dlq_offset", Encoding.UTF8.GetBytes(res.Offset.Value.ToString()));
                        headers.Add("dlq_error", Encoding.UTF8.GetBytes(e.Message));
                        
                        await _dlqProducer.ProduceAsync("alert-topic-dlq", new Message<byte[], byte[]>()
                        {
                            Key = res.Message.Key != null ? Encoding.UTF8.GetBytes(res.Message.Key) : Array.Empty<byte>(),
                            Value = res.Message.Value,
                            Headers = headers
                        },stoppingToken);
                        _logger.LogInformation("[CONSUMER] Bad message have been sent to DLQ Topic");
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
