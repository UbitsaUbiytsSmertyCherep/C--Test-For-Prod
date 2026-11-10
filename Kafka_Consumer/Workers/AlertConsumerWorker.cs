using Confluent.Kafka;
using Google.Protobuf;
using Kafka_Contracts.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka_with_Worker.Workers;

public class AlertConsumerWorker: BackgroundService
{
    private readonly ILogger<AlertConsumerWorker> _logger;
    private readonly ConsumerConfig _config;
    private readonly string _topicName = "alert-topic";


    public AlertConsumerWorker(ILogger<AlertConsumerWorker> logger, ConsumerConfig config)
    {
        _logger = logger;
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        using var consumer = new ConsumerBuilder<string, byte[]>(_config).Build();
        consumer.Subscribe(_topicName);
        
        _logger.LogInformation("Subscribed to {Topic}", _topicName);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {

                try
                {
                    var res = consumer.Consume(stoppingToken);
                    try
                    {
                        var req = AlertReq.Parser.ParseFrom(res.Message.Value);
                        _logger.LogInformation("Recieved: Key:{key}, Value: {value}, Partition:{part}", res.Message.Key,
                            req, res.TopicPartition);
                        await Task.Delay(1000, stoppingToken);
                        consumer.Commit(res);
                        _logger.LogInformation("Data Commited | Offset: {Offset}", res.Offset);
                    }
                    catch ( InvalidProtocolBufferException e)
                    {
                        _logger.LogError(e, "Invalid Protocol Buffer");
                        consumer.Commit(res);
                    }


                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Error occured: {Message}\n\tReason:{Reason}", e.Message, e.Error.Reason);
                    consumer.Commit(new ConsumeResult<string, byte[]>()
                    {

                    });
                }

            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer is stopping...");
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("Consumer closed softly.");
        }


    }
    
}
