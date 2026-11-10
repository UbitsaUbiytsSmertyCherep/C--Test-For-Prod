using Confluent.Kafka;
using Google.Protobuf;
using Kafka_Contracts.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka_Worker_Producer.Workers;

public class AlertProducer: BackgroundService
{
    private readonly ILogger<AlertProducer> _logger;
    private readonly ProducerConfig _config;
    private readonly string _topic = "alert-topic";

    public AlertProducer(ILogger<AlertProducer> logger, ProducerConfig config)
    {
        _logger = logger;
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        
        using var producer = new ProducerBuilder<string, byte[] > (_config).Build();
        _logger.LogInformation("Producer started");
        
        try
        {
            for (int i = 0; i < 3; i++)
            {
                var message = new AlertReq()
                    {
                        Id = i,
                        Data = {},
                        Author = $"User_{i}"
                    }
                    .ToByteArray();
                var res = await producer.ProduceAsync(_topic, new Message<string, byte[]>()
                {
                    Key = $"User_{i}",
                    Value = message,
                }, cancellationToken:stoppingToken);
                _logger.LogInformation("Sent: Key:{Key} | Value:{Value}", res.Message.Key, res.Message.Value);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Producer stopped");
        }
    }
}