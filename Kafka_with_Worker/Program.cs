using Confluent.Kafka;
using Kafka_with_Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton(new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
});
builder.Services.AddHostedService<AlertConsumerWorker>();
var app = builder.Build();
await app.RunAsync();