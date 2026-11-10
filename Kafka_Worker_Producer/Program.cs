// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Kafka_Worker_Producer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;



var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton(new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    AllowAutoCreateTopics = false
});

builder.Services.AddHostedService<AlertProducer>();


var app = builder.Build();
await app.RunAsync();
