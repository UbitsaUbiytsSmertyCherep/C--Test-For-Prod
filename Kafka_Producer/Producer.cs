// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Kafka_Worker_Producer;
using Kafka_Worker_Producer.Persistence;
using Kafka_Worker_Producer.Workers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;



var builder = Host.CreateApplicationBuilder(args);
var conStr = "Host=localhost;Port=5432;Database=alert-db;Username=app_user;Password=secret_password";
builder.Services.AddDbContext<AppDbContext>(options =>
{
    options.UseNpgsql(conStr);
});
builder.Services.AddSingleton<IProducer<string, byte[]>>(_ =>
{
    var conf = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        Acks = Acks.All,
        EnableIdempotence = true
    };
    return new ProducerBuilder<string, byte[]>(conf).Build();
});

builder.Services.AddHostedService<AlertGeneratorWorker>();
builder.Services.AddHostedService<OutboxPublishWorker>();

var app = builder.Build();
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
    
}

await app.RunAsync();
