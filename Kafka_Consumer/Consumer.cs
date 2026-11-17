using Confluent.Kafka;
using Kafka_with_Worker;
using Kafka_with_Worker.Persistence;
using Kafka_with_Worker.Workers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

string connectionString = "Host=localhost;port=5432;username=app_user;password=secret_password;database=consumer-db";
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton(new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
});


builder.Services.AddSingleton<IProducer<byte[], byte[]>>(_ =>
{
    var conf = new ProducerConfig()
    {
        BootstrapServers = "localhost:9092",
        AllowAutoCreateTopics = false
    };
    return new ProducerBuilder<byte[], byte[]>(conf).Build();
});



builder.Services.AddDbContext<ConsumerDbContext>(options =>
{
    options.UseNpgsql(connectionString);
});
builder.Services.AddHostedService<AlertConsumerWorker>();
var app = builder.Build();
using (var scope = app.Services.CreateScope())
{
    await using var db = scope.ServiceProvider.GetRequiredService<ConsumerDbContext>();
    await db.Database.EnsureCreatedAsync();
}

await app.RunAsync();