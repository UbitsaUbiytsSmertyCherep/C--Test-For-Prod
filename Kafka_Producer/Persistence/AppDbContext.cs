using Kafka_Contracts.Contracts;
using Kafka_Worker_Producer.Domain;
using Microsoft.EntityFrameworkCore;

namespace Kafka_Worker_Producer.Persistence;

public class AppDbContext: DbContext
{
    public DbSet<Alert> Alerts => Set<Alert>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(x => new { x.ProcessedAtUtc, x.CreatedAtUtc });
    }
}