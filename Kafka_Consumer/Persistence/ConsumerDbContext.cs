using Kafka_with_Worker.Domain;
using Microsoft.EntityFrameworkCore;

namespace Kafka_with_Worker.Persistence;

public class ConsumerDbContext: DbContext
{
    
    public DbSet<InboxRecord> InboxRecords => Set<InboxRecord>();
    public DbSet<AlertRecieved> AlertsRecieved=> Set<AlertRecieved>();
    public ConsumerDbContext(DbContextOptions options) : base(options) {}

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InboxRecord>().HasKey(e => e.Id);
        modelBuilder.Entity<AlertRecieved>().HasKey(x => x.Id);
    }
}