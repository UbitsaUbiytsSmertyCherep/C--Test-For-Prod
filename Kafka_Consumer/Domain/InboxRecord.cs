namespace Kafka_with_Worker.Domain;

public class InboxRecord
{
    public Guid Id { get; set; }
    public DateTime ReceivedAt { get; set; }
}