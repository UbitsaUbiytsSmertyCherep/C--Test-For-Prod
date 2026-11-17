namespace Kafka_Worker_Producer.Domain;

public class Alert
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public int Severity { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
