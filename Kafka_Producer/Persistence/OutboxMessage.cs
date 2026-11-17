namespace Kafka_Worker_Producer.Persistence;

public class OutboxMessage
{
    public Guid Id { get; set; }
    public string Topic { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public byte[] Payload { get; set; } = [];
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? ProcessedAtUtc { get; set; }
    public string? Error { get; set; }
    
}