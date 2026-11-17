namespace Kafka_with_Worker.Domain;

public class AlertRecieved
{

public Guid Id { get; set; }
public string Title { get; set; } = string.Empty;
public int Severity { get; set; }
public DateTime CreatedAtUtc { get; set; }


}