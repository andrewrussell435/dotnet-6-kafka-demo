using Confluent.Kafka;

namespace KafkaDemo;

public class PublisherWorker : BackgroundService
{
    private readonly ILogger<PublisherWorker> _logger;
    private readonly string _kafkaTopic;
    private readonly ProducerConfig _config;
    
    public PublisherWorker(ILogger<PublisherWorker> logger)
    {
        _logger = logger;
        _kafkaTopic = "test-topic";
        _config =  new ProducerConfig { BootstrapServers = "broker:29092" };
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
    {
        _logger.LogInformation("Publisher Worker running at: {time}", DateTimeOffset.Now);

        while (!stoppingToken.IsCancellationRequested)
        {
            string message = $"Current time is {DateTimeOffset.Now.ToString("MM/dd/yyyy")}";
            
            using var producer = new ProducerBuilder<Null, string>(_config).Build();
            try
            {
                var deliveryResult = await producer.ProduceAsync(_kafkaTopic, new Message<Null, string> { Value=message });

                var successMessage = $"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'";
                _logger.LogInformation(successMessage);
            }
            catch (ProduceException<Null, string> e)
            {
                var errorMessage = $"Delivery failed: {e.Error.Reason}";
                _logger.LogError(errorMessage);
            }
            
            await Task.Delay(2000, stoppingToken);
        }
    }, stoppingToken);
}