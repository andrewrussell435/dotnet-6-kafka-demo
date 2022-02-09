using Confluent.Kafka;

namespace KafkaDemo;

public class ConsumerWorker : BackgroundService
{
    private readonly ILogger<ConsumerWorker> _logger;
    private readonly string _kafkaTopic;
    private readonly ConsumerConfig _config;

    public ConsumerWorker(ILogger<ConsumerWorker> logger)
    {
        _logger = logger;
        _kafkaTopic = "test-topic";
        var host = "broker:29092";
        _config = GetConfig(host);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
    {
        _logger.LogInformation("Consumer Worker running at: {time}", DateTimeOffset.Now);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();

            try
            {
                consumer.Subscribe(_kafkaTopic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        _logger.LogInformation(
                            $"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (Exception e)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }

            await Task.Delay(500, stoppingToken);
        }
    }, stoppingToken);

    private ConsumerConfig GetConfig(string host)
    {
        return new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = host,
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are not yet any committed offsets for the consumer group for the
            // topic/partitions of interest. By default, offsets are committed
            // automatically, so in this example, consumption will only start from the
            // earliest message in the topic 'my-topic' the first time you run the program.
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
    }
}