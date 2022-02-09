using KafkaDemo;

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<ConsumerWorker>();
        services.AddHostedService<PublisherWorker>();
    })
    .Build();

await host.RunAsync();