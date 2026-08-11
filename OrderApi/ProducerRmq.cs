using System.Text.Json;
using Contracts;
using RabbitMQ.Client;

namespace OrderApi;

public class ProducerRmq : IAsyncDisposable
{
    private readonly IConnection connection;

    private readonly IChannel channel;

    //private static string QueueName => "orders";

    private ProducerRmq(IConnection connection, IChannel channel)
    {
        this.connection = connection;
        this.channel = channel;
    }

    public static async Task<ProducerRmq> CreateAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };
        var connection = await factory.CreateConnectionAsync();

        var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
        var channel = await connection.CreateChannelAsync(options);

        //await channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false);
        await channel.ExchangeDeclareAsync(exchange: RabbitMqTopology.OrdersExchange, type: ExchangeType.Fanout, durable: true, autoDelete: false);

        return new ProducerRmq(connection, channel);
    }

    public async Task PublishAsync(OrderCreated order)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(order);

        var properties = new BasicProperties
        {
            MessageId = Guid.NewGuid().ToString(),
            Persistent = true, // message will survive broker restarts
        };

        //await this.channel.BasicPublishAsync(exchange: string.Empty, routingKey: QueueName, body: body);
        await this.channel.BasicPublishAsync(exchange: RabbitMqTopology.OrdersExchange, routingKey: string.Empty, mandatory: false, basicProperties: properties, body: body);
    }

    public async ValueTask DisposeAsync()
    {
        await this.channel.DisposeAsync();
        await this.connection.DisposeAsync();
    }
}