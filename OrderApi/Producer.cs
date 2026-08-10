using System.Text.Json;
using Contracts;
using RabbitMQ.Client;

namespace OrderApi;

public class Producer : IAsyncDisposable
{
    private readonly IConnection connection;

    private readonly IChannel channel;

    private static string QueueName => "orders";

    private Producer(IConnection connection, IChannel channel)
    {
        this.connection = connection;
        this.channel = channel;
    }

    public static async Task<Producer> CreateAsync()
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false);

        return new Producer(connection, channel);
    }

    public async Task PublishAsync(OrderCreated order)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(order);

        await this.channel.BasicPublishAsync(exchange: string.Empty, routingKey: QueueName, body: body);
    }

    public async ValueTask DisposeAsync()
    {
        await this.channel.DisposeAsync();
        await this.connection.DisposeAsync();
    }
}