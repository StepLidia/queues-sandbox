using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PaymentWorker;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    private IConnection? connection;
    private IChannel? channel;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };

        this.connection = await factory.CreateConnectionAsync(cancellationToken);
        this.channel = await this.connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: "orders",
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, args) =>
        {
            try
            {
                var message = Encoding.UTF8.GetString(args.Body.ToArray());
                var order = JsonSerializer.Deserialize<Contracts.OrderCreated>(message);

                logger.LogInformation("Processing order: {OrderId}, Product: {ProductId}, Quantity: {Quantity}, TotalPrice: {TotalPrice}",
                    order?.OrderId, order?.ProductId, order?.Quantity, order?.TotalPrice);

                await Task.Delay(1000, cancellationToken); // Simulate processing time

                await channel.BasicAckAsync(args.DeliveryTag, multiple: false, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing order: {Message}, requeing...", args.Body.ToString());
                await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true, cancellationToken: cancellationToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: "orders",
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);

        // Keep the service running until cancellation is requested
        await Task.Delay(Timeout.Infinite, cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (this.channel is not null)
        {
            await this.channel.DisposeAsync();
        }

        if (this.connection is not null)
        {
            await this.connection.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }
}
