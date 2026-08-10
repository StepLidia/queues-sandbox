using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PaymentWorker;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    private IConnection? connection;
    private IChannel? channel;
    private static string QueueName => "orders-payment";
    private static string ExchangeName => "orders-exchange";

    private static string DeadLetterExchangeName => "orders-dead-letter-exchange";

    private static string DeadLetterQueueName => "orders-dead-letter-queue";

    private static string DeadLetterRoutingKey => "payment-dead";

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };

        this.connection = await factory.CreateConnectionAsync(cancellationToken);
        this.channel = await this.connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await this.DeclareDeadLetterExchangeAndQueueAsync(channel, cancellationToken);

        var arguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = DeadLetterExchangeName,
            ["x-dead-letter-routing-key"] = DeadLetterRoutingKey
        };

        await this.channel.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: arguments,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: QueueName,
            exchange: ExchangeName,
            routingKey: string.Empty,
            cancellationToken: cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, args) =>
        {
            long deliveryCount = 0;

            if (args.BasicProperties.Headers?.TryGetValue("x-delivery-count", out var value) == true)
            {
                deliveryCount = Convert.ToInt64(value);
            }

            logger.LogInformation(
                "Delivery tag: {DeliveryTag}, Redelivered: {Redelivered}, DeliveryCount: {DeliveryCount}",
                args.DeliveryTag,
                args.Redelivered,
                deliveryCount);

            try
            {
                var message = Encoding.UTF8.GetString(args.Body.ToArray());
                var order = JsonSerializer.Deserialize<Contracts.OrderCreated>(message);

                logger.LogInformation("Processing order: {OrderId}, Product: {ProductId}, Quantity: {Quantity}, TotalPrice: {TotalPrice}",
                    order?.OrderId, order?.ProductId, order?.Quantity, order?.TotalPrice);

                await Task.Delay(1000, cancellationToken); // Simulate processing time

                throw new Exception("Simulated payment processing failure"); // Simulate a failure
                await channel.BasicAckAsync(args.DeliveryTag, multiple: false, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing message. DeliveryTag: {DeliveryTag}, requeing...", args.DeliveryTag);
                //logger.LogError(ex, "Error processing message. DeliveryTag: {DeliveryTag}, sending it to dead letter queue", args.DeliveryTag);
                //await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true, cancellationToken: cancellationToken);
                await channel.BasicRejectAsync(args.DeliveryTag, requeue: true, cancellationToken: cancellationToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: QueueName,
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

    private async Task DeclareDeadLetterExchangeAndQueueAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(
            exchange: DeadLetterExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: DeadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: DeadLetterQueueName,
            exchange: DeadLetterExchangeName,
            routingKey: DeadLetterRoutingKey,
            cancellationToken: cancellationToken);
    }
}
