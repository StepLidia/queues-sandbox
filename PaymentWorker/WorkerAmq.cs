using Apache.NMS;
using Apache.NMS.AMQP;

namespace PaymentWorker;

public class WorkerAmq(ILogger<WorkerRmq> logger) : BackgroundService
{
    private IConnection? connection;
    private ISession? session;
    private IMessageConsumer? messageConsumer;

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory("amqp://localhost:61616");

        this.connection = factory.CreateConnection("artemis", "artemis");
        this.connection.Start();

        this.session = this.connection.CreateSession(AcknowledgementMode.ClientAcknowledge);

        var queue = this.session.GetQueue(ActiveMqTopology.OrdersQueue);

        this.messageConsumer = this.session.CreateConsumer(queue);
        this.messageConsumer.Listener += message =>
        {
            if (message is ITextMessage textMessage)
            {
                logger.LogInformation("Received order: {Order}", textMessage.Text);
                textMessage.Acknowledge();
            }
        };

        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        this.messageConsumer?.Dispose();
        this.session?.Dispose();
        this.connection?.Dispose();

        return base.StopAsync(cancellationToken);
    }
}
