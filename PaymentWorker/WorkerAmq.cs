using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PaymentWorker;

public class WorkerAmq(ILogger<WorkerRmq> logger) : BackgroundService
{
    private IConnection? connection;
    private IChannel? channel;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {

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
