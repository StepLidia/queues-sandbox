using Apache.NMS;
using Apache.NMS.AMQP;
using System.Text.Json;

namespace OrderApi;

public class ProducerAmq : IDisposable
{
    private readonly IConnection connection;
    private readonly Apache.NMS.ISession session;
    private readonly IMessageProducer producer;

    private ProducerAmq(IConnection connection, Apache.NMS.ISession session, IMessageProducer producer)
    {
        this.connection = connection;
        this.session = session;
        this.producer = producer;
    }

    public static ProducerAmq Create()
    {
        var factory = new ConnectionFactory("amqp://localhost:61616");

        var connection = factory.CreateConnection("artemis", "artemis");
        connection.Start();

        var session = connection.CreateSession();
        var queue = session.GetQueue(ActiveMqTopology.OrdersQueue);
        var producer = session.CreateProducer(queue);

        return new ProducerAmq(connection, session, producer);
    }

    public void Publish(Contracts.OrderCreated order)
    {
        var message = session.CreateTextMessage(JsonSerializer.Serialize(order));
        producer.Send(message);
    }

    public void Dispose()
    {
        producer.Dispose();
        session.Dispose();
        connection.Dispose();
    }
}