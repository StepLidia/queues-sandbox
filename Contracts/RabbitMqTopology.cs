public static class RabbitMqTopology
{
    public static string OrdersExchange => "orders-exchange";
    public static string PaymentQueue => "orders-payment";
    public static string InventoryQueue => "orders-inventory";
    public static string DeadLetterExchangeName => "orders-dead-letter-exchange";
    public static string DeadLetterQueueName => "orders-dead-letter-queue";
    public static string DeadLetterRoutingKey => "payment-dead";
}