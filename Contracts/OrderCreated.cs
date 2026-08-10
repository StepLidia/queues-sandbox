namespace Contracts;

public record OrderCreated(
    Guid OrderId,
    string ProductId,
    int Quantity,
    decimal TotalPrice
);
