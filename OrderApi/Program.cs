using Contracts;
using Microsoft.AspNetCore.Mvc;
using OrderApi;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddSingleton(await ProducerRmq.CreateAsync());
builder.Services.AddSingleton(ProducerAmq.Create());

var app = builder.Build();

var producer = app.Services.GetService<ProducerAmq>();

Console.WriteLine(
    producer is null
        ? "ProducerAmq NOT registered"
        : "ProducerAmq registered");

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/orders", async (OrderCreated order, ProducerRmq producer) =>
{
    await producer.PublishAsync(order);
    return Results.Accepted();
});

app.MapPost("/orders-amq", (OrderCreated order, ProducerAmq producer) =>
{
    producer.Publish(order);
    return Results.Accepted();
});

app.Run();
