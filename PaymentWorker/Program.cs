using PaymentWorker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<WorkerRmq>();

var host = builder.Build();
host.Run();
