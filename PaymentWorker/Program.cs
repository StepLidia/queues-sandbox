using PaymentWorker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<WorkerAmq>();

var host = builder.Build();
host.Run();
