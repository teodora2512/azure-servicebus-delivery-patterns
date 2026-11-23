# Azure Service Bus Producer & Consumer – C# / .NET 10

Implementare pentru evenimentul `OrderPlaced`, în care o comandă este plasată, evenimentul este publicat prin Azure Service Bus, iar consumatorul creează și persistă factura în SQL Server.

Soluția urmărește: **Transactional Outbox, idempotency, tranzacții DB, versionare, retry controlat, Dead-Letter Queue și protecția Message Lock**.

## 1. Arhitectură

Nu trimitem evenimentul direct din endpoint după salvarea comenzii, deoarece dacă DB salvează comanda, dar Azure Service Bus nu este disponibil, evenimentul s-ar putea pierde. Folosim **Transactional Outbox**.

1. API primește request-ul și creează comanda.
2. În aceeași tranzacție SQL salvează `Order` și `OutboxMessage`.
3. Outbox Publisher citește evenimentele pending și le publică în Azure Service Bus.
4. Mesajul ajunge în queue `order-placed`.
5. Invoice Consumer validează, deduplică după `EventId` și verifică `Version`.
6. Consumer salvează factura și înregistrarea Inbox în aceeași tranzacție DB.
7. Abia după commit-ul DB, consumer apelează `CompleteMessageAsync`.

## 2. Structura proiectului

```text
OrderInvoiceService/
├── Models/
│   ├── Order.cs
│   ├── Invoice.cs
│   ├── OrderPlacedEvent.cs
│   ├── OutboxMessage.cs
│   └── InboxMessage.cs
├── Data/
│   └── AppDbContext.cs
├── Messaging/
│   ├── OutboxPublisher.cs
│   └── InvoiceConsumer.cs
├── Services/
│   └── OrderService.cs
├── Program.cs
└── appsettings.json
```

## 3. Crearea proiectului și instalarea pachetelor

```bash
dotnet new worker -n OrderInvoiceService -f net10.0
cd OrderInvoiceService

dotnet add package Azure.Messaging.ServiceBus
dotnet add package Microsoft.EntityFrameworkCore.SqlServer
dotnet add package Microsoft.Extensions.Hosting
dotnet add package Microsoft.Extensions.Configuration.UserSecrets
```

Pachete/servicii utilizate:
- **Azure Service Bus** – transportul evenimentelor.
- **Entity Framework Core 10** – persistență SQL.
- **SQL Server** – stocarea comenzilor, facturilor, Outbox și Inbox.
- **BackgroundService** – Outbox Publisher și Consumer.

## 4. Modelele de date

### `Models/Order.cs`

```csharp
namespace OrderInvoiceService.Models;

public class Order
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public string Currency { get; set; } = "RON";
    public int Version { get; set; } = 1;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}
```

### `Models/Invoice.cs`

```csharp
namespace OrderInvoiceService.Models;

public class Invoice
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "RON";
    public int Version { get; set; }
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}
```

> În fragmentul de mai sus, tipul `DateTime` este o greșeală de scriere. Folosește `DateTime`:

```csharp
public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
```

### `Models/OrderPlacedEvent.cs`

```csharp
namespace OrderInvoiceService.Models;

public record OrderPlacedEvent
{
    public Guid EventId { get; init; }
    public Guid OrderId { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public decimal TotalAmount { get; init; }
    public string Currency { get; init; } = "RON";
    public int Version { get; init; }
    public DateTime OccurredAtUtc { get; init; }
}
```

> Corectare typo: `OccurredAtUtc` trebuie să fie `DateTime`.

```csharp
public DateTime OccurredAtUtc { get; init; }
```

### `Models/OutboxMessage.cs`

```csharp
namespace OrderInvoiceService.Models;

public class OutboxMessage
{
    public Guid Id { get; set; }
    public string Type { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? PublishedAtUtc { get; set; }
}
```

> Corectare typo: `CreatedAtUtc` trebuie să fie `DateTime`.

### `Models/InboxMessage.cs`

```csharp
namespace OrderInvoiceService.Models;

public class InboxMessage
{
    public Guid EventId { get; set; }
    public Guid OrderId { get; set; }
    public int Version { get; set; }
    public DateTime ProcessedAtUtc { get; set; }
}
```

> Corectare typo: `ProcessedAtUtc` trebuie să fie `DateTime`.

Se configurează constrângeri unice pentru `EventId` în Inbox și pentru `OrderId` în Invoice, pentru a preveni duplicarea inclusiv în condiții de concurență.

## 5. Configurarea bazei de date – `AppDbContext`

`Data/AppDbContext.cs`

```csharp
using Microsoft.EntityFrameworkCore;
using OrderInvoiceService.Models;

namespace OrderInvoiceService.Data;

public class AppDbContext(DbContextOptions<AppDbContext> options)
    : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Invoice> Invoices => Set<Invoice>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Order>().HasKey(x => x.Id);
        modelBuilder.Entity<Invoice>().HasKey(x => x.Id);

        // O singură factură per comandă.
        modelBuilder.Entity<Invoice>()
            .HasIndex(x => x.OrderId)
            .IsUnique();

        modelBuilder.Entity<OutboxMessage>().HasKey(x => x.Id);
        modelBuilder.Entity<OutboxMessage>()
            .Property(x => x.Payload)
            .IsRequired();

        // EventId este cheia Inbox și garantează unicitatea evenimentului.
        modelBuilder.Entity<InboxMessage>().HasKey(x => x.EventId);
        modelBuilder.Entity<InboxMessage>()
            .HasIndex(x => x.EventId)
            .IsUnique();
    }
}
```

## 6. Producer – crearea comenzii și Outbox

Producer-ul salvează comanda și evenimentul Outbox în aceeași tranzacție SQL. Astfel, nu există situația în care comanda există în DB, dar evenimentul nu a fost înregistrat pentru publicare.

### `Services/OrderService.cs`

```csharp
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using OrderInvoiceService.Data;
using OrderInvoiceService.Models;

namespace OrderInvoiceService.Services;


public class OrderService(AppDbContext db)
{
    public async Task<Guid> PlaceOrderAsync(
        string customerId,
        decimal totalAmount,
        string currency,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(customerId))
            throw new ArgumentException("CustomerId is required.");

        if (totalAmount <= 0)
            throw new ArgumentException("Total amount must be positive.");

        if (string.IsNullOrWhiteSpace(currency))
            throw new ArgumentException("Currency is required.");

        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = customerId,
            TotalAmount = totalAmount,
            Currency = currency.ToUpperInvariant(),
            Version = 1,
            CreatedAtUtc = DateTime.UtcNow
        };

        var orderPlaced = new OrderPlacedEvent
        {
            EventId = Guid.NewGuid(),
            OrderId = order.Id,
            CustomerId = order.CustomerId,
            TotalAmount = order.TotalAmount,
            Currency = order.Currency,
            Version = order.Version,
            OccurredAtUtc = DateTime.UtcNow
        };

        var outbox = new OutboxMessage
        {
            Id = orderPlaced.EventId,
            Type = nameof(OrderPlacedEvent),
            Payload = JsonSerializer.Serialize(orderPlaced),
            CreatedAtUtc = DateTime.UtcNow
        };

        await using var transaction =
            await db.Database.BeginTransactionAsync(cancellationToken);

        db.Orders.Add(order);
        db.OutboxMessages.Add(outbox);

        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        return order.Id;
    }
}
```

## 7. Outbox Publisher – publicarea în Azure Service Bus

Worker-ul verifică periodic Outbox-ul, publică mesajele și marchează mesajul ca publicat.

### `Messaging/OutboxPublisher.cs`

```csharp
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OrderInvoiceService.Data;
using OrderInvoiceService.Models;

namespace OrderInvoiceService.Messaging;


public class OutboxPublisher(
    IServiceScopeFactory scopeFactory,
    ServiceBusSender sender,
    ILogger<OutboxPublisher> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PublishPendingAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
            logger.LogError(ex, "Outbox publishing failed.");
            }

            await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
        }
    }


    private async Task PublishPendingAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var pending = await db.OutboxMessages
            .Where(x => x.PublishedAtUtc == null)
            .OrderBy(x => x.CreatedAtUtc)
            .Take(50)
            .ToListAsync(ct);

        foreach (var item in pending)
        {
            ct.ThrowIfCancellationRequested();

            var evt = JsonSerializer.Deserialize<OrderPlacedEvent>(item.Payload);
            if (evt is null)
                continue;

            var message = new ServiceBusMessage(item.Payload)
            {
                MessageId = evt.EventId.ToString(),
                Subject = item.Type,
                ContentType = "application/json"
            };

            message.ApplicationProperties["OrderId"] = evt.OrderId.ToString();
            message.ApplicationProperties["Version"] = evt.Version;

            await sender.SendMessageAsync(message, ct);

            // Marcăm doar după trimiterea cu succes.
            item.PublishedAtUtc = DateTime.UtcNow;
            await db.SaveChangesAsync(ct);
        }
    }
}
```

### De ce pot exista duplicate?

Dacă Service Bus primește mesajul, dar aplicația se oprește înainte să marcheze `PublishedAtUtc`, la următoarea execuție mesajul va fi publicat din nou. Outbox oferă livrare **at-least-once**, nu atomicitate între SQL și Service Bus. Duplicatele sunt gestionate de consumer prin Inbox și indexul unic `OrderId` al facturii.

Într-un sistem cu mai multe instanțe publisher, trebuie adăugat un mecanism de claim/lease asupra rândurilor Outbox pentru a preveni publicarea concurentă a aceleiași înregistrări.

## 8. Consumer – procesarea evenimentului și crearea facturii

Consumer-ul:
1. Primește mesajul.
2. Verifică dacă JSON-ul este valid.
3. Verifică datele obligatorii.
4. Verifică dacă evenimentul a mai fost procesat.
5. Verifică versiunea facturii.
6. Salvează Inbox + Invoice într-o tranzacție.
7. După commit, apelează `CompleteMessageAsync`.

### Reguli de procesare

| Situație | Acțiune |
|---|---|
| Nu există factură | Creează factura |
| Evenimentul are versiune mai mare | Actualizează factura |
| Evenimentul are versiune mai mică | Ignoră și confirmă |
| Evenimentul are versiune egală | Ignoră și confirmă |
| `EventId` deja procesat | Ignoră și confirmă |
| JSON corupt | Dead-letter |
| Date invalide | Dead-letter |
| DB indisponibilă | Abandon + retry |

### `Messaging/InvoiceConsumer.cs`

```csharp
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OrderInvoiceService.Data;
using OrderInvoiceService.Models;

namespace OrderInvoiceService.Messaging;


public class InvoiceConsumer(
    IServiceScopeFactory scopeFactory,
    ServiceBusClient client,
    IConfiguration configuration,
    ILogger<InvoiceConsumer> logger)
    : BackgroundService
{
    private ServiceBusProcessor? processor;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueName = configuration["ServiceBus:QueueName"]
            ?? throw new InvalidOperationException("ServiceBus:QueueName is missing.");

        processor = client.CreateProcessor(
            queueName,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(10),
                MaxConcurrentCalls = 1,
                PrefetchCount = 0
            });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;

        await processor.StartProcessingAsync(stoppingToken);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Oprire normală a worker-ului.
        }

        await processor.StopProcessingAsync(CancellationToken.None);
    }


    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        var ct = args.CancellationToken;
        var message = args.Message;
        OrderPlacedEvent? evt;

        // Deserializare: eroare permanentă -> DLQ.
        try
        {
            evt = JsonSerializer.Deserialize<OrderPlacedEvent>(message.Body.ToString());
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Invalid JSON. MessageId={MessageId}", message.MessageId);

            await args.DeadLetterMessageAsync(
                message,
                deadLetterReason: "InvalidJson",
                deadLetterErrorDescription: "Message body is not valid OrderPlacedEvent JSON.",
                cancellationToken: ct);
            return;
        }

        // Validare: date invalide -> DLQ.
        if (evt is null ||
            evt.EventId == Guid.Empty ||
            evt.OrderId == Guid.Empty ||
            string.IsNullOrWhiteSpace(evt.CustomerId) ||
            evt.TotalAmount <= 0 ||
            string.IsNullOrWhiteSpace(evt.Currency) ||
            evt.Version <= 0)
        {
            await args.DeadLetterMessageAsync(
                message,
                deadLetterReason: "InvalidOrderPlacedEvent",
                deadLetterErrorDescription: "Required fields are missing or invalid.",
                cancellationToken: ct);
            return;
        }

        try
        {
            // Persist factura și Inbox. Returnarea normală include și duplicatele/versiunile stale.
            await PersistInvoiceAsync(evt, ct);

            // Confirmăm DOAR după persistarea DB/commit.
            await args.CompleteMessageAsync(message, ct);

            logger.LogInformation(
                "Message processed. OrderId={OrderId}, Version={Version}",
                evt.OrderId,
                evt.Version);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Nu confirmăm mesajul. Inbox-ul protejează în cazul în care DB commit a reușit
            // înainte de întrerupere, iar Service Bus va redelivera după expirarea lock-ului.
            throw;
        }
        catch (SqlException ex)
        {
            logger.LogError(ex, "Database error processing OrderId={OrderId}", evt.OrderId);
            await args.AbandonMessageAsync(message, cancellationToken: ct);
        }
        catch (TimeoutException ex)
        {
            logger.LogError(ex, "Timeout processing OrderId={OrderId}", evt.OrderId);
            await args.AbandonMessageAsync(message, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            // Erori neașteptate: NU confirmăm. Abandon pentru redelivery.
            logger.LogError(ex, "Unexpected error processing OrderId={OrderId}", evt.OrderId);
            await args.AbandonMessageAsync(message, cancellationToken: ct);
        }
    }


    private async Task PersistInvoiceAsync(OrderPlacedEvent evt, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        await using var transaction = await db.Database.BeginTransactionAsync(
            System.Data.IsolationLevel.Serializable,
            ct);

        // Idempotency pe EventId (Inbox).
        var alreadyProcessed = await db.InboxMessages
            .AnyAsync(x => x.EventId == evt.EventId, ct);

        if (alreadyProcessed)
        {
            await transaction.CommitAsync(ct);
            return;
        }

        var invoice = await db.Invoices
            .FirstOrDefaultAsync(x => x.OrderId == evt.OrderId, ct);

        // Versiune veche sau egală: ignorăm și înregistrăm în Inbox.
        if (invoice != null && evt.Version <= invoice.Version)
        {
            db.InboxMessages.Add(new InboxMessage
            {
                EventId = evt.EventId,
                OrderId = evt.OrderId,
                Version = evt.Version,
                ProcessedAtUtc = DateTime.UtcNow
            });

            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            return;
        }

        // Creare factura dacă nu există.
        if (invoice == null)
        {
            invoice = new Invoice
            {
                Id = Guid.NewGuid(),
                OrderId = evt.OrderId,
                CustomerId = evt.CustomerId,
                Amount = evt.TotalAmount,
                Currency = evt.Currency,
                Version = evt.Version,
                CreatedAtUtc = DateTime.UtcNow
            };

            db.Invoices.Add(invoice);
        }
        else
        {
            // Actualizăm numai dacă evenimentul este de versiune mai nouă.
            invoice.CustomerId = evt.CustomerId;
            invoice.Amount = evt.TotalAmount;
            invoice.Currency = evt.Currency;
            invoice.Version = evt.Version;
        }


        db.InboxMessages.Add(new InboxMessage
        {
            EventId = evt.EventId,
            OrderId = evt.OrderId,
            Version = evt.Version,
            ProcessedAtUtc = DateTime.UtcNow
        });


        // Factura și Inbox persistate atomic în aceeași tranzacție.
        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }


    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(
            args.Exception,
            "Service Bus processor error. Entity={EntityPath}, Source={ErrorSource}",
            args.EntityPath,
            args.ErrorSource);

        return Task.CompletedTask;
    }


    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (processor != null)
            await processor.DisposeAsync();

        await base.StopAsync(cancellationToken);
    }
}
```

## 9. Configurarea aplicației și dependency injection

### `Program.cs`

```csharp
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using OrderInvoiceService.Data;
using OrderInvoiceService.Messaging;
using OrderInvoiceService.Services;


var builder = Host.CreateApplicationBuilder(args);

var dbConnection = builder.Configuration.GetConnectionString("Database")
    ?? throw new InvalidOperationException("Missing ConnectionStrings:Database configuration.");

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(dbConnection));

var serviceBusConnection = builder.Configuration["ServiceBus:ConnectionString"]
    ?? throw new InvalidOperationException("Missing ServiceBus:ConnectionString configuration.");

var queueName = builder.Configuration["ServiceBus:QueueName"]
    ?? throw new InvalidOperationException("Missing ServiceBus:QueueName configuration.");

builder.Services.AddSingleton(_ => new ServiceBusClient(serviceBusConnection));
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<ServiceBusClient>().CreateSender(queueName));
builder.Services.AddScoped<OrderService>();
builder.Services.AddHostedService<OutboxPublisher>();
builder.Services.AddHostedService<InvoiceConsumer>();

var host = builder.Build();
await host.RunAsync();
```

### `appsettings.json`

Nu introducem secrete în acest fișier.

```json
{
  "ServiceBus": {
    "QueueName": "order-placed"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.Hosting.Lifetime": "Information"
    }
  }
}
```

## 10. Configurarea secretelor local

Pe macOS, pentru development, folosește User Secrets.

Inițializează:

```bash
dotnet user-secrets init
```

Setează connection string SQL:

```bash
dotnet user-secrets set "ConnectionStrings:Database" "YOUR_SQL_CONNECTION_STRING"
```

Setează connection string Azure Service Bus:

```bash
dotnet user-secrets set "ServiceBus:ConnectionString" "YOUR_SERVICE_BUS_CONNECTION_STRING"
```

Pentru producție sunt preferabile:
- Azure Managed Identity
- Azure Key Vault
- Environment variables
- Managed identity pentru Service Bus (fără connection string)

Variabile de mediu (exemplu):

```bash
export ConnectionStrings__Database="YOUR_SQL_CONNECTION_STRING"
export ServiceBus__ConnectionString="YOUR_SERVICE_BUS_CONNECTION_STRING"
export ServiceBus__QueueName="order-placed"
```

## 11. Crearea tabelelor în SQL Server

Instalează EF Core CLI dacă nu este instalat:

```bash
dotnet tool install --global dotnet-ef --version 10.*
```

Creează migration:

```bash
dotnet ef migrations add InitialCreate
```

Aplică migration:

```bash
dotnet ef database update
```

Tabele:

| Tabel | Rol |
|---|---|
| `Orders` | Comenzi plasate |
| `Invoices` | Facturi generate |
| `OutboxMessages` | Evenimente pending pentru publicare |
| `InboxMessages` | Evenimente procesate |

## 12. Configurarea Azure Service Bus

În Azure Portal creează un Service Bus Namespace și o Queue.

Setări recomandate pentru exemplu:

| Parametru | Valoare |
|---|---|
| Queue name | `order-placed` |
| Requires duplicate detection | Opțional, recomandat |
| Lock duration | 1 minut |
| Max delivery count | 5 |
| Dead lettering on expiration | Enabled |
| Sessions | Disabled |

### Retry și Dead-Letter Queue

Pentru erori tranzitorii, consumer-ul abandonează mesajul:

```csharp
await args.AbandonMessageAsync(message, cancellationToken: ct);
```

Service Bus îl redeliver-ează. După depășirea `MaxDeliveryCount`, mesajul ajunge automat în Dead-Letter Queue.

Pentru erori permanente, mesajul este dead-letter direct:

```csharp
await args.DeadLetterMessageAsync(
    message,
    deadLetterReason: "InvalidOrderPlacedEvent",
    deadLetterErrorDescription: "Invalid data.",
    cancellationToken: ct);
```

## 13. Protecție pentru procesare concurentă

Consumer-ul folosește o tranzacție serializabilă în metoda `PersistInvoiceAsync`:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(
    System.Data.IsolationLevel.Serializable,
    ct);
```

În combinație cu indexul unic pe `Invoice.OrderId`, ajută la evitarea creării/actualizării concurente a aceleiași facturi. Pentru sisteme cu throughput ridicat, ia în considerare optimistic concurrency (`rowversion`) și tratarea explicită a conflictelor de concurență.

## 14. Scenarii de oprire/eroare

### Scenariul 1 – aplicația se oprește înainte de salvarea facturii

Tranzacția DB este rollback. Mesajul nu este confirmat, deci Service Bus îl livrează din nou.

### Scenariul 2 – factura este salvată, dar aplicația se oprește înainte de `Complete`

Factura este deja salvată. La redelivery, consumer-ul găsește `EventId` în Inbox și confirmă mesajul fără a crea din nou factura.

### Scenariul 3 – Outbox publică mesajul, dar nu marchează `Published`

Mesajul poate fi publicat de două ori. Consumer-ul îl identifică prin Inbox sau cheia unică `OrderId` și îl ignoră.

### Scenariul 4 – sosește Version 1 după Version 2

Consumer-ul compară versiunea cu cea persistată. Version 1 este ignorată și mesajul este confirmat, fără a suprascrie factura Version 2.

## 15. Verificarea celor 8 reguli

| # | Cerință | Implementare |
|---|---|---|
| 1 | Complete după persist | `SaveChangesAsync` + `CommitAsync`, apoi `CompleteMessageAsync` |
| 2 | Fără confirmare în catch | Catch-urile nu confirmă mesajul; abandon/rethrow la erori |
| 3 | Idempotency | Inbox `EventId` + index unic `Invoice.OrderId` |
| 4 | Retry vs DLQ | SQL/timeout → Abandon; JSON/date invalide → Dead-letter |
| 5 | Transactional Outbox | Order și Outbox în aceeași tranzacție |
| 6 | Versionare | `Version` pe Order, Event și Invoice |
| 7 | Cancellation + Lock | CancellationToken transmis; auto-renewal până la 10 minute |
| 8 | Secrets | User Secrets / Environment / Managed Identity |

## Observații tehnice pentru producție

- Outbox Publisher trebuie să aibă claim/lease pentru a coordona mai multe instanțe.
- Publisher-ul poate beneficia de retry cu backoff și jitter.
- Folosește `rowversion`/optimistic concurrency pentru comenzi și facturi, după caz.
- Preferă Managed Identity și Key Vault în Azure.
- Monitorizează Outbox backlog, retry count, DLQ și timpul de procesare.
- Adaugă un API pentru plasarea comenzilor și un endpoint pentru consultarea facturilor.
- `OrderPlaced` este, de regulă, un eveniment inițial cu `Version = 1`. Modificările ulterioare ar trebui reprezentate prin evenimente distincte (`OrderUpdated`, `OrderCancelled` etc.), folosind aceeași strategie de versionare și idempotency.

