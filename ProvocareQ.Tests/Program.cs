using System.Collections.Concurrent;
using System.Text.Json;

Console.WriteLine("=== PROVOCAREA Q: TESTE CU DEFECTE INJECTATE (V1 vs V2) ===\n");

int v1Passed = 0, v2Passed = 0;

// 1. TEST DUPLICAT (același mesaj livrat de 2 ori)
v1Passed += RunTest("1. Duplicat (V1 - fara reguli)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v1 = new V1Consumer(broker);
    var orderId = Guid.NewGuid();
    var msg = new BrokerMessage(orderId.ToString(), JsonSerializer.Serialize(new OrderEventV1(orderId, "CUST-1", 500m)));

    broker.Enqueue(msg);
    v1.ProcessNext(simulateCrashBeforeComplete: false);
    // Simulam relivrarea aceluiasi mesaj de catre broker
    broker.Enqueue(msg);
    v1.ProcessNext(simulateCrashBeforeComplete: false);

    // Verificam starea rezultata: trebuie sa existe O SINGURA factura
    return v1.Invoices.Count(i => i.OrderId == orderId) == 1 && broker.DeadLetterQueue.Count == 0;
}) ? 1 : 0;

v2Passed += RunTest("1. Duplicat (V2 - cu reguli)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v2 = new V2Consumer(broker);
    var eventId = Guid.NewGuid();
    var orderId = Guid.NewGuid();
    var msg = new BrokerMessage(eventId.ToString(), JsonSerializer.Serialize(new OrderEventV2(eventId, orderId, "CUST-1", 500m, Version: 1)));

    broker.Enqueue(msg);
    v2.ProcessNext(simulateCrashBeforeComplete: false);
    broker.Enqueue(msg);
    v2.ProcessNext(simulateCrashBeforeComplete: false);

    return v2.Invoices.Count(i => i.OrderId == orderId) == 1 && v2.InboxEventIds.Count == 1;
}) ? 1 : 0;

// 2. TEST CADERE INAINTE DE COMPLETE (efect salvat, confirmare pierduta, mesaj relivrat)
v1Passed += RunTest("2. Cadere inainte de Complete (V1)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v1 = new V1Consumer(broker);
    var orderId = Guid.NewGuid();
    broker.Enqueue(new BrokerMessage(orderId.ToString(), JsonSerializer.Serialize(new OrderEventV1(orderId, "CUST-1", 500m))));

    // Prima livrare: salveaza factura, dar cade inainte de Complete -> mesajul ramane in coada (lock expirat)
    v1.ProcessNext(simulateCrashBeforeComplete: true);
    // A doua livrare: mesajul revine
    v1.ProcessNext(simulateCrashBeforeComplete: false);

    return v1.Invoices.Count(i => i.OrderId == orderId) == 1 && broker.ActiveCount == 0;
}) ? 1 : 0;

v2Passed += RunTest("2. Cadere inainte de Complete (V2)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v2 = new V2Consumer(broker);
    var eventId = Guid.NewGuid();
    var orderId = Guid.NewGuid();
    broker.Enqueue(new BrokerMessage(eventId.ToString(), JsonSerializer.Serialize(new OrderEventV2(eventId, orderId, "CUST-1", 500m, Version: 1))));

    v2.ProcessNext(simulateCrashBeforeComplete: true);
    v2.ProcessNext(simulateCrashBeforeComplete: false);

    return v2.Invoices.Count(i => i.OrderId == orderId) == 1 && broker.ActiveCount == 0;
}) ? 1 : 0;

// 3. TEST ORDINE INVERSA (v2 soseste inainte de v1 pentru aceeasi comanda)
v1Passed += RunTest("3. Ordine inversa (V1 - fara versiune)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v1 = new V1Consumer(broker);
    var orderId = Guid.NewGuid();

    // Soseste intai v2 (suma corecta 900), apoi v1 (suma veche 500)
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), JsonSerializer.Serialize(new OrderEventV1(orderId, "CUST-1", 900m))));
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), JsonSerializer.Serialize(new OrderEventV1(orderId, "CUST-1", 500m))));

    v1.ProcessNext(false);
    v1.ProcessNext(false);

    // V1 ignora al doilea mesaj pentru ca OrderId exista deja, dar nu stie sa actualizeze daca v1 ar fi venit primul si v2 al doilea!
    // Daca testam invers (v1 primul = 500, v2 al doilea = 900), V1 ramane blocat pe suma veche 500!
    var broker2 = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v1_b = new V1Consumer(broker2);
    var orderId2 = Guid.NewGuid();
    broker2.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), JsonSerializer.Serialize(new OrderEventV1(orderId2, "CUST-1", 500m))));
    broker2.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), JsonSerializer.Serialize(new OrderEventV1(orderId2, "CUST-1", 900m))));
    v1_b.ProcessNext(false);
    v1_b.ProcessNext(false);

    return v1_b.Invoices.First(i => i.OrderId == orderId2).Amount == 900m; // VA PICA (ramane 500m)
}) ? 1 : 0;

v2Passed += RunTest("3. Ordine inversa (V2 - cu Version)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v2 = new V2Consumer(broker);
    var orderId = Guid.NewGuid();

    // Soseste v2 (900 RON), apoi v1 (500 RON)
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), JsonSerializer.Serialize(new OrderEventV2(Guid.NewGuid(), orderId, "CUST-1", 900m, Version: 2))));
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), JsonSerializer.Serialize(new OrderEventV2(Guid.NewGuid(), orderId, "CUST-1", 500m, Version: 1))));

    v2.ProcessNext(false);
    v2.ProcessNext(false);

    // Starea finala trebuie sa ramana 900 RON (v2), nu sa mearga inapoi la 500 RON
    return v2.Invoices.First(i => i.OrderId == orderId).Amount == 900m && v2.Invoices.First(i => i.OrderId == orderId).Version == 2;
}) ? 1 : 0;

// 4. TEST MESAJ OTRAVIT (JSON corupt -> trebuie sa mearga IMEDIAT in DLQ la prima livrare, fara retry)
v1Passed += RunTest("4. Mesaj otravit (V1 - BUG: face Abandon/retry in loc de DLQ)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v1 = new V1Consumer(broker);
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), "{ JSON CORUPT %%%"));

    v1.ProcessNext(false);

    // Asteptare corecta: mesajul permanent corupt sa fie in DLQ dupa 1 singura incercare
    return broker.DeadLetterQueue.Count == 1 && broker.ActiveCount == 0; // VA PICA in V1 (ramane in ActiveCount pentru retry!)
}) ? 1 : 0;

v2Passed += RunTest("4. Mesaj otravit (V2 - DLQ imediat cu motiv)", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v2 = new V2Consumer(broker);
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), "{ JSON CORUPT %%%"));

    v2.ProcessNext(false);

    return broker.DeadLetterQueue.Count == 1 && broker.DeadLetterQueue[0].Reason == "InvalidJson";
}) ? 1 : 0;

// DEMO BUG FIX (Test ROSU -> VERDE pentru V1 pe mesaj otravit)
Console.WriteLine("\n--- DEMO BUG REAL GASIT IN V1 SI CORECTAT ---");
RunTest("V1 FIXED - Mesaj otravit trimis direct in DLQ", () =>
{
    var broker = new InMemoryPeekLockBroker(maxDeliveryCount: 3);
    var v1Fixed = new V1ConsumerFixed(broker);
    broker.Enqueue(new BrokerMessage(Guid.NewGuid().ToString(), "{ JSON CORUPT %%%"));

    v1Fixed.ProcessNext(false);

    return broker.DeadLetterQueue.Count == 1 && broker.DeadLetterQueue[0].Reason == "InvalidJson";
});

Console.WriteLine($"\nREZULTAT FINAL: V1 a trecut {v1Passed}/4 teste | V2 a trecut {v2Passed}/4 teste.");

static bool RunTest(string name, Func<bool> test)
{
    bool ok = test();
    Console.ForegroundColor = ok ? ConsoleColor.Green : ConsoleColor.Red;
    Console.WriteLine($"[{(ok ? "PASS / VERDE" : "FAIL / ROSU")}] {name}");
    Console.ResetColor();
    return ok;
}

// --- INFRASTRUCTURA BROKER FALS (PEEK-LOCK) ---
record BrokerMessage(string MessageId, string Body)
{
    public int DeliveryCount { get; set; }
}
record DeadLetterEntry(BrokerMessage Message, string Reason);

class InMemoryPeekLockBroker(int maxDeliveryCount)
{
    private readonly Queue<BrokerMessage> _queue = new();
    public List<DeadLetterEntry> DeadLetterQueue { get; } = [];
    public int ActiveCount => _queue.Count;

    public void Enqueue(BrokerMessage msg) => _queue.Enqueue(msg);

    public BrokerMessage? ReceivePeekLock()
    {
        if (!_queue.TryDequeue(out var msg)) return null;
        msg.DeliveryCount++;
        return msg;
    }

    public void Complete(BrokerMessage msg) { /* Mesaj sters definitiv */ }

    public void Abandon(BrokerMessage msg)
    {
        if (msg.DeliveryCount >= maxDeliveryCount)
            DeadLetterQueue.Add(new DeadLetterEntry(msg, "MaxDeliveryCountExceeded"));
        else
            _queue.Enqueue(msg);
    }

    public void DeadLetter(BrokerMessage msg, string reason) =>
        DeadLetterQueue.Add(new DeadLetterEntry(msg, reason));
}

// --- MODELE SI CONSUMATORI SIMULATI DUPA CODUL GENERAT DE AI ---
record OrderEventV1(Guid OrderId, string CustomerId, decimal TotalAmount);
record OrderEventV2(Guid EventId, Guid OrderId, string CustomerId, decimal TotalAmount, int Version);
class InvoiceRecord { public Guid OrderId { get; set; } public decimal Amount { get; set; } public int Version { get; set; } }

// Versiunea 1 (Fara reguli - exact logica din Versiunea1.docx)
class V1Consumer(InMemoryPeekLockBroker broker)
{
    public List<InvoiceRecord> Invoices { get; } = [];
    public void ProcessNext(bool simulateCrashBeforeComplete)
    {
        var msg = broker.ReceivePeekLock();
        if (msg is null) return;
        try
        {
            var order = JsonSerializer.Deserialize<OrderEventV1>(msg.Body) ?? throw new InvalidOperationException();
            if (order.OrderId == Guid.Empty || order.TotalAmount <= 0) throw new InvalidOperationException();

            if (!Invoices.Any(i => i.OrderId == order.OrderId))
                Invoices.Add(new InvoiceRecord { OrderId = order.OrderId, Amount = order.TotalAmount, Version = 1 });

            if (simulateCrashBeforeComplete)
            {
                // Cadere simulata: lock-ul expira si mesajul revine in coada
                broker.Abandon(msg);
                return;
            }
            broker.Complete(msg);
        }
        catch (Exception)
        {
            // BUG V1: Orice eroare (inclusiv JsonException) face Abandon in loc de DLQ!
            broker.Abandon(msg);
        }
    }
}

// Versiunea 1 CORECTATA (trateaza separat JsonException / date invalide -> DLQ)
class V1ConsumerFixed(InMemoryPeekLockBroker broker)
{
    public List<InvoiceRecord> Invoices { get; } = [];
    public void ProcessNext(bool simulateCrashBeforeComplete)
    {
        var msg = broker.ReceivePeekLock();
        if (msg is null) return;
        try
        {
            var order = JsonSerializer.Deserialize<OrderEventV1>(msg.Body) ?? throw new JsonException("Null payload");
            if (order.OrderId == Guid.Empty || order.TotalAmount <= 0)
            {
                broker.DeadLetter(msg, "InvalidData");
                return;
            }

            if (!Invoices.Any(i => i.OrderId == order.OrderId))
                Invoices.Add(new InvoiceRecord { OrderId = order.OrderId, Amount = order.TotalAmount, Version = 1 });

            if (simulateCrashBeforeComplete) { broker.Abandon(msg); return; }
            broker.Complete(msg);
        }
        catch (JsonException)
        {
            // FIX: Eroare permanenta -> DLQ imediat
            broker.DeadLetter(msg, "InvalidJson");
        }
        catch (Exception)
        {
            broker.Abandon(msg);
        }
    }
}

// Versiunea 2 (Cu regulile din Cursul 7 - exact logica din Versiunea2.docx)
class V2Consumer(InMemoryPeekLockBroker broker)
{
    public List<InvoiceRecord> Invoices { get; } = [];
    public HashSet<Guid> InboxEventIds { get; } = [];

    public void ProcessNext(bool simulateCrashBeforeComplete)
    {
        var msg = broker.ReceivePeekLock();
        if (msg is null) return;

        OrderEventV2? evt;
        try
        {
            evt = JsonSerializer.Deserialize<OrderEventV2>(msg.Body);
        }
        catch (JsonException)
        {
            broker.DeadLetter(msg, "InvalidJson");
            return;
        }

        if (evt is null || evt.EventId == Guid.Empty || evt.OrderId == Guid.Empty || evt.TotalAmount <= 0 || evt.Version <= 0)
        {
            broker.DeadLetter(msg, "InvalidOrderPlacedEvent");
            return;
        }

        if (InboxEventIds.Contains(evt.EventId))
        {
            broker.Complete(msg);
            return;
        }

        var existing = Invoices.FirstOrDefault(i => i.OrderId == evt.OrderId);
        if (existing != null && evt.Version <= existing.Version)
        {
            InboxEventIds.Add(evt.EventId);
            broker.Complete(msg);
            return;
        }

        if (existing == null)
            Invoices.Add(new InvoiceRecord { OrderId = evt.OrderId, Amount = evt.TotalAmount, Version = evt.Version });
        else
        {
            existing.Amount = evt.TotalAmount;
            existing.Version = evt.Version;
        }

        InboxEventIds.Add(evt.EventId);

        if (simulateCrashBeforeComplete)
        {
            broker.Abandon(msg);
            return;
        }
        broker.Complete(msg);
    }
}