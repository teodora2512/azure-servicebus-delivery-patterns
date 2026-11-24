# Azure Service Bus Delivery Guarantees & Consumer Idempotency 

Experiment comparativ în **C# / .NET 10** între cod generat de AI fără constrângeri arhitecturale (**V1**) și cod generat pe baza regulilor de livrare fiabilă (**V2**) pentru un flux asincron de tip `OrderPlaced` → Emitere Factură.

- **Model AI testat:** ChatGPT (GPT-5)
- **Data experimentului:** 22.11.2026
- **Domeniu:** Procesare comenzi (`OrderPlaced`) și emitere facturi (`Invoice`) prin Azure Service Bus

---

## Prompturile utilizate

### Prompt V1 (Fără reguli)
> Scrie o implementare completă în C# / .NET 10 folosind Azure Service Bus (`Azure.Messaging.ServiceBus`) pentru următorul scenariu: un serviciu de comenzi (`OrderProducer`) publică evenimentul `OrderPlaced` într-un queue, iar un consumator (`InvoiceConsumer`) citește evenimentul, generează factura și o salvează în baza de date.

### Prompt V2 (Cu cele 8 reguli de livrare și reziliență)
> Scrie o implementare completă în C# / .NET 10 folosind Azure Service Bus (`Azure.Messaging.ServiceBus`) pentru scenariul în care plasarea unei comenzi publică evenimentul `OrderPlaced`, iar un consumator creează și persistă factura în baza de date. Respectă obligatoriu următoarele 8 reguli:
> 1. Confirmă mesajul (`CompleteMessageAsync`) doar **după** ce efectul este salvat în baza de date, niciodată înainte.
> 2. Niciun bloc `catch` generic nu confirmă mesajul (fără pierdere tăcută).
> 3. Handler idempotent (tabel `Inbox` după `EventId` și/sau cheie naturală `OrderId` în aceeași tranzacție).
> 4. Erori tranzitorii (DB/timeout) → `AbandonMessageAsync` (retry); erori permanente (JSON corupt, date invalide) → `DeadLetterMessageAsync` cu motiv explicit.
> 5. Transactional Outbox la producător (salvare `Order` + `OutboxMessage` în aceeași tranzacție).
> 6. Versionare (`Version`) pe comandă, eveniment și factură pentru a ignora evenimentele sosite în ordine inversă.
> 7. Transmite `CancellationToken` în toate operațiile asincrone și protejează durata de blocare a mesajului (`MaxAutoLockRenewalDuration`).
> 8. Niciun secret sau connection string în `appsettings.json` sau în cod (folosește User Secrets / variabile de mediu).

---

## Evaluare comparativă (V1 vs. V2)

| # | Cerință arhitecturală | V1 (Fără reguli) | V2 (Cu reguli) | Observații / Dovadă cod |
| :- | :--- | :---: | :---: | :--- |
| 1 | `Complete` **după** salvarea în DB | ✓ | ✓ | Ambele confirmă după `SaveAsync` / `CommitAsync` |
| 2 | Fără `catch` generic cu `Complete` | ✓ | ✓ | Blocurile `catch` fac `Abandon`, nu confirmă tăcut |
| 3 | Handler idempotent (Inbox / cheie) | Parțial | ✓ | V1 are *race condition* la concurență (`MaxConcurrentCalls = 2`); V2 folosește Inbox + tranzacție |
| 4 | Tranzitoriu → `Abandon` \| Permanent → `DLQ` | ✗ | ✓ | **Bug V1:** face `Abandon` pe JSON corupt; V2 trimite imediat în `DLQ` cu motiv |
| 5 | Outbox la producător | ✗ | ✓ | V1 trimite direct; V2 salvează `Order` + `OutboxMessage` atomic |
| 6 | Versiune pentru ordine | ✗ | ✓ | V1 nu are `Version`; V2 ignoră `evt.Version <= invoice.Version` |
| 7 | `CancellationToken` + Lock renewal | ✗ | ✓ | V1 ignoră `ct`; V2 transmite `ct` + `MaxAutoLockRenewalDuration = 10m` |
| 8 | Fără secrete în `appsettings.json` | ✗ | ✓ | V1 conține placeholder de connection string; V2 folosește `user-secrets` |
| **Total** | **Scor reguli respectate** | **2.5 / 8** | **8 / 8** | *(V2 conține însă eroarea de tip `DateDateTime?` în modele)* |

---

## Bug-uri reale identificate în codul generat de AI

1. **Mesaje otrăvite tratate ca erori tranzitorii (V1):** `JsonException` și erorile de validare sunt prinse într-un `catch (Exception)` generic care apelează `AbandonMessageAsync`, forțând brokerul să retrimită inutil un mesaj corupt până la epuizarea `MaxDeliveryCount`. Remediat în `V1ConsumerFixed` prin separarea `catch (JsonException)` → `DeadLetterMessageAsync`.
2. **Eroare de compilare și omisiune de domeniu (V2):** AI-ul a generat tipul inexistent `DateDateTime?` în `OutboxMessage.cs` și `InboxMessage.cs` și a eliminat lista de produse (`Items`) din modelul facturii.

---

## Structura repository-ului

- `OrderPlaced_AzureServiceBus_fara_reguli.md` — Promptul și răspunsul brut V1 (neschimbat)
- `OrderInvoiceService_AzureServiceBus_cu_regulti.md` — Promptul și răspunsul brut V2 (neschimbat)
- `ProvocareQ.Tests/` — Broker fals în memorie (`InMemoryPeekLockBroker` cu semantică *peek-lock*) + suita de 4 teste cu defecte injectate (duplicat, cădere înainte de `Complete`, ordine inversă, mesaj otrăvit) și demonstrația fix-ului (`V1ConsumerFixed`)
- `Prezentare-Provocarea-Q.pdf` — Prezentarea concluziilor (3 slide-uri)

---

## Rulare teste

```bash
dotnet run --project ProvocareQ.Tests
```