# Povocarea Q — AI scrie consumatorul: e idempotent sau doar compilează?

- **Domeniu:** `OrderPlaced` -> Emitere Factură
- **Model testat:** ChatGPT (GPT-5), 22.11.2026

## Structura repository-ului
- `OrderPlaced_AzureServiceBus_fara_reguli.md` — Promptul și răspunsul AI fără reguli (neschimbat)
- `OrderInvoiceService_AzureServiceBus_cu_regulti.md` — Promptul și răspunsul AI cu cele 8 reguli din Cursul 7 (neschimbat)
- `ProvocareQ.Tests/` — Broker fals (`InMemoryPeekLockBroker`) + cele 4 teste cu defecte injectate și demonstrația fix-ului (`V1ConsumerFixed`)
- `Prezentare-Provocarea-Q.pdf` — Prezentarea (3 slide-uri)

## Rulare teste
```bash
dotnet run --project ProvocareQ.Tests
```