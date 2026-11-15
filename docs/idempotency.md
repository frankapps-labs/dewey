# Idempotency: creation, retries, and external effects

Dewey provides **at-least-once execution**, not exactly-once effects. These are
three separate contracts:

1. **Creating work:** `create_or_get_task(..., idempotency_key=...)` deduplicates a
   business request within a task type. Reusing a key with different creation
   inputs raises `IdempotencyConflictError`; it does not silently replace work.
2. **Writing the ledger:** execution/dispatch generations fence stale outcomes.
   They cannot undo an email, charge, or other external effect.
3. **Applying an effect:** the application and receiving system must durably
   deduplicate a stable operation key, or use a transactional business/outbox
   design. A local flag written after an external call is not sufficient.

## Stable handler context

Django, synchronous/asynchronous SQLAlchemy executors, and the native runtime
provide `dewey.current_task()` while running an admitted handler. No signature or
serialized argument changes are required. Direct calls outside an executor raise
`RuntimeError`; awaited async work inherits context. Do not detach background work
past the handler lifetime or assume new threads inherit it.

```python
import dewey

@dewey.task("payments.capture", max_attempts=3)
def capture(payment_reference: str):
    context = dewey.current_task()
    provider.capture(
        payment_reference,
        idempotency_key=context.effect_key("capture"),
    )
```

`TaskContext` contains `task_id`, `task_type`, and the admitted `attempt` number.
`effect_key(operation)` returns a stable 64-character hexadecimal key derived from
**task identity and operation name only**, with a fixed v1 derivation. It stays the
same across retries, worker replacement and sync/async execution. Names are 1–64
ASCII letters/digits/`._-`, starting with a letter. Use a different stable name for
each effect, e.g. `capture` and `receipt-email`. Do not rename an operation while
pending tasks may still retry. Never use attempt, worker identity or dispatch token
as the dedupe key: those change on retry.

The provider must actually support durable keyed deduplication; check its key
length, account/operation scope, retention window and replayed-response semantics.
When a provider commits an effect but its reply or the ledger outcome is lost,
the retry must return the original receipt rather than apply the effect again.
This also applies when an old handler survives a parent crash. Process containment
reduces overlap but cannot undo an effect already applied before termination.

Keys identify a **task row**, not arbitrary equivalent business requests. Separate
rows have separate keys. Use creation-time idempotency or a business-level provider
key when distinct task rows represent the same logical operation. When custom task
IDs or shared provider accounts cross databases/environments, ensure identity is
unique in the provider's namespace. Application-side database effects can instead
use a unique operation receipt in the same transaction as the business write.

Do not claim idempotency merely because duplicate handler invocations were ignored
in memory: verify durable receipt identity and business non-mutation after a real
lost-response/crash retry.
