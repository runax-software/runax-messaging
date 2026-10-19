namespace Runax.Messaging.AotSmoke;

/// <summary>Completes once each consumer has handled its message.</summary>
internal sealed class Probe
{
    public TaskCompletionSource<OrderPlaced> Orders { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource<InvoiceIssued> Invoices { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}

internal sealed class OrderPlacedConsumer(Probe probe) : MessageConsumer<OrderPlaced>
{
    private int _attempts;

    public override string Topic => "orders";

    protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        // Fail the first delivery so the retry pipeline runs too.
        if (Interlocked.Increment(ref _attempts) == 1)
            throw new InvalidOperationException("Simulated transient failure.");

        probe.Orders.TrySetResult(message);
        return ValueTask.CompletedTask;
    }
}

internal sealed class InvoiceIssuedConsumer(Probe probe) : MessageConsumer<InvoiceIssued>
{
    public override string Topic => "invoices";

    protected override ValueTask HandleAsync(InvoiceIssued message, CancellationToken cancellationToken = default)
    {
        probe.Invoices.TrySetResult(message);
        return ValueTask.CompletedTask;
    }
}
