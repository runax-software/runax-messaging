using System.Text;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Tests;

public class MessageHeadersTests
{
    [Fact]
    public void Headers_are_ordered_contract_first_then_trace_context_then_by_name()
    {
        var headers = new Dictionary<string, string>
        {
            ["zeta"] = "z",
            [MessageHeaders.TraceState] = "ts",
            ["alpha"] = "a",
            [MessageHeaders.ContractVersion] = "2",
            [MessageHeaders.TraceParent] = "tp",
            [MessageHeaders.ContractName] = "orders",
        };

        MessageHeaders.InPriorityOrder(headers).Select(h => h.Key).ShouldBe(
        [
            MessageHeaders.ContractName,
            MessageHeaders.ContractVersion,
            MessageHeaders.TraceParent,
            MessageHeaders.TraceState,
            "alpha",
            "zeta",
        ]);
    }

    [Fact]
    public void Broker_header_values_are_formatted_as_strings()
    {
        MessageHeaders.FormatValue("text").ShouldBe("text");
        MessageHeaders.FormatValue(Encoding.UTF8.GetBytes("bytes")).ShouldBe("bytes");
        MessageHeaders.FormatValue(true).ShouldBe("true");
        MessageHeaders.FormatValue(42).ShouldBe("42");
        MessageHeaders.FormatValue(1.5).ShouldBe("1.5");
        MessageHeaders.FormatValue(null).ShouldBeNull();
        MessageHeaders.FormatValue(new List<object>()).ShouldBeNull();
    }
}
