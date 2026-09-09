using Pz.Connectors.TestKit;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config seeds it with the service-account key
/// material or a minted OAuth token.</summary>
public sealed class BqRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new BqRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
