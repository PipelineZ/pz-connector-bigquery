using System.Text.RegularExpressions;

namespace Pz.Connector.BigQuery.Tests;

public sealed partial class BqCodesTests
{
    // 9 (01xx) + 7 (02xx) + 6 (03xx) + 9 (04xx) consts declared in BqCodes -- counted by hand and
    // kept in sync with BqCodes.All whenever a code is added or removed.
    private const int ExpectedCodeCount = 31;

    [Fact]
    public void All_codes_are_unique_and_match_the_PZBQ_shape()
    {
        Assert.Equal(ExpectedCodeCount, BqCodes.All.Count);
        Assert.Equal(BqCodes.All.Count, BqCodes.All.Distinct(StringComparer.Ordinal).Count());
        Assert.All(BqCodes.All, code => Assert.Matches(CodeShape(), code));
    }

    [Fact]
    public void Message_redacts_then_prefixes_with_the_code()
    {
        var redactor = new BqRedactor(["s3cret"]);

        Assert.Equal("bigquery: PZBQ0101: project *** required",
            BqCodes.Message(BqCodes.Config_ProjectRequired, redactor, "project s3cret required"));
    }

    [Fact]
    public void Message_still_masks_a_secret_that_contains_the_connector_prefix()
    {
        var redactor = new BqRedactor(["bigquery:secret"]);

        Assert.Equal("bigquery: PZBQ0101: token *** leaked",
            BqCodes.Message(BqCodes.Config_ProjectRequired, redactor, "token bigquery:secret leaked"));
    }

    [GeneratedRegex(@"^PZBQ0[1-4]\d\d$")]
    private static partial Regex CodeShape();
}
