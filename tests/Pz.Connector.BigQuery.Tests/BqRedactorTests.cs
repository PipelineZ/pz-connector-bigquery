namespace Pz.Connector.BigQuery.Tests;

public sealed class BqRedactorTests
{
    [Fact]
    public void Replaces_every_occurrence_of_every_registered_secret_longest_first()
    {
        var redactor = new BqRedactor(["ya29.abc", "abc"]);

        // "abc" is a substring of "ya29.abc" -- shortest-first would shred the longer secret's
        // remnant into an un-matchable "ya29.***" before the longer replace ever runs.
        Assert.Equal("token *** short ***", redactor.Redact("token ya29.abc short abc"));
    }

    [Fact]
    public void Ignores_secrets_shorter_than_three_characters()
    {
        var redactor = new BqRedactor(["", "ab"]);

        Assert.Equal("ab", redactor.Redact("ab"));
    }

    [Fact]
    public void None_is_identity()
    {
        Assert.Equal("x", BqRedactor.None.Redact("x"));
    }

    [Fact]
    public void AddSecret_on_None_is_a_no_op()
    {
        // None is one process-wide shared instance -- if AddSecret mutated it, one test's or one
        // request's token would leak into every other caller that also uses BqRedactor.None.
        // "secret-xyz" (>= 3 chars) so this cannot pass merely because Sorted() would have dropped
        // a too-short secret anyway.
        BqRedactor.None.AddSecret("secret-xyz");

        Assert.Equal("holds secret-xyz here", BqRedactor.None.Redact("holds secret-xyz here"));
    }

    [Fact]
    public void Masks_a_registered_key_json_blob_wherever_it_is_echoed()
    {
        var keyJson = """{"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEv...\n-----END PRIVATE KEY-----\n"}""";
        var redactor = new BqRedactor([keyJson]);

        Assert.DoesNotContain("BEGIN PRIVATE KEY", redactor.Redact($"key rejected: {keyJson}"), StringComparison.Ordinal);
    }

    [Fact]
    public void Rewrites_bearer_authorization_headers_even_for_unregistered_tokens()
    {
        var redactor = BqRedactor.None;

        Assert.Equal("Authorization: ***", redactor.Redact("Authorization: Bearer ya29.a0Af-secret-token"));
    }

    [Fact]
    public void Rewrites_private_key_and_private_key_id_members_keeping_the_key_name()
    {
        var redactor = BqRedactor.None;
        var text = """{"private_key_id":"abcd1234","private_key":"-----BEGIN PRIVATE KEY-----\nline1\nline2\n-----END PRIVATE KEY-----\n"}""";

        Assert.Equal("""{"private_key_id":"***","private_key":"***"}""", redactor.Redact(text));
    }

    [Fact]
    public void Rewrites_oauth_token_pairs_even_for_unregistered_values()
    {
        var redactor = BqRedactor.None;

        Assert.Equal("cb?access_token=***&refresh_token=***&client_secret=***&other=1",
            redactor.Redact("cb?access_token=abc.def&refresh_token=xyz123&client_secret=s3cr3t&other=1"));
    }

    [Fact]
    public void AddSecret_masks_tokens_minted_after_construction_and_is_thread_safe()
    {
        var redactor = new BqRedactor([]);

        Parallel.For(0, 50, i => redactor.AddSecret($"minted-token-{i}"));

        Assert.DoesNotContain("minted-token-7", redactor.Redact("holder minted-token-7 done"), StringComparison.Ordinal);
        Assert.DoesNotContain("minted-token-42", redactor.Redact("holder minted-token-42 done"), StringComparison.Ordinal);
    }
}
