using System.Security.Cryptography;
using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqAuthTests
{
    private static BqConnectionConfig Config(BqAuthKind kind, string? keyFile = null, string? keyJson = null) =>
        new("my-proj", kind, keyFile, keyJson, null, null, BqConnectionConfig.DefaultRestBase, null, false, BqRedactor.None);

    // A syntactically valid, throwaway service-account key: GoogleCredential.FromJson only needs a
    // key that parses and signs -- it never calls out to Google, so no real credential is required
    // for this offline test.
    private static string GenerateServiceAccountKeyJson()
    {
        using var rsa = RSA.Create(2048);
        var privateKeyPem = rsa.ExportPkcs8PrivateKeyPem();

        return JsonSerializer.Serialize(new
        {
            type = "service_account",
            project_id = "my-proj",
            private_key_id = "abc123",
            private_key = privateKeyPem,
            client_email = "test@my-proj.iam.gserviceaccount.com",
            client_id = "123456789",
            token_uri = "https://oauth2.googleapis.com/token",
        });
    }

    [Fact]
    public async Task None_auth_yields_no_credential_and_no_token()
    {
        var credential = BqAuth.Create(Config(BqAuthKind.None));
        Assert.Null(credential);

        var token = await BqAuth.AccessTokenAsync(credential, CancellationToken.None);
        Assert.Null(token);
    }

    [Fact]
    public void Service_account_with_a_syntactically_valid_key_yields_a_credential()
    {
        var keyJson = GenerateServiceAccountKeyJson();

        var credential = BqAuth.Create(Config(BqAuthKind.ServiceAccount, keyJson: keyJson));

        Assert.NotNull(credential);
        // Deliberately not calling BqAuth.AccessTokenAsync here: minting a token would perform a
        // real network call against oauth2.googleapis.com, which this offline test must not do.
    }

    [Fact]
    public void Garbage_key_json_raises_PZBQ0108_without_echoing_the_input()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => BqAuth.Create(Config(BqAuthKind.ServiceAccount, keyJson: "not-json-at-all-{{{")));

        Assert.Contains("PZBQ0108", ex.Message);
        Assert.DoesNotContain("not-json-at-all", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Missing_key_file_raises_PZBQ0107_naming_the_path()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.json");

        var ex = Assert.Throws<PzConnectorException>(
            () => BqAuth.Create(Config(BqAuthKind.ServiceAccount, keyFile: missingPath)));

        Assert.Contains("PZBQ0107", ex.Message);
        Assert.Contains(missingPath, ex.Message);
        Assert.False(ex.IsTransient);
    }
}
