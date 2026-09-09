using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqConnectionConfigTests
{
    private static ConnectorConfig Config(Dictionary<string, object?> values) => new(values);

    [Fact]
    public void Minimal_service_account_config_defaults_the_rest_base_and_scheme()
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "my-proj",
            ["auth"] = "service_account",
            ["key_file"] = "sa.json",
        }), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal("my-proj", config.Project);
        Assert.Equal(BqAuthKind.ServiceAccount, config.AuthKind);
        Assert.Equal("sa.json", config.KeyFile);
        Assert.Null(config.KeyJson);
        Assert.Equal(new Uri("https://bigquery.googleapis.com/"), config.RestBase);
        Assert.False(config.PlaintextStorage);
        Assert.Null(config.StorageEndpoint);
        Assert.Null(config.StagingDataset);
    }

    [Fact]
    public void Project_is_required_non_empty_and_charset_restricted()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["auth"] = "adc" }), errors));
        Assert.Contains(errors, e => e.Contains("'project' is required"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["project"] = "MyProj", ["auth"] = "adc" }), errors));
        Assert.Contains(errors, e => e.Contains("'project' must match"));

        errors.Clear();
        var ok = BqConnectionConfig.Parse(Config(new() { ["project"] = "my-proj.eu:1", ["auth"] = "adc" }), errors);
        Assert.Empty(errors);
        Assert.Equal("my-proj.eu:1", ok!.Project);
    }

    [Fact]
    public void Auth_is_required_and_restricted_to_the_three_kinds()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["project"] = "p" }), errors));
        Assert.Contains(errors, e => e.Contains("'auth' is required"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["project"] = "p", ["auth"] = "oauth" }), errors));
        Assert.Contains(errors, e => e.Contains("'auth' must be one of service_account, adc, none (got 'oauth')"));
    }

    [Fact]
    public void Service_account_needs_exactly_one_of_key_file_or_key_json()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["project"] = "p", ["auth"] = "service_account" }), errors));
        Assert.Contains(errors, e => e.Contains("requires 'key_file' or 'key_json'"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "service_account", ["key_file"] = "a.json", ["key_json"] = "{}",
        }), errors));
        Assert.Contains(errors, e => e.Contains("not both"));
    }

    [Fact]
    public void Adc_with_no_extra_keys_parses_cleanly()
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(Config(new() { ["project"] = "p", ["auth"] = "adc" }), errors);

        Assert.Empty(errors);
        Assert.Equal(BqAuthKind.Adc, config!.AuthKind);
        Assert.Null(config.KeyFile);
        Assert.Null(config.KeyJson);
    }

    [Fact]
    public void Adc_rejects_key_file_and_or_key_json()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["key_file"] = "sa.json",
        }), errors));
        Assert.Contains(errors, e => e.Contains("'adc' auth takes no further keys") && e.Contains("'key_file'"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["key_json"] = "{}",
        }), errors));
        Assert.Contains(errors, e => e.Contains("'adc' auth takes no further keys") && e.Contains("'key_json'"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["key_file"] = "sa.json", ["key_json"] = "{}",
        }), errors));
        Assert.Contains(errors, e => e.Contains("'adc' auth takes no further keys")
            && e.Contains("'key_file'") && e.Contains("'key_json'"));
    }

    [Fact]
    public void None_without_endpoint_is_refused()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["project"] = "p", ["auth"] = "none" }), errors));

        Assert.Contains(errors, e => e.Contains("'auth: none' is only valid with 'endpoint' set"));
    }

    [Fact]
    public void None_with_endpoint_is_accepted()
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "none", ["endpoint"] = "http://localhost:9050",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal(BqAuthKind.None, config!.AuthKind);
    }

    [Fact]
    public void Location_if_present_must_be_non_empty()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["project"] = "p", ["auth"] = "adc", ["location"] = "" }), errors));
        Assert.Contains(errors, e => e.Contains("'location' must not be empty"));

        errors.Clear();
        var config = BqConnectionConfig.Parse(Config(new() { ["project"] = "p", ["auth"] = "adc", ["location"] = "EU" }), errors);
        Assert.Empty(errors);
        Assert.Equal("EU", config!.Location);
    }

    [Fact]
    public void Endpoint_http_normalizes_trailing_slash_and_sets_plaintext_storage()
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "none", ["endpoint"] = "http://localhost:9050",
        }), errors);

        Assert.Empty(errors);
        Assert.EndsWith("/", config!.RestBase.ToString());
        Assert.True(config.PlaintextStorage);
    }

    [Fact]
    public void Endpoint_with_a_path_keeps_the_path_and_adds_a_trailing_slash()
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "none", ["endpoint"] = "https://proxy/api/v2",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal("/api/v2/", config!.RestBase.AbsolutePath);
        Assert.False(config.PlaintextStorage);
    }

    [Fact]
    public void Endpoint_must_be_absolute_http_or_https()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["endpoint"] = "ftp://host",
        }), errors));

        Assert.Contains(errors, e => e.Contains("absolute http or https URL"));
    }

    [Fact]
    public void Storage_endpoint_must_be_host_colon_port_with_no_scheme_or_path()
    {
        var errors = new List<string>();
        var ok = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["storage_endpoint"] = "localhost:9060",
        }), errors);
        Assert.Empty(errors);
        Assert.Equal("localhost:9060", ok!.StorageEndpoint);

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["storage_endpoint"] = "http://localhost:9060",
        }), errors));
        Assert.Contains(errors, e => e.Contains("'storage_endpoint' must be 'host:port'"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["storage_endpoint"] = "localhost:9060/path",
        }), errors));
        Assert.Contains(errors, e => e.Contains("'storage_endpoint' must be 'host:port'"));

        errors.Clear();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["storage_endpoint"] = "localhost:99999",
        }), errors));
        Assert.Contains(errors, e => e.Contains("port must be between 1 and 65535"));
    }

    [Fact]
    public void Staging_dataset_parses_as_project_qualified_or_bare_dataset()
    {
        var errors = new List<string>();
        var qualified = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["staging_dataset"] = "other.pz_staging",
        }), errors);
        Assert.Empty(errors);
        Assert.Equal(new TableRef("other", "pz_staging", ""), qualified!.StagingDataset);

        errors.Clear();
        var bare = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "adc", ["staging_dataset"] = "pz_staging",
        }), errors);
        Assert.Empty(errors);
        Assert.Equal(new TableRef("p", "pz_staging", ""), bare!.StagingDataset);
    }

    [Fact]
    public void Relative_key_file_resolves_against_base_dir_and_rooted_paths_stay()
    {
        var errors = new List<string>();
        var relative = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "service_account", ["key_file"] = "secrets/sa.json", ["base_dir"] = "/proj",
        }), errors);
        Assert.Empty(errors);
        Assert.Equal(Path.Combine("/proj", "secrets/sa.json"), relative!.KeyFile);

        errors.Clear();
        var rooted = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "service_account", ["key_file"] = "/abs/sa.json", ["base_dir"] = "/proj",
        }), errors);
        Assert.Equal("/abs/sa.json", rooted!.KeyFile);
    }

    [Fact]
    public void Key_json_is_registered_with_the_redactor()
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(Config(new()
        {
            ["project"] = "p", ["auth"] = "service_account", ["key_json"] = "top-secret-blob",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal("key ***", config!.Redactor.Redact("key top-secret-blob"));
    }

    [Fact]
    public void Unknown_keys_are_refused_naming_the_known_ones_without_base_dir()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new() { ["region"] = "eu" }), errors));

        var unknownError = Assert.Single(errors, e => e.StartsWith("unknown connection key 'region'", StringComparison.Ordinal));
        Assert.DoesNotContain("base_dir", unknownError);
    }

    [Fact]
    public void Errors_aggregate_across_multiple_rules()
    {
        var errors = new List<string>();
        Assert.Null(BqConnectionConfig.Parse(Config(new()
        {
            ["auth"] = "bogus", ["location"] = "", ["storage_endpoint"] = "bad",
        }), errors));

        Assert.True(errors.Count >= 4); // project missing, auth invalid, location empty, storage_endpoint bad
    }
}
