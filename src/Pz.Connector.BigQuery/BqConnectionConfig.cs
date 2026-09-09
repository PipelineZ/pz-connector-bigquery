using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Which credential BigQuery calls carry. <c>None</c> sends no <c>Authorization</c> header
/// at all -- meaningful only against an emulator or an authenticating proxy, never against Google.</summary>
internal enum BqAuthKind { ServiceAccount, Adc, None }

/// <summary>The typed connection surface (spec §4). <see cref="RestBase"/> always ends with <c>/</c>
/// so request paths compose relatively against it (the RFC 3986 lesson: a path-bearing base URL is
/// clobbered by a leading-slash request path). <see cref="StagingDataset"/> is a <see cref="TableRef"/>
/// with <see cref="TableRef.Table"/> set to <c>""</c> -- a dataset has no table part, and reusing
/// <see cref="TableRef"/> means its <c>Quoted</c>/<c>ResourcePath</c> machinery does not need a second
/// copy for the dataset-only shape.</summary>
internal sealed partial record BqConnectionConfig(
    string Project,
    BqAuthKind AuthKind,
    string? KeyFile,
    string? KeyJson,
    string? Location,
    TableRef? StagingDataset,
    Uri RestBase,
    string? StorageEndpoint,
    bool PlaintextStorage,
    BqRedactor Redactor)
{
    public static readonly Uri DefaultRestBase = new("https://bigquery.googleapis.com/");

    private static readonly string[] KnownKeys =
        ["project", "auth", "key_file", "key_json", "location", "staging_dataset", "endpoint", "storage_endpoint", "base_dir"];

    private static readonly string[] AuthKinds = ["service_account", "adc", "none"];

    public static BqConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys.Where(k => k != "base_dir"))}");
        }

        var project = ParseProject(config, errors);

        var authText = config.GetString("auth");
        BqAuthKind? authKind = ParseAuthKind(authText, errors);

        var keyFile = ResolveKeyFile(config);
        var keyJson = config.GetString("key_json");
        if (!string.IsNullOrEmpty(keyJson))
        {
            secrets.Add(keyJson);
        }

        var endpointText = config.GetString("endpoint");
        var restBase = ParseEndpoint(endpointText, errors);

        if (authKind == BqAuthKind.ServiceAccount)
        {
            var hasFile = !string.IsNullOrEmpty(keyFile);
            var hasJson = !string.IsNullOrEmpty(keyJson);
            if (hasFile == hasJson)
            {
                errors.Add(hasFile
                    ? "'service_account' auth takes 'key_file' or 'key_json', not both"
                    : "'service_account' auth requires 'key_file' or 'key_json'");
            }
        }
        else if (authKind == BqAuthKind.None && string.IsNullOrEmpty(endpointText))
        {
            errors.Add("'auth: none' is only valid with 'endpoint' set (an emulator or authenticating proxy); "
                + "use service_account or adc against Google");
        }

        string? location = null;
        if (config.Values.ContainsKey("location"))
        {
            location = config.GetString("location");
            if (string.IsNullOrEmpty(location))
            {
                errors.Add("'location' must not be empty");
                location = null;
            }
        }

        TableRef? stagingDataset = null;
        var stagingText = config.GetString("staging_dataset");
        if (!string.IsNullOrEmpty(stagingText))
        {
            if (TableRef.TryParseDataset(stagingText, project ?? "", out var parsed, out var stagingError))
            {
                stagingDataset = parsed;
            }
            else
            {
                errors.Add($"'staging_dataset': {stagingError}");
            }
        }

        var storageEndpoint = ParseStorageEndpoint(config.GetString("storage_endpoint"), errors);

        if (errors.Count != start || project is null || authKind is null || restBase is null)
        {
            return null;
        }

        return new BqConnectionConfig(
            project,
            authKind.Value,
            authKind == BqAuthKind.ServiceAccount ? keyFile : null,
            authKind == BqAuthKind.ServiceAccount ? keyJson : null,
            location,
            stagingDataset,
            restBase,
            storageEndpoint,
            restBase.Scheme == "http",
            new BqRedactor(secrets));
    }

    private static string? ParseProject(ConnectorConfig config, List<string> errors)
    {
        var project = config.GetString("project");
        if (string.IsNullOrEmpty(project))
        {
            errors.Add("'project' is required (billing project and default project for 2-part entity names)");
            return null;
        }

        if (!ProjectPattern().IsMatch(project))
        {
            errors.Add($"'project' must match [a-z0-9.:-]+ (got '{project}')");
            return null;
        }

        return project;
    }

    private static BqAuthKind? ParseAuthKind(string? authText, List<string> errors)
    {
        if (string.IsNullOrEmpty(authText))
        {
            errors.Add($"'auth' is required (one of: {string.Join(", ", AuthKinds)})");
            return null;
        }

        if (!AuthKinds.Contains(authText, StringComparer.Ordinal))
        {
            errors.Add($"'auth' must be one of {string.Join(", ", AuthKinds)} (got '{authText}')");
            return null;
        }

        return authText switch
        {
            "service_account" => BqAuthKind.ServiceAccount,
            "adc" => BqAuthKind.Adc,
            _ => BqAuthKind.None,
        };
    }

    private static string? ResolveKeyFile(ConnectorConfig config)
    {
        var keyFile = config.GetString("key_file");
        if (string.IsNullOrEmpty(keyFile))
        {
            return null;
        }

        var baseDir = config.GetString("base_dir");
        return baseDir is not null && !Path.IsPathRooted(keyFile) ? Path.Combine(baseDir, keyFile) : keyFile;
    }

    private static Uri? ParseEndpoint(string? endpointText, List<string> errors)
    {
        if (string.IsNullOrEmpty(endpointText))
        {
            return DefaultRestBase;
        }

        if (!Uri.TryCreate(endpointText, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
        {
            errors.Add($"'endpoint' must be an absolute http or https URL; got '{endpointText}'");
            return null;
        }

        if (uri.AbsolutePath.EndsWith('/'))
        {
            return uri;
        }

        var builder = new UriBuilder(uri) { Path = uri.AbsolutePath + "/" };
        return builder.Uri;
    }

    private static string? ParseStorageEndpoint(string? text, List<string> errors)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        if (!StorageEndpointPattern().IsMatch(text))
        {
            errors.Add($"'storage_endpoint' must be 'host:port' with no scheme and no path (got '{text}')");
            return null;
        }

        var port = int.Parse(text[(text.LastIndexOf(':') + 1)..], System.Globalization.CultureInfo.InvariantCulture);
        if (port is < 1 or > 65535)
        {
            errors.Add($"'storage_endpoint' port must be between 1 and 65535 (got '{text}')");
            return null;
        }

        return text;
    }

    [GeneratedRegex("^[a-z0-9.:-]+$")]
    private static partial Regex ProjectPattern();

    // host:port only -- no "scheme://" prefix and no "/path" suffix. The host itself may not contain
    // ':' (that would be a second, ambiguous port separator) or '/'.
    [GeneratedRegex(@"^[^\s:/]+:\d{1,5}$")]
    private static partial Regex StorageEndpointPattern();
}
