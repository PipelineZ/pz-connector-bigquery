using Google.Apis.Auth.OAuth2;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Turns a parsed <see cref="BqConnectionConfig"/> into the credential its REST and gRPC
/// clients share. <c>None</c> yields no credential and no <c>Authorization</c> header at all -- the
/// emulator/proxy case spec §4 restricts to when <c>endpoint</c> is set. Every failure here is offline
/// (a bad key file or a broken ADC environment) rather than a network error, so all three are
/// non-transient.</summary>
internal static class BqAuth
{
    // bigquery covers the API surface itself; cloud-platform is needed by ADC-style credentials
    // (e.g. a GCE/GKE metadata-server identity) that were not minted with a narrower scope already.
    private static readonly string[] Scopes =
        ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"];

    public static GoogleCredential? Create(BqConnectionConfig config)
    {
        if (config.AuthKind == BqAuthKind.None)
        {
            return null;
        }

        var credential = config.AuthKind == BqAuthKind.ServiceAccount
            ? ServiceAccountCredential(config)
            : ApplicationDefaultCredential(config);

        return credential.IsCreateScopedRequired ? credential.CreateScoped(Scopes) : credential;
    }

    public static async Task<string?> AccessTokenAsync(GoogleCredential? credential, CancellationToken ct)
    {
        if (credential is null)
        {
            return null;
        }

        return await ((ITokenAccess)credential).GetAccessTokenForRequestAsync(null, ct).ConfigureAwait(false);
    }

    private static GoogleCredential ApplicationDefaultCredential(BqConnectionConfig config)
    {
        try
        {
            return GoogleCredential.GetApplicationDefault();
        }
        catch (Exception ex) when (ex is InvalidOperationException or IOException or UnauthorizedAccessException
            or AggregateException)
        {
            // GetApplicationDefault blocks on an async lookup, so failures can arrive wrapped in an
            // AggregateException; unwrap a single inner failure for the message.
            var cause = ex is AggregateException { InnerExceptions: [var inner] } ? inner : ex;
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Config_AdcNotResolved, config.Redactor,
                    $"could not resolve Application Default Credentials: {cause.Message} -- run "
                    + "'gcloud auth application-default login', set GOOGLE_APPLICATION_CREDENTIALS, or use 'service_account' auth"),
                isTransient: false, innerException: ex);
        }
    }

    private static GoogleCredential ServiceAccountCredential(BqConnectionConfig config)
    {
        // GoogleCredential.FromFile/FromJson are obsolete in favor of CredentialFactory (its type
        // parameter picks the credential kind directly, rather than sniffing the JSON's own "type"
        // field) -- key_file/key_json are always a service-account key by construction of the
        // service_account auth kind, so ServiceAccountCredential is the right type to ask for.
        if (config.KeyFile is { Length: > 0 } keyFile)
        {
            try
            {
                return CredentialFactory.FromFile<ServiceAccountCredential>(keyFile).ToGoogleCredential();
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
            {
                // Never the file's contents in the message (secret hygiene) -- the path plus the
                // loader's own reason is enough to act on.
                throw new PzConnectorException(
                    BqCodes.Message(BqCodes.Config_KeyFileNotLoaded, config.Redactor,
                        $"'key_file' could not be loaded from '{keyFile}': {ex.Message}"),
                    isTransient: false, innerException: ex);
            }
        }

        try
        {
            return CredentialFactory.FromJson<ServiceAccountCredential>(config.KeyJson).ToGoogleCredential();
        }
        catch (Exception ex)
        {
            // The parser throws serializer-specific exception types whose messages can echo input
            // fragments verbatim -- and this input is a private key, so the message is FIXED text:
            // neither the raw json nor the parser's own wording ever reaches the user-facing error.
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Config_KeyJsonInvalid, config.Redactor,
                    "'key_json' is not a valid service-account key (it must be the unmodified JSON key file "
                    + "downloaded from the Cloud Console)"),
                isTransient: false, innerException: ex);
        }
    }
}
