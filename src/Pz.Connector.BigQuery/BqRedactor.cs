using System.Text.RegularExpressions;

namespace Pz.Connector.BigQuery;

/// <summary>Strips credentials from any text that may reach a <c>PzConnectorException</c> message.
/// Every registered secret is replaced wherever it occurs, longest first -- a short secret that is a
/// substring of a longer one (an unqualified token inside a full key blob, say) would otherwise shred
/// the longer secret's remnant before its own replace ever runs. Beyond registered secrets, the
/// credential-bearing shapes BigQuery's REST/gRPC transports and Google's OAuth libraries print are
/// rewritten even when the value is not one of ours: an <c>Authorization: Bearer</c> header echoed
/// into a diagnostic, a service-account key's <c>private_key</c>/<c>private_key_id</c> JSON members,
/// an OAuth token query-string pair. Secrets shorter than 3 characters are not matched; replacing them
/// would shred unrelated text.</summary>
internal sealed partial class BqRedactor
{
    public const string Mask = "***";

    public static readonly BqRedactor None = new([]);

    // Swapped, never mutated in place: a concurrent Redact reads whatever reference it grabbed, and
    // array reference assignment is atomic, so it never sees a torn array.
    private string[] _secrets;
    private readonly object _gate = new();

    public BqRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = Sorted(secrets, []);
    }

    /// <summary>Registers a secret minted after construction -- an OAuth access token fetched
    /// mid-run, say. Thread-safe: concurrent callers may mint and register tokens at once. A no-op
    /// on <see cref="None"/>, which every caller not wired up with a real redactor (most tests,
    /// notably) shares as one process-wide instance -- letting it accumulate secrets would leak
    /// state between unrelated tests and requests.</summary>
    public void AddSecret(string secret)
    {
        if (ReferenceEquals(this, None))
        {
            return;
        }

        lock (_gate)
        {
            _secrets = Sorted([secret], _secrets);
        }
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
        }

        text = AuthorizationBearerHeader().Replace(text, m => $"{m.Groups["key"].Value} {Mask}");
        text = PrivateKeyMember().Replace(text, m => $"{m.Groups["prefix"].Value}{Mask}\"");
        return TokenPair().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
    }

    private static string[] Sorted(IEnumerable<string> incoming, IEnumerable<string> existing) =>
        incoming.Concat(existing).Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();

    // "Authorization: Bearer <token>" as the transport's own diagnostics and HttpClient logging echo
    // it; the token runs to the next whitespace.
    [GeneratedRegex("""(?<key>\bAuthorization:)\s+Bearer\s+\S+""", RegexOptions.IgnoreCase)]
    private static partial Regex AuthorizationBearerHeader();

    // "private_key": "..." / "private_key_id": "..." -- the two secret members of a service-account
    // JSON key. Only the value is masked; the key name (captured together with its quotes and
    // separator, so original spacing survives) stays, so a redacted key blob is still recognizable in
    // a log. The value alternation tolerates escaped characters (\n for the key's embedded newlines,
    // \" for a literal quote) without ending the match early. Written as an escaped (not raw) string
    // literal because the pattern's own trailing '"' would otherwise sit flush against a raw string's
    // closing delimiter.
    [GeneratedRegex("(?<prefix>\"private_key(?:_id)?\"\\s*:\\s*\")(?:[^\"\\\\]|\\\\.)*\"")]
    private static partial Regex PrivateKeyMember();

    // access_token=/refresh_token=/client_secret= as OAuth endpoints and client libraries print them
    // in a query string or form body; '&', ';', ',' end the value so a following parameter's name is
    // never swallowed into the match.
    [GeneratedRegex("""(?<key>\b(?:access_token|refresh_token|client_secret)\b)=(?:"[^"]*"|[^\s;,&]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex TokenPair();
}
