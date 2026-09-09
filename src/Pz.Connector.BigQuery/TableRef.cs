using System.Text;
using System.Text.RegularExpressions;

namespace Pz.Connector.BigQuery;

/// <summary>A fully-resolved BigQuery table address. Parsed once from an entity name plus
/// the connection's default project, then carried everywhere a table identity is needed: REST paths
/// (<see cref="ResourcePath"/>) and generated SQL (<see cref="Quoted"/>). Charsets match what BigQuery
/// itself accepts for each part -- project ids may carry the domain-scoped <c>:</c>, dataset ids are
/// restricted to <c>[A-Za-z0-9_]</c>, and table ids additionally allow <c>-</c> and Unicode letters.</summary>
internal readonly partial record struct TableRef(string Project, string Dataset, string Table)
{
    /// <summary><c>` p `.` d `.` t `</c> with every backtick and backslash inside each part escaped --
    /// generated SQL always addresses a table this way so the identifier is used byte-for-byte rather
    /// than reparsed by DuckDB or BigQuery's own SQL lexer.</summary>
    public string Quoted => $"{QuoteIdentifier(Project)}.{QuoteIdentifier(Dataset)}.{QuoteIdentifier(Table)}";

    /// <summary>The REST resource path for this table, relative to a project-scoped API base.</summary>
    public string ResourcePath => $"projects/{Project}/datasets/{Dataset}/tables/{Table}";

    private static readonly Regex ProjectPattern = ProjectRegex();
    private static readonly Regex DatasetPattern = DatasetRegex();
    private static readonly Regex TablePattern = TableRegex();

    /// <summary>Backtick-quotes a single identifier part: <c>`</c> becomes <c>\`</c> and <c>\</c>
    /// becomes <c>\\</c>, so the escaped backslash can never be mistaken for the start of an escape
    /// sequence it did not introduce.</summary>
    public static string QuoteIdentifier(string part)
    {
        var sb = new StringBuilder(part.Length + 2);
        sb.Append('`');
        foreach (var c in part)
        {
            if (c is '`' or '\\')
            {
                sb.Append('\\');
            }

            sb.Append(c);
        }

        sb.Append('`');
        return sb.ToString();
    }

    /// <summary>Parses an entity name naming a table: <c>dataset.table</c> (using
    /// <paramref name="defaultProject"/>) or <c>project.dataset.table</c>. Any other shape,
    /// or a backtick anywhere in the text, is refused.</summary>
    public static bool TryParse(string entity, string defaultProject, out TableRef result, out string? error)
    {
        if (!TrySplit(entity, out var parts, out error))
        {
            result = default;
            return false;
        }

        string project, dataset, table;
        switch (parts.Length)
        {
            case 2:
                project = defaultProject;
                dataset = parts[0];
                table = parts[1];
                break;
            case 3:
                project = parts[0];
                dataset = parts[1];
                table = parts[2];
                break;
            default:
                result = default;
                error = $"'{entity}' is not a valid table name; use 'dataset.table' or 'project.dataset.table'";
                return false;
        }

        if (!ValidateParts(entity, project, dataset, table, out error))
        {
            result = default;
            return false;
        }

        result = new TableRef(project, dataset, table);
        error = null;
        return true;
    }

    /// <summary>Parses an entity name naming a dataset (no table): <c>dataset</c> (using
    /// <paramref name="defaultProject"/>) or <c>project.dataset</c> -- <c>staging_dataset</c>'s shape.
    /// Stored as a <see cref="TableRef"/> with <see cref="Table"/> set to <c>""</c>.</summary>
    public static bool TryParseDataset(string text, string defaultProject, out TableRef result, out string? error)
    {
        if (!TrySplit(text, out var parts, out error))
        {
            result = default;
            return false;
        }

        string project, dataset;
        switch (parts.Length)
        {
            case 1:
                project = defaultProject;
                dataset = parts[0];
                break;
            case 2:
                project = parts[0];
                dataset = parts[1];
                break;
            default:
                result = default;
                error = $"'{text}' is not a valid dataset name; use 'dataset' or 'project.dataset'";
                return false;
        }

        if (!ValidatePart("project", text, project, ProjectPattern, out error)
            || !ValidatePart("dataset", text, dataset, DatasetPattern, out error))
        {
            result = default;
            return false;
        }

        result = new TableRef(project, dataset, "");
        error = null;
        return true;
    }

    private static bool TrySplit(string text, out string[] parts, out string? error)
    {
        if (text.Contains('`', StringComparison.Ordinal))
        {
            parts = [];
            error = "backticks are not accepted; write dataset.table or project.dataset.table";
            return false;
        }

        parts = text.Split('.');
        error = null;
        return true;
    }

    private static bool ValidateParts(string entity, string project, string dataset, string table, out string? error)
    {
        if (!ValidatePart("project", entity, project, ProjectPattern, out error))
        {
            return false;
        }

        if (!ValidatePart("dataset", entity, dataset, DatasetPattern, out error))
        {
            return false;
        }

        return ValidatePart("table", entity, table, TablePattern, out error);
    }

    private static bool ValidatePart(string label, string entity, string value, Regex pattern, out string? error)
    {
        if (pattern.IsMatch(value))
        {
            error = null;
            return true;
        }

        error = $"'{entity}' has an invalid {label} id '{value}'";
        return false;
    }

    [GeneratedRegex("^[a-z0-9.:-]+$")]
    private static partial Regex ProjectRegex();

    [GeneratedRegex("^[A-Za-z0-9_]+$")]
    private static partial Regex DatasetRegex();

    [GeneratedRegex(@"^[\p{L}\p{N}_-]+$")]
    private static partial Regex TableRegex();
}
