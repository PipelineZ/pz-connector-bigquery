namespace Pz.Connector.BigQuery;

/// <summary>PZBQ error codes (spec §9). Every user-facing <c>PzConnectorException</c> message has the
/// shape <c>bigquery: PZBQ####: &lt;redacted text&gt;</c>. Codes are grouped by the failure surface
/// that raises them: 01xx connection config (validated before any network call), 02xx read, 03xx
/// write, 04xx remote (whatever the BigQuery service or the transport itself reports back).</summary>
internal static class BqCodes
{
    // 01xx -- connection config: raised validating connections.yml, before any request is sent.
    public const string Config_ProjectRequired = "PZBQ0101";
    public const string Config_AuthRequired = "PZBQ0102";
    public const string Config_ServiceAccountKeyExactlyOne = "PZBQ0103";
    public const string Config_AuthNoneNeedsEndpoint = "PZBQ0104";
    public const string Config_QueryModeNeedsStagingDataset = "PZBQ0105";
    public const string Config_AdcNotResolved = "PZBQ0106";
    public const string Config_KeyFileNotLoaded = "PZBQ0107";
    public const string Config_KeyJsonInvalid = "PZBQ0108";
    public const string Config_Invalid = "PZBQ0109";

    // 02xx -- read: raised compiling or opening a source.
    public const string Read_BadEntityName = "PZBQ0201";
    public const string Read_CursorColumnNotInSchema = "PZBQ0202";
    public const string Read_UnsupportedCursorType = "PZBQ0203";
    public const string Read_TableIsView = "PZBQ0204";
    public const string Read_TableNotFound = "PZBQ0205";
    public const string Read_PermissionDenied = "PZBQ0206";
    public const string Read_BadDatasetOption = "PZBQ0207";

    // 03xx -- write: raised compiling or opening a sink.
    public const string Write_MergeKeyMissing = "PZBQ0301";
    public const string Write_ReservedColumnSeq = "PZBQ0302";
    public const string Write_UnsupportedArrowType = "PZBQ0303";
    public const string Write_SchemaMismatch = "PZBQ0304";
    public const string Write_SchemaEvolveUnsupported = "PZBQ0305";
    public const string Write_BadWriteMode = "PZBQ0306";

    // 04xx -- remote: raised classifying a REST, job, or gRPC failure the service or transport
    // reported back. NotFound/PermissionDenied from the Storage Read API (gRPC) land on the 02xx
    // read codes instead (0205/0206) -- that transport is read-only, so its errors are read errors.
    public const string Remote_Transient = "PZBQ0401";
    public const string Remote_TransientService = "PZBQ0402";
    public const string Remote_QuotaExceeded = "PZBQ0403";
    public const string Remote_Unauthenticated = "PZBQ0404";
    public const string Remote_PermissionDenied = "PZBQ0405";
    public const string Remote_NotFound = "PZBQ0406";
    public const string Remote_InvalidQuery = "PZBQ0407";
    public const string Remote_GrpcTransient = "PZBQ0408";
    public const string Remote_Other = "PZBQ0409";

    /// <summary>Every code above, kept in sync by hand: reflection over the consts would need trimmer
    /// annotations to survive Native AOT, for a list that changes only when a code is added.</summary>
    internal static readonly IReadOnlyList<string> All =
    [
        Config_ProjectRequired, Config_AuthRequired, Config_ServiceAccountKeyExactlyOne, Config_AuthNoneNeedsEndpoint,
        Config_QueryModeNeedsStagingDataset, Config_AdcNotResolved, Config_KeyFileNotLoaded, Config_KeyJsonInvalid, Config_Invalid,

        Read_BadEntityName, Read_CursorColumnNotInSchema, Read_UnsupportedCursorType, Read_TableIsView, Read_TableNotFound,
        Read_PermissionDenied, Read_BadDatasetOption,

        Write_MergeKeyMissing, Write_ReservedColumnSeq, Write_UnsupportedArrowType, Write_SchemaMismatch,
        Write_SchemaEvolveUnsupported, Write_BadWriteMode,

        Remote_Transient, Remote_TransientService, Remote_QuotaExceeded, Remote_Unauthenticated, Remote_PermissionDenied,
        Remote_NotFound, Remote_InvalidQuery, Remote_GrpcTransient, Remote_Other,
    ];

    /// <summary>Redaction runs before the prefix goes on, so a secret that happens to contain
    /// "bigquery:" cannot shred the one part of the message that is ours.</summary>
    public static string Message(string code, BqRedactor redactor, string text) => $"bigquery: {code}: {redactor.Redact(text)}";
}
