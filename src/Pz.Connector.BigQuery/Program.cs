using Pz.Connector.BigQuery;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new BqConnector(ctx.LoggerFactory)).ConfigureAwait(false);
