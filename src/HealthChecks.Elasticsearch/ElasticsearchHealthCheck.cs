using Elasticsearch.Net;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Nest;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace HealthChecks.Elasticsearch
{
    public class ElasticsearchHealthCheck
        : IHealthCheck
    {
        private static readonly ConcurrentDictionary<string, ElasticClient> _connections = new ConcurrentDictionary<string, ElasticClient>();

        private readonly ElasticsearchOptions _options;

        public ElasticsearchHealthCheck(ElasticsearchOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                var connectionKey = _options.Uri ?? _options.CloudId;
                if (!_connections.TryGetValue(connectionKey, out ElasticClient lowLevelClient))
                {
                    ConnectionSettings settings = new ConnectionSettings();
                    if (!string.IsNullOrEmpty(_options.Uri))
                    {
                        settings = new ConnectionSettings(new Uri(_options.Uri));

                        if (_options.AuthenticateWithBasicCredentials)
                        {
                            settings = settings.BasicAuthentication(_options.UserName, _options.Password);
                        }
                        else if (_options.AuthenticateWithCertificate)
                        {
                            settings = settings.ClientCertificate(_options.Certificate);
                        }
                    }
                    else if (!string.IsNullOrEmpty(_options.CloudId))
                    {
                        if (_options.AuthenticateWithBasicCredentials)
                        {
                            var credentials = new BasicAuthenticationCredentials(_options.UserName, _options.Password);
                            settings = new ConnectionSettings(new CloudConnectionPool(_options.CloudId, credentials));
                        }
                        else if (_options.AuthenticateWithApiKey)
                        {
                            var credentials = new ApiKeyAuthenticationCredentials(_options.ApiKey);
                            settings = new ConnectionSettings(new CloudConnectionPool(_options.CloudId, credentials));
                        }
                    }

                    if (_options.RequestTimeout.HasValue)
                    {
                        settings = settings.RequestTimeout(_options.RequestTimeout.Value);
                    }

                    if (_options.CertificateValidationCallback != null)
                    {
                        settings = settings.ServerCertificateValidationCallback(_options.CertificateValidationCallback);
                    }

                    lowLevelClient = new ElasticClient(settings);
                    if (!_connections.TryAdd(connectionKey, lowLevelClient))
                    {
                        lowLevelClient = _connections[connectionKey];
                    }
                }

                var pingResult = await lowLevelClient.PingAsync(ct: cancellationToken);
                var isSuccess = pingResult.ApiCall.HttpStatusCode == 200;

                return isSuccess
                    ? HealthCheckResult.Healthy()
                    : new HealthCheckResult(context.Registration.FailureStatus);
            }
            catch (Exception ex)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, exception: ex);
            }
        }
    }
}