'use strict';
// tslint:disable-next-line: variable-name
var SMCloudStore = {
    /**
     * Initializes a new client to interact with cloud providers' object storage services.
     *
     * @param provider - Name of the cloud provider to use (see `SMCloudStore.Providers`)
     * @param connection - Dictionary with connection options. List of keys is specific for every cloud provider
     * @returns An instance of a cloud provider module
     */
    Create: function (provider, connection) {
        // Validate arguments
        var supportedProviders = SMCloudStore.Providers();
        if (!provider || (typeof provider === 'string' && supportedProviders.indexOf(provider) < 0)) {
            throw Error('The specified provider is not valid. Valid providers inlcude: ' + supportedProviders.join(', '));
        }
        if (!connection) {
            throw Error('The connection argument must be non-empty');
        }
        // Require the specific provider, then initialize it
        var providerModule;
        if (typeof providerModule === "string")
            providerModule = require('@smcloudstore/' + provider);
        else
            providerModule = provider;
        return new providerModule(connection);
    },
    /**
     * Returns a list of supported providers.
     *
     * @returns List of supported provider names
     */
    Providers: function () {
        return [
            'aws-s3',
            'azure-storage',
            'backblaze-b2',
            'generic-s3',
            'google-cloud-storage',
            'minio'
        ];
    }
};
module.exports = SMCloudStore;
