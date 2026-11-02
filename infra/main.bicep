targetScope = 'resourceGroup'

@description('Review region, quota, service prices and budget before any deployment.')
param location string = 'australiaeast'
@minLength(3)
@maxLength(11)
param prefix string = 'mobilitylab'
@allowed(['dev'])
param environment string = 'dev'
@description('Explicit opt-in after user spending approval; false creates no resources.')
param deployLab bool = false
@description('Optional existing action group resource ID; empty means alerts have no notification receiver.')
param alertActionGroupId string = ''

// environment only allows dev: production is deliberately blocked.

var suffix = uniqueString(resourceGroup().id, prefix)
var tags = { project: 'urban-mobility-lakehouse', environment: environment, lifecycle: 'portfolio-lab' }
var blobReader = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
var blobContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')

resource lake 'Microsoft.Storage/storageAccounts@2023-05-01' = if (deployLab) {
  name: '${prefix}${suffix}'
  location: location
  tags: tags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    publicNetworkAccess: 'Enabled' // Authenticated public-endpoint LAB, never a production claim.
    networkAcls: { bypass: 'None', defaultAction: 'Allow' }
    accessTier: 'Hot'
  }
}
resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = if (deployLab) {
  parent: lake
  name: 'default'
  properties: { containerDeleteRetentionPolicy: { enabled: true, days: 7 } }
}
resource raw 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = if (deployLab) {
  parent: blob
  name: 'raw'
  properties: { publicAccess: 'None' }
}
resource processed 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = if (deployLab) {
  parent: blob
  name: 'processed'
  properties: { publicAccess: 'None' }
}
resource access 'Microsoft.Databricks/accessConnectors@2024-05-01' = if (deployLab) {
  name: '${prefix}-uc-${suffix}'
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {}
}
resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = if (deployLab) {
  name: '${prefix}-dbx-${suffix}'
  location: location
  tags: tags
  sku: { name: 'premium' }
  properties: {
    managedResourceGroupId: '${subscription().id}/resourceGroups/${prefix}-managed-${suffix}'
    publicNetworkAccess: 'Enabled'
    parameters: { enableNoPublicIp: { value: true } }
  }
}
resource factory 'Microsoft.DataFactory/factories@2018-06-01' = if (deployLab) {
  name: '${prefix}-adf-${suffix}'
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: { publicNetworkAccess: 'Enabled' }
}
resource adfRawWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (deployLab) {
  name: guid(raw.id, factory.id, blobContributor)
  scope: raw
  properties: { roleDefinitionId: blobContributor, principalId: factory!.identity.principalId, principalType: 'ServicePrincipal' }
}
resource databricksRawReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (deployLab) {
  name: guid(raw.id, access.id, blobReader)
  scope: raw
  properties: { roleDefinitionId: blobReader, principalId: access!.identity.principalId, principalType: 'ServicePrincipal' }
}
resource databricksProcessedWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (deployLab) {
  name: guid(processed.id, access.id, blobContributor)
  scope: processed
  properties: { roleDefinitionId: blobContributor, principalId: access!.identity.principalId, principalType: 'ServicePrincipal' }
}
resource http 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = if (deployLab) {
  parent: factory
  name: 'TlcPublicHttp'
  properties: { type: 'HttpServer', typeProperties: { url: 'https://d37ci6vzurychx.cloudfront.net/trip-data/', authenticationType: 'Anonymous', enableServerCertificateValidation: true } }
}
resource adls 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = if (deployLab) {
  parent: factory
  name: 'LakeManagedIdentity'
  properties: { type: 'AzureBlobFS', typeProperties: { url: 'https://${lake.name}.dfs.${az.environment().suffixes.storage}' } }
}
resource sourceDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = if (deployLab) {
  parent: factory
  name: 'TlcBinary'
  properties: {
    type: 'Binary'
    linkedServiceName: { type: 'LinkedServiceReference', referenceName: http.name }
    parameters: { sourceMonth: { type: 'String' } }
    typeProperties: { location: { type: 'HttpServerLocation', relativeUrl: { type: 'Expression', value: '@concat(\'yellow_tripdata_\',dataset().sourceMonth,\'.parquet\')' } } }
  }
}
resource rawDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = if (deployLab) {
  parent: factory
  name: 'RawBinary'
  properties: {
    type: 'Binary'
    linkedServiceName: { type: 'LinkedServiceReference', referenceName: adls.name }
    parameters: { sourceMonth: { type: 'String' }, runId: { type: 'String' } }
    typeProperties: { location: {
      type: 'AzureBlobFSLocation'
      fileSystem: 'raw'
      folderPath: { type: 'Expression', value: '@concat(\'yellow/\',dataset().sourceMonth,\'/\',dataset().runId)' }
      fileName: 'source.parquet'
    } }
  }
}
resource ingest 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = if (deployLab) {
  parent: factory
  name: 'RetainTlcSource'
  properties: {
    description: 'Manual raw capture only. Run transform after bootstrap; see docs/AZURE.md. No schedule.'
    concurrency: 1
    parameters: { sourceMonth: { type: 'String', defaultValue: '2024-01' } }
    activities: [
      {
        name: 'CheckMonth'
        type: 'IfCondition'
        typeProperties: {
          expression: { type: 'Expression', value: '@contains(createArray(\'2024-01\',\'2024-02\',\'2024-03\'),pipeline().parameters.sourceMonth)' }
          ifTrueActivities: [
            {
              name: 'CopyOriginalBytes'
              type: 'Copy'
              policy: { timeout: '0.00:20:00', retry: 2, retryIntervalInSeconds: 30 }
              inputs: [{ type: 'DatasetReference', referenceName: sourceDataset.name, parameters: { sourceMonth: { type: 'Expression', value: '@pipeline().parameters.sourceMonth' } } }]
              outputs: [{ type: 'DatasetReference', referenceName: rawDataset.name, parameters: { sourceMonth: { type: 'Expression', value: '@pipeline().parameters.sourceMonth' }, runId: { type: 'Expression', value: '@pipeline().RunId' } } }]
              typeProperties: {
                source: { type: 'BinarySource', storeSettings: { type: 'HttpReadSettings', requestMethod: 'GET', requestTimeout: '00:02:00' }, formatSettings: { type: 'BinaryReadSettings' } }
                sink: { type: 'BinarySink', storeSettings: { type: 'AzureBlobFSWriteSettings' } }
                enableStaging: false
                validateDataConsistency: true
              }
            }
          ]
          ifFalseActivities: [{ name: 'RejectMonth', type: 'Fail', typeProperties: { errorCode: 'SOURCE_MONTH_NOT_APPROVED', message: 'This lab permits 2024-01, 2024-02 and 2024-03 only.' } }]
        }
      }
    ]
  }
}
resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = if (deployLab) {
  name: '${prefix}-logs-${suffix}'
  location: location
  tags: tags
  properties: { sku: { name: 'PerGB2018' }, retentionInDays: 30, workspaceCapping: { dailyQuotaGb: 1 } }
}
resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (deployLab) {
  name: 'pipeline-audit'
  scope: factory
  properties: { workspaceId: logs.id, logAnalyticsDestinationType: 'Dedicated', logs: [{ category: 'PipelineRuns', enabled: true }, { category: 'ActivityRuns', enabled: true }] }
}
resource failures 'Microsoft.Insights/metricAlerts@2018-03-01' = if (deployLab) {
  name: '${prefix}-pipeline-failed'
  location: 'global'
  tags: tags
  properties: {
    description: 'ADF failures in 15 minutes. Configure action-group receiver explicitly before relying on notifications.'
    severity: 2
    enabled: true
    scopes: [factory.id]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: { 'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria', allOf: [{ name: 'PipelineFailure', metricNamespace: 'Microsoft.DataFactory/factories', metricName: 'PipelineFailedRuns', operator: 'GreaterThan', threshold: 0, timeAggregation: 'Total', criterionType: 'StaticThresholdCriterion' }] }
    actions: empty(alertActionGroupId) ? [] : [{ actionGroupId: alertActionGroupId }]
  }
}
output storageAccountName string = deployLab ? lake.name : 'not-deployed'
output factoryName string = deployLab ? factory.name : 'not-deployed'
output factoryPrincipalId string = deployLab ? factory!.identity.principalId : 'not-deployed'
output accessConnectorId string = deployLab ? access.id : 'not-deployed'
output databricksWorkspaceUrl string = deployLab ? workspace!.properties.workspaceUrl : 'not-deployed'
output rawUri string = deployLab ? 'abfss://raw@${lake.name}.dfs.${az.environment().suffixes.storage}/' : 'not-deployed'
output processedUri string = deployLab ? 'abfss://processed@${lake.name}.dfs.${az.environment().suffixes.storage}/' : 'not-deployed'
