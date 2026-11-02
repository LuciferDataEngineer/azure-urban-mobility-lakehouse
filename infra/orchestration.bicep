targetScope = 'resourceGroup'
// SECOND PHASE: after approval, UC bootstrap, notebook import and job creation.
param factoryName string
param workspaceName string
param databricksJobId string
param catalog string = 'mobility'
param schema string = 'yellow'

resource factory 'Microsoft.DataFactory/factories@2018-06-01' existing = { name: factoryName }
resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' existing = { name: workspaceName }
resource databricks 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'DatabricksManagedIdentity'
  properties: {
    type: 'AzureDatabricks'
    typeProperties: {
      domain: 'https://${workspace.properties.workspaceUrl}'
      authentication: 'MSI'
      workspaceResourceId: workspace.id
    }
  }
}
resource pipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: factory
  name: 'CaptureAndPublishMonth'
  properties: {
    description: 'No trigger; serial month capture followed by serialized Databricks job.'
    concurrency: 1
    parameters: {
      sourceMonth: { type: 'String', defaultValue: '2024-01' }
      allowRevision: { type: 'String', defaultValue: 'false' }
    }
    activities: [
      {
        name: 'CaptureRaw'
        type: 'ExecutePipeline'
        typeProperties: {
          pipeline: { type: 'PipelineReference', referenceName: 'RetainTlcSource' }
          waitOnCompletion: true
          parameters: { sourceMonth: { type: 'Expression', value: '@pipeline().parameters.sourceMonth' } }
        }
      }
      {
        name: 'PublishDelta'
        type: 'DatabricksJob'
        dependsOn: [{ activity: 'CaptureRaw', dependencyConditions: ['Succeeded'] }]
        linkedServiceName: { type: 'LinkedServiceReference', referenceName: databricks.name }
        policy: { timeout: '0.01:00:00', retry: 1, retryIntervalInSeconds: 60 }
        typeProperties: {
          jobId: databricksJobId
          jobParameters: {
            catalog: catalog
            schema: schema
            source_month: { type: 'Expression', value: '@pipeline().parameters.sourceMonth' }
            allow_revision: { type: 'Expression', value: '@pipeline().parameters.allowRevision' }
            source_uri: {
              type: 'Expression'
              value: '@concat(\'/Volumes/${catalog}/${schema}/raw/yellow/\',pipeline().parameters.sourceMonth,\'/\',activity(\'CaptureRaw\').output.pipelineRunId,\'/source.parquet\')'
            }
          }
        }
      }
    ]
  }
}
output pipelineName string = pipeline.name
