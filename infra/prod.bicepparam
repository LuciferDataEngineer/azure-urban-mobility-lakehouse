using './main.bicep'
// Deliberately fails the allowed-value contract: private endpoints, DNS, VNet injection,
// governed egress and service access tests must be implemented before a production deployment.
param environment = 'prod'
param deployLab = false
