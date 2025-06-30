#Extract Params from system args
param (
    [int]$scid,
    [string]$metric_name,
    [string]$source,
    [string]$uri
)

#Get Current Date and Time from System
$time = (Get-Date -format "yyyy-MM-dd HH:mm:ss").ToString()

#Create HashTable
$hashtable = @{scid=$scid;t=$time;metric_name=$metric_name;source=$source}

#Convert to JSON
$json = $hashtable | ConvertTo-Json

#Send POST Request
Invoke-WebRequest -Uri $uri -Method POST -Body "$json" -Headers @{"Content-Type" = "application/json"}