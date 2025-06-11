@echo off

set "uri=%~1"
set "scid=%~2"
set "t=%~3"
:: Define variables
set "URI=http://%uri%"
set "BODY={\"scid\": \"%scid%\", \"t\": \"%t%\", \"metric_name\": \"XCD\"}"
:: Call PowerShell with Invoke-WebRequest
PowerShell -Command "Invoke-WebRequest -Uri '%URI%' -Method POST -Headers @{'Content-Type'='application/json'} -Body '%BODY%'"