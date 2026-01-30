if (-not $env:GRIST_API_KEY) {
    Write-Error "GRIST_API_KEY environment variable is not set."
    exit 1
}

$headers = @{
    "Authorization" = "Bearer $env:GRIST_API_KEY"
    "Content-Type" = "application/json"
}

try {
    $response = Invoke-RestMethod -Uri "https://docs.getgrist.com/api/orgs" -Method Get -Headers $headers
    $response | Format-Table -Property id, name, domain
}
catch {
    Write-Error $_.Exception.Message
}
