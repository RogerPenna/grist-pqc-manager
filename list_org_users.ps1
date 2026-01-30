if (-not $env:GRIST_API_KEY) {
    Write-Error "GRIST_API_KEY environment variable is not set."
    exit 1
}

$orgId = "54594"
$headers = @{
    "Authorization" = "Bearer $env:GRIST_API_KEY"
    "Content-Type" = "application/json"
}

try {
    # Using the /orgs/{orgId}/access endpoint to get members
    $response = Invoke-RestMethod -Uri "https://docs.getgrist.com/api/orgs/$orgId/access" -Method Get -Headers $headers
    
    # The response usually contains a 'users' property or similar depending on the exact API version
    # Accessing the 'users' list directly from the response
    if ($response.users) {
        $response.users | Select-Object id, name, email, access | Format-Table
    } else {
        $response | Format-List
    }
}
catch {
    Write-Error $_.Exception.Message
}
