param(
    [string]$EnvFilePath = "",
    [switch]$DryRun,
    [string[]]$Set = @(),
    [string]$DispatchAppPrivateKeyPath = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($EnvFilePath)) {
    $EnvFilePath = Join-Path $repoRoot ".env.github"
}

$contractPath = Join-Path $repoRoot "docs\ops\env-contract.csv"
$templatePath = Join-Path $repoRoot ".env.template"

function Parse-EnvFile {
    param([string]$Path)

    $map = @{}
    if (-not (Test-Path $Path)) {
        return $map
    }

    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or $line -notmatch "^([^=]+)=(.*)$") {
            continue
        }
        $map[$matches[1].Trim()] = $matches[2]
    }

    return $map
}

function Load-ContractRows {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        throw "Env contract not found at $Path"
    }

    return @(Import-Csv -Path $Path | Where-Object { $_.template -eq "true" -and $_.github_storage -in @("var", "secret") })
}

function ConvertFrom-SecureStringPlain {
    param([Parameter(Mandatory = $true)][System.Security.SecureString]$Secure)

    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

function Normalize-EnvValue {
    param([AllowNull()][string]$Value)

    if ($null -eq $Value) {
        return ""
    }

    return $Value.Replace("`r", "").Replace("`n", "\n")
}

function Trim-TrailingLineBreaks {
    param([AllowNull()][string]$Value)

    if ($null -eq $Value) {
        return ""
    }

    return $Value.TrimEnd([char[]]@("`r", "`n"))
}

function Get-RepoSlug {
    try {
        $remote = (& git -C $repoRoot config --get remote.origin.url 2>$null | Out-String).Trim()
    }
    catch {
        $remote = ""
    }

    if ($remote -match "github\.com[:/](?<owner>[^/]+)/(?<repo>[^/.]+)(?:\.git)?$") {
        return "$($matches['owner'])/$($matches['repo'])"
    }

    return ""
}

function Get-RepoOwner {
    $repoSlug = Get-RepoSlug
    if ($repoSlug -match "^(?<owner>[^/]+)/(?<repo>.+)$") {
        return $matches["owner"]
    }

    return ""
}

function New-Resolution {
    param([string]$Value = "", [string]$Source = "default", [bool]$PromptRequired = $false)

    return @{
        Value = (Normalize-EnvValue -Value $Value)
        Source = $Source
        PromptRequired = $PromptRequired
    }
}

$overrideMap = @{}
foreach ($entry in $Set) {
    if ($entry -match "^([^=]+)=(.*)$") {
        $overrideMap[$matches[1].Trim()] = $matches[2]
    }
}

$existingMap = Parse-EnvFile -Path $EnvFilePath
$templateMap = Parse-EnvFile -Path $templatePath
$contractRows = Load-ContractRows -Path $contractPath

function Resolve-DiscoveredValue {
    param([string]$Key)

    $owner = Get-RepoOwner
    if ([string]::IsNullOrWhiteSpace($owner)) {
        return (New-Resolution)
    }

    switch ($Key) {
        "CONTROL_PLANE_REPOSITORY" {
            return (New-Resolution -Value "$owner/asset-allocation-control-plane" -Source "git")
        }
        "JOBS_REPOSITORY" {
            return (New-Resolution -Value "$owner/asset-allocation-jobs" -Source "git")
        }
    }

    return (New-Resolution)
}

function Prompt-PlainValue {
    param([string]$Name, [string]$Suggestion = "", [string]$Description = "")

    if ($Description) {
        Write-Host "# $Description" -ForegroundColor DarkGray
    }

    $input = Read-Host "$Name [$Suggestion]"
    if ([string]::IsNullOrWhiteSpace($input)) {
        return $Suggestion
    }

    return $input
}

function Prompt-SecretValue {
    param([string]$Name, [string]$Description = "")

    if ($Description) {
        Write-Host "# $Description" -ForegroundColor DarkGray
    }

    $secure = Read-Host "$Name [secret]" -AsSecureString
    return (ConvertFrom-SecureStringPlain -Secure $secure)
}

function Resolve-ReadableFilePath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $candidates = New-Object System.Collections.Generic.List[string]
    $candidates.Add($Path)
    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        $candidates.Add((Join-Path $repoRoot $Path))
    }

    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (Test-Path -LiteralPath $candidate -PathType Leaf) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    throw "File not found at path '$Path'."
}

function Read-SecretFileValue {
    param([Parameter(Mandatory = $true)][string]$Path)

    $resolvedPath = Resolve-ReadableFilePath -Path $Path
    $content = Get-Content -LiteralPath $resolvedPath -Raw
    return (Trim-TrailingLineBreaks -Value $content)
}

function Prompt-SecretFileValue {
    param([string]$Name, [string]$Description = "")

    if ($Description) {
        Write-Host "# $Description" -ForegroundColor DarkGray
    }

    $path = Read-Host "$Name file path"
    if ([string]::IsNullOrWhiteSpace($path)) {
        return ""
    }

    return (Read-SecretFileValue -Path $path)
}

$results = New-Object System.Collections.Generic.List[object]
$dispatchPrivateKeyResolution = $null
if (-not [string]::IsNullOrWhiteSpace($DispatchAppPrivateKeyPath)) {
    $dispatchPrivateKeyResolution = New-Resolution -Value (Read-SecretFileValue -Path $DispatchAppPrivateKeyPath) -Source "file"
}

foreach ($row in $contractRows) {
    $name = $row.name
    $description = (($row.notes | Out-String).Trim())
    $isSecret = $row.github_storage -eq "secret"
    $defaultValue = if ($templateMap.ContainsKey($name)) { Normalize-EnvValue -Value $templateMap[$name] } else { "" }

    if ($name -eq "DISPATCH_APP_PRIVATE_KEY" -and $null -ne $dispatchPrivateKeyResolution) {
        $results.Add([pscustomobject]@{
                Name = $name
                Value = $dispatchPrivateKeyResolution.Value
                Source = $dispatchPrivateKeyResolution.Source
                IsSecret = $true
                PromptRequired = $false
            })
        continue
    }

    if ($existingMap.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($existingMap[$name])) {
        $results.Add([pscustomobject]@{
                Name = $name
                Value = (Normalize-EnvValue -Value $existingMap[$name])
                Source = "existing"
                IsSecret = $isSecret
                PromptRequired = $false
            })
        continue
    }

    if ($overrideMap.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($overrideMap[$name])) {
        $results.Add([pscustomobject]@{
                Name = $name
                Value = (Normalize-EnvValue -Value $overrideMap[$name])
                Source = "prompted"
                IsSecret = $isSecret
                PromptRequired = $false
            })
        continue
    }

    if (-not $isSecret) {
        $discovered = Resolve-DiscoveredValue -Key $name
        if (-not [string]::IsNullOrWhiteSpace($discovered.Value)) {
            $results.Add([pscustomobject]@{
                    Name = $name
                    Value = $discovered.Value
                    Source = $discovered.Source
                    IsSecret = $false
                    PromptRequired = $false
                })
            continue
        }

        if ($DryRun) {
            $results.Add([pscustomobject]@{
                    Name = $name
                    Value = $defaultValue
                    Source = "default"
                    IsSecret = $false
                    PromptRequired = $true
                })
            continue
        }

        $value = Prompt-PlainValue -Name $name -Suggestion $defaultValue -Description $description
        $source = if ([string]::IsNullOrWhiteSpace($value) -or $value -eq $defaultValue) { "default" } else { "prompted" }
        $results.Add([pscustomobject]@{
                Name = $name
                Value = (Normalize-EnvValue -Value $value)
                Source = $source
                IsSecret = $false
                PromptRequired = $false
            })
        continue
    }

    if ($DryRun) {
        $results.Add([pscustomobject]@{
                Name = $name
                Value = $defaultValue
                Source = "default"
                IsSecret = $true
                PromptRequired = $true
            })
        continue
    }

    $secretValue = if ($name -eq "DISPATCH_APP_PRIVATE_KEY") {
        Prompt-SecretFileValue -Name $name -Description $description
    }
    else {
        Prompt-SecretValue -Name $name -Description $description
    }

    $secretSource = if ([string]::IsNullOrWhiteSpace($secretValue)) {
        "default"
    }
    elseif ($name -eq "DISPATCH_APP_PRIVATE_KEY") {
        "file"
    }
    else {
        "prompted"
    }

    $results.Add([pscustomobject]@{
            Name = $name
            Value = (Normalize-EnvValue -Value $secretValue)
            Source = $secretSource
            IsSecret = $true
            PromptRequired = $false
        })
}

$lines = foreach ($result in $results) {
    "{0}={1}" -f $result.Name, $result.Value
}

Write-Host "Target env file: $EnvFilePath" -ForegroundColor Cyan
foreach ($result in $results) {
    $displayValue = if ($result.IsSecret -and -not [string]::IsNullOrWhiteSpace($result.Value)) { "<redacted>" } else { $result.Value }
    Write-Host ("{0}={1} [source={2}; prompt_required={3}]" -f $result.Name, $displayValue, $result.Source, $result.PromptRequired.ToString().ToLowerInvariant())
}

if ($DryRun) {
    Write-Host ""
    Write-Host "# Preview (.env.github)" -ForegroundColor Cyan
    foreach ($result in $results) {
        $displayValue = if ($result.IsSecret -and -not [string]::IsNullOrWhiteSpace($result.Value)) { "<redacted>" } else { $result.Value }
        Write-Host ("{0}={1}" -f $result.Name, $displayValue)
    }
    return
}

Set-Content -Path $EnvFilePath -Value $lines -Encoding utf8
Write-Host "Wrote $EnvFilePath" -ForegroundColor Green
