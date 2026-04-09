param(
    [string]$EnvFilePath = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($EnvFilePath)) {
    $EnvFilePath = Join-Path $repoRoot ".env.github"
}

$contractPath = Join-Path $repoRoot "docs\ops\env-contract.csv"

function Parse-EnvFile {
    param([string]$Path)

    $map = @{}
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or $line -notmatch "^([^=]+)=(.*)$") {
            continue
        }
        $map[$matches[1].Trim()] = $matches[2]
    }

    return $map
}

function Load-EnvContract {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        throw "Env contract not found at $Path"
    }

    $map = @{}
    foreach ($row in (Import-Csv -Path $Path)) {
        $name = (($row.name | Out-String).Trim())
        if ($name) {
            $map[$name] = $row
        }
    }

    return $map
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

function Invoke-GhRepoCommand {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$CaptureOutput
    )

    $repoSlug = Get-RepoSlug
    if ([string]::IsNullOrWhiteSpace($repoSlug)) {
        throw "Could not determine GitHub repository slug from remote.origin.url."
    }

    $fullArguments = @($Arguments + @("--repo", $repoSlug))
    if ($CaptureOutput) {
        return &(Get-Command gh -ErrorAction Stop).Source @fullArguments
    }

    & (Get-Command gh -ErrorAction Stop).Source @fullArguments
}

if (-not (Test-Path $EnvFilePath)) {
    throw "Env file not found at $EnvFilePath. Run scripts/setup-env.ps1 first."
}

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) is required to sync vars and secrets."
}

$envMap = Parse-EnvFile -Path $EnvFilePath
$contractMap = Load-EnvContract -Path $contractPath
$undocumented = @($envMap.Keys | Where-Object { -not $contractMap.ContainsKey($_) } | Sort-Object -Unique)
if ($undocumented.Count -gt 0) {
    throw "$EnvFilePath contains undocumented keys: $($undocumented -join ', ')"
}

$expectedVars = New-Object System.Collections.Generic.List[string]
$expectedSecrets = New-Object System.Collections.Generic.List[string]
foreach ($key in ($contractMap.Keys | Sort-Object)) {
    $entry = $contractMap[$key]
    $storage = (($entry.github_storage | Out-String).Trim()).ToLowerInvariant()
    if ($storage -notin @("var", "secret")) {
        continue
    }

    $value = if ($envMap.ContainsKey($key)) { $envMap[$key] } else { "" }
    if ($storage -eq "var") {
        $expectedVars.Add($key)
    }
    else {
        $expectedSecrets.Add($key)
    }

    if ([string]::IsNullOrWhiteSpace($value)) {
        Write-Host "Skipping empty ${storage}: $key" -ForegroundColor Yellow
        continue
    }

    if ($DryRun) {
        Write-Host "[DRY RUN] Would set ${storage}: $key"
        continue
    }

    if ($storage -eq "var") {
        Invoke-GhRepoCommand -Arguments @("variable", "set", $key, "--body", $value)
    }
    else {
        Invoke-GhRepoCommand -Arguments @("secret", "set", $key, "--body", $value)
    }

    Write-Host "Synced ${storage}: $key" -ForegroundColor Green
}

function Remove-UnexpectedItems {
    param([string]$Kind, [string[]]$Expected)

    $remote = @(Invoke-GhRepoCommand -Arguments @($Kind, "list", "--json", "name", "--jq", ".[].name") -CaptureOutput)
    $unexpected = @($remote | Where-Object { $_ -and $_ -notin $Expected } | Sort-Object -Unique)
    foreach ($name in $unexpected) {
        if ($DryRun) {
            Write-Host "[DRY RUN] Would delete unexpected ${Kind}: $name"
            continue
        }

        Invoke-GhRepoCommand -Arguments @($Kind, "delete", $name)
        Write-Host "Deleted unexpected ${Kind}: $name" -ForegroundColor Yellow
    }
}

Remove-UnexpectedItems -Kind "variable" -Expected $expectedVars.ToArray()
Remove-UnexpectedItems -Kind "secret" -Expected $expectedSecrets.ToArray()
