param(
    [string]$Version,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$pyprojectPath = Join-Path $repoRoot "python\pyproject.toml"
$contractPath = Join-Path $repoRoot "docs\architecture\architecture-contract.md"
$touchedFiles = @(
    "python/pyproject.toml",
    "docs/architecture/architecture-contract.md"
)

function Read-RequiredFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required file not found: $Path"
    }

    return Get-Content -LiteralPath $Path -Raw
}

function Write-Utf8File {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Content
    )

    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

function Get-CurrentVersion {
    param([Parameter(Mandatory = $true)][string]$Content)

    $match = [regex]::Match($Content, '(?m)^version = "(?<version>\d+\.\d+\.\d+)"\r?$')
    if (-not $match.Success) {
        throw "Could not locate the package version in python/pyproject.toml."
    }

    return $match.Groups["version"].Value
}

function Replace-ExactTarget {
    param(
        [Parameter(Mandatory = $true)][string]$Content,
        [Parameter(Mandatory = $true)][string]$OldValue,
        [Parameter(Mandatory = $true)][string]$NewValue,
        [Parameter(Mandatory = $true)][string]$Description
    )

    $matchCount = [regex]::Matches($Content, [regex]::Escape($OldValue)).Count
    if ($matchCount -ne 1) {
        throw "Expected exactly one $Description target, found $matchCount."
    }

    return $Content.Replace($OldValue, $NewValue)
}

function Get-SingleRegexMatch {
    param(
        [Parameter(Mandatory = $true)][string]$Content,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][string]$Description
    )

    $matches = [regex]::Matches($Content, $Pattern)
    if ($matches.Count -ne 1) {
        throw "Expected exactly one $Description target, found $($matches.Count)."
    }

    return $matches[0]
}

$pyprojectContent = Read-RequiredFile -Path $pyprojectPath
$contractContent = Read-RequiredFile -Path $contractPath
$currentVersion = Get-CurrentVersion -Content $pyprojectContent

if ([string]::IsNullOrWhiteSpace($Version)) {
    Write-Output "Current version: $currentVersion"
    $Version = Read-Host "Enter new version (X.Y.Z)"
}

if ([string]::IsNullOrWhiteSpace($Version)) {
    throw "Version is required."
}

if ($Version -notmatch '^\d+\.\d+\.\d+$') {
    throw "Version '$Version' must match stable semver X.Y.Z."
}

if ($Version -eq $currentVersion) {
    throw "Version '$Version' matches the current package version."
}

$pyprojectOldLine = 'version = "{0}"' -f $currentVersion
$pyprojectNewLine = 'version = "{0}"' -f $Version
$contractBaselineMatch = Get-SingleRegexMatch `
    -Content $contractContent `
    -Pattern '(?m)^(?<prefix>\| Canonical Baseline \| `main` branch, package version `)(?<version>\d+\.\d+\.\d+)(?<suffix>` in `python/pyproject\.toml` \|)\r?$' `
    -Description "architecture contract canonical baseline line"
$contractBaselineVersion = $contractBaselineMatch.Groups["version"].Value
$contractOldLine = $contractBaselineMatch.Value
$contractNewLine = '{0}{1}{2}' -f `
    $contractBaselineMatch.Groups["prefix"].Value, `
    $Version, `
    $contractBaselineMatch.Groups["suffix"].Value

if ($contractBaselineVersion -ne $currentVersion) {
    Write-Warning "Architecture contract canonical baseline version '$contractBaselineVersion' does not match python/pyproject.toml version '$currentVersion'. Continuing with the requested release version."
}

$updatedPyproject = Replace-ExactTarget `
    -Content $pyprojectContent `
    -OldValue $pyprojectOldLine `
    -NewValue $pyprojectNewLine `
    -Description "python/pyproject.toml version line"

$updatedContract = Replace-ExactTarget `
    -Content $contractContent `
    -OldValue $contractOldLine `
    -NewValue $contractNewLine `
    -Description "architecture contract canonical baseline line"

if (-not $DryRun) {
    Write-Utf8File -Path $pyprojectPath -Content $updatedPyproject
    Write-Utf8File -Path $contractPath -Content $updatedContract
}

$mode = if ($DryRun) { "dry-run" } else { "updated" }
Write-Output "Mode: $mode"
Write-Output "Current version: $currentVersion"
Write-Output "New version: $Version"
Write-Output "Touched files:"
foreach ($path in $touchedFiles) {
    Write-Output "- $path"
}
