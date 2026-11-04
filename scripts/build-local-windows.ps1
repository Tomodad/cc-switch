$ErrorActionPreference = 'Stop'
# Every native command is checked explicitly, including commands piped to Tee-Object.
function Invoke-Logged {
    param([string]$Log, [string]$Command, [string[]]$Arguments)
    & $Command @Arguments 2>&1 | Tee-Object -FilePath "validation/$Log"
    if ($LASTEXITCODE -ne 0) { throw "$Command failed ($LASTEXITCODE); see $Log" }
}
New-Item -ItemType Directory -Force validation, release-assets | Out-Null
Invoke-Logged 'typecheck.log' 'pnpm' @('typecheck')
Invoke-Logged 'frontend-format.log' 'pnpm' @('format:check')
# Avoid cross-file CPU contention on hosted Windows; preserve every test and timeout.
Invoke-Logged 'frontend-tests.log' 'pnpm' @('exec', 'vitest', 'run', '--maxWorkers=1', '--minWorkers=1')
Invoke-Logged 'frontend-build.log' 'pnpm' @('build:renderer')
Invoke-Logged 'rust-format.log' 'cargo' @('fmt', '--check', '--manifest-path', 'src-tauri/Cargo.toml')
Invoke-Logged 'clippy.log' 'cargo' @('clippy', '--locked', '--all-targets', '--manifest-path', 'src-tauri/Cargo.toml', '--', '-D', 'warnings')
Invoke-Logged 'backend-tests.log' 'cargo' @('nextest', 'run', '--locked', '--manifest-path', 'src-tauri/Cargo.toml', '--profile', 'ci')
Invoke-Logged 'doctests.log' 'cargo' @('test', '--locked', '--doc', '--manifest-path', 'src-tauri/Cargo.toml')
# Disable only updater artifact signing. Do not change the official updater endpoints/key.
'{"bundle":{"createUpdaterArtifacts":false}}' | Set-Content -Encoding utf8NoBOM local-unsigned.config.json
Invoke-Logged 'production-build.log' 'pnpm' @('tauri', 'build', '--bundles', 'nsis', '--config', 'local-unsigned.config.json')
$exe = Resolve-Path 'src-tauri/target/release/cc-switch.exe'
# Start-Process -Wait is essential for a Windows GUI-subsystem executable.
$process = Start-Process -FilePath $exe -ArgumentList '--verify-embedded-ui' -Wait -PassThru -NoNewWindow -RedirectStandardOutput 'validation/embedded-ui.json' -RedirectStandardError 'validation/embedded-ui.stderr.log'
if ($process.ExitCode -ne 0) { throw 'Embedded UI verification failed' }
$ui = Get-Content 'validation/embedded-ui.json' -Raw | ConvertFrom-Json
if (!$ui.passed -or !$ui.production_protocol) { throw 'Not a verified production UI' }
$installers = @(Get-ChildItem 'src-tauri/target/release/bundle/nsis/*-setup.exe')
if ($installers.Count -ne 1) { throw 'Expected exactly one NSIS installer' }
$installer = $installers[0]
Invoke-Logged 'installer-extract.log' '7z' @('x', $installer.FullName, '-oinstaller-check', '-y')
$payloads = @(Get-ChildItem installer-check -Recurse -Filter cc-switch.exe)
if ($payloads.Count -ne 1) { throw 'Expected exactly one application executable in installer' }
$exeHash = (Get-FileHash $exe -Algorithm SHA256).Hash
if ((Get-FileHash $payloads[0].FullName -Algorithm SHA256).Hash -ne $exeHash) { throw 'Installer payload differs from verified executable' }
@{passed=$true; executable_sha256=$exeHash.ToLower(); installer_sha256=(Get-FileHash $installer.FullName -Algorithm SHA256).Hash.ToLower()} | ConvertTo-Json | Set-Content -Encoding utf8NoBOM validation/installer-verification.json
Copy-Item $exe release-assets/cc-switch.exe
Copy-Item $installer.FullName release-assets/CC-Switch-4.0.25-x64-setup.exe
Invoke-Logged 'source-archive.log' 'git' @('archive', '--format=zip', '--output=release-assets/source.zip', 'HEAD')
# git writes the patch directly, avoiding Windows newline/encoding transformations.
Invoke-Logged 'source-patch.log' 'git' @('diff', '--binary', '--output=release-assets/local-patches-vs-official.patch', "$env:UPSTREAM_SHA..HEAD")
Invoke-Logged 'package-validation.log' 'python' @('scripts/validate-local-release.py')
