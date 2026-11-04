param([ValidateSet('all', 'frontend', 'backend', 'build', 'verify')][string]$Stage = 'all')
$ErrorActionPreference = 'Stop'
# Every native command is checked explicitly, including commands piped to Tee-Object.
function Invoke-Logged {
    param([string]$Log, [string]$Command, [string[]]$Arguments)
    & $Command @Arguments 2>&1 | Tee-Object -FilePath "validation/$Log"
    if ($LASTEXITCODE -ne 0) { throw "$Command failed ($LASTEXITCODE); see $Log" }
}
New-Item -ItemType Directory -Force validation, release-assets | Out-Null
if ($Stage -in @('all', 'frontend')) {
Invoke-Logged 'typecheck.log' 'pnpm' @('typecheck')
Invoke-Logged 'frontend-format.log' 'pnpm' @('format:check')
# Avoid cross-file CPU contention on hosted Windows; preserve every test and timeout.
Invoke-Logged 'frontend-tests.log' 'pnpm' @('exec', 'vitest', 'run', '--maxWorkers=1', '--minWorkers=1')
Invoke-Logged 'frontend-build.log' 'pnpm' @('build:renderer')
}
if ($Stage -in @('all', 'backend')) {
Invoke-Logged 'rust-format.log' 'cargo' @('fmt', '--check', '--manifest-path', 'src-tauri/Cargo.toml')
Invoke-Logged 'clippy.log' 'cargo' @('clippy', '--locked', '--all-targets', '--manifest-path', 'src-tauri/Cargo.toml', '--', '-D', 'warnings')
Invoke-Logged 'backend-tests.log' 'cargo' @('nextest', 'run', '--locked', '--manifest-path', 'src-tauri/Cargo.toml', '--profile', 'ci')
Invoke-Logged 'doctests.log' 'cargo' @('test', '--locked', '--doc', '--manifest-path', 'src-tauri/Cargo.toml')
}
if ($Stage -in @('all', 'build')) {
# Disable only updater artifact signing. Do not change the official updater endpoints/key.
'{"bundle":{"createUpdaterArtifacts":false}}' | Set-Content -Encoding utf8NoBOM local-unsigned.config.json
Invoke-Logged 'production-build.log' 'pnpm' @('tauri', 'build', '--bundles', 'nsis', '--config', 'local-unsigned.config.json')
}
if ($Stage -in @('all', 'verify')) {
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
# The bundler changes exactly one UNK marker to NSS inside the installer, then
# restores the standalone build. Compare every byte with only that known change.
Invoke-Logged 'payload-comparator-tests.log' 'python' @('scripts/test_compare_nsis_payload.py')
Invoke-Logged 'payload-comparison.log' 'python' @('scripts/compare_nsis_payload.py', $exe.Path, $payloads[0].FullName, 'validation/payload-comparison.json')
# Validate the actual installer payload independently, without installing or launching the app.
$process = Start-Process -FilePath $payloads[0].FullName -ArgumentList '--verify-embedded-ui' -Wait -PassThru -NoNewWindow -RedirectStandardOutput 'validation/payload-embedded-ui.json' -RedirectStandardError 'validation/payload-embedded-ui.stderr.log'
if ($process.ExitCode -ne 0) { throw 'Installer payload embedded UI verification failed' }
$payloadUi = Get-Content 'validation/payload-embedded-ui.json' -Raw | ConvertFrom-Json
if (!$payloadUi.passed -or !$payloadUi.production_protocol) { throw 'Installer payload is not a verified production UI' }
# Publish this exact verified payload, so the downloaded EXE and installer agree.
Copy-Item $payloads[0].FullName release-assets/cc-switch.exe
$payloadHash = (Get-FileHash $payloads[0].FullName -Algorithm SHA256).Hash.ToLower()
$publishedHash = (Get-FileHash release-assets/cc-switch.exe -Algorithm SHA256).Hash.ToLower()
if ($publishedHash -ne $payloadHash) { throw 'Published executable differs from installer payload' }
@{passed=$true; executable_sha256=$publishedHash; payload_executable_sha256=$payloadHash; build_executable_sha256=(Get-FileHash $exe -Algorithm SHA256).Hash.ToLower(); installer_sha256=(Get-FileHash $installer.FullName -Algorithm SHA256).Hash.ToLower(); payload_ui_verified=$true; published_payload_identical=$true} | ConvertTo-Json | Set-Content -Encoding utf8NoBOM validation/installer-verification.json
Copy-Item $installer.FullName release-assets/CC-Switch-4.0.25-x64-setup.exe
Invoke-Logged 'source-archive.log' 'git' @('archive', '--format=zip', '--output=release-assets/source.zip', 'HEAD')
# git writes the patch directly, avoiding Windows newline/encoding transformations.
Invoke-Logged 'source-patch.log' 'git' @('diff', '--binary', '--output=release-assets/local-patches-vs-official.patch', "$env:UPSTREAM_SHA..HEAD")
Invoke-Logged 'package-validation.log' 'python' @('scripts/validate-local-release.py')
}
