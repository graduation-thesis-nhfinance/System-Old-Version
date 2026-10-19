# ==============================================================================
# START TUNNEL IN BACKGROUND (CHAY NGAM - KHONG CAN GIU POWERSHELL)
# ==============================================================================

$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$logPath = Join-Path $PSScriptRoot "tunnel.log"
$urlFilePath = Join-Path $PSScriptRoot "TUNNEL_URL.txt"

# 1. Dung tien trinh tunnel cu neu co
Get-Process cloudflared -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
if (Test-Path $logPath) { Remove-Item $logPath -Force -ErrorAction SilentlyContinue }

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  KHOI DONG CLOUDFLARE TUNNEL CHAY NGAM CHO ADMIN PORTAL (PORT 8090)  " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Dang ket noi voi Cloudflare Edge Network..." -ForegroundColor Yellow

# 2. Khoi dong cloudflared chay an (Hidden) va ghi log de lay URL
$proc = Start-Process -FilePath "cloudflared" -ArgumentList "tunnel --url http://localhost:8090" -RedirectStandardError $logPath -WindowStyle Hidden -PassThru

# 3. Quet log de trich xuat duong link trycloudflare.com
$tunnelUrl = ""
for ($i = 0; $i -lt 15; $i++) {
    Start-Sleep -Milliseconds 800
    if (Test-Path $logPath) {
        $content = Get-Content $logPath -Raw -ErrorAction SilentlyContinue
        if ($content -match "(https://[a-zA-Z0-9-]+\.trycloudflare\.com)") {
            $tunnelUrl = $matches[1]
            break
        }
    }
}

if ($tunnelUrl) {
    Set-Content -Path $urlFilePath -Value $tunnelUrl -Force
    Set-Clipboard -Value $tunnelUrl -ErrorAction SilentlyContinue

    Write-Host ""
    Write-Host "🎉 DUONG HAM HTTPS PUBLIC DA DUOC KHOI TAO THANH CONG!" -ForegroundColor Green
    Write-Host ""
    Write-Host "👉 LINK ADMIN PORTAL: " -NoNewline -ForegroundColor Cyan
    Write-Host "$tunnelUrl" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "✅ Da copy duong link tren vao Clipboard (Ctrl+V de dan vao trinh duyet)." -ForegroundColor Green
    Write-Host "✅ Link cung duoc luu vao file: TUNNEL_URL.txt" -ForegroundColor Green
    Write-Host ""
    Write-Host "💡 BAN CO THE DONG CUA SO POWERSHELL NAY NGAY BAY GIO!" -ForegroundColor Magenta
    Write-Host "   Tunnel van se tiep tuc chay ngam duoi nen." -ForegroundColor White
    Write-Host "   (Khi nao muon dung lai chi can chay: .\stop_tunnel.ps1)" -ForegroundColor DarkGray
    Write-Host ""
} else {
    Write-Host "⚠️ Khong the lay URL tu Cloudflare Tunnel trong thoi gian cho." -ForegroundColor Red
    Write-Host "Chi tiet log tai: $logPath" -ForegroundColor DarkGray
}
