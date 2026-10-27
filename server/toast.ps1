# Windows 右下角通知（零依赖）：由后端 subprocess 调用，标题/正文从参数传入（文件本身保持纯 ASCII）
param([string]$Title = 'Fund', [string]$Body = '')
$ErrorActionPreference = 'Stop'
[void][Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime]
[void][Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime]

function Esc([string]$s) {
    if ($null -eq $s) { return '' }
    return $s.Replace('&', '&amp;').Replace('<', '&lt;').Replace('>', '&gt;')
}

$xml = "<toast><visual><binding template='ToastText02'><text id='1'>$(Esc $Title)</text><text id='2'>$(Esc $Body)</text></binding></visual></toast>"
$doc = [Windows.Data.Xml.Dom.XmlDocument]::new()
$doc.LoadXml($xml)
$toast = [Windows.UI.Notifications.ToastNotification]::new($doc)
# 用 Windows PowerShell 已注册的 AppID，保证通知能落到操作中心
$app = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe'
[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($app).Show($toast)
Write-Output 'OK'
