' Auto-start entry for the fund dashboard (launched from the Startup folder shortcut).
' Flow: wait for logon to settle -> make sure the backend is up -> open the page.
' Kept ASCII-only on purpose: wscript reads .vbs files as ANSI, non-ASCII would garble.
Option Explicit

Const BASE_URL = "http://127.0.0.1:8000"

Dim sh
Set sh = CreateObject("WScript.Shell")

' Give the system/network a moment to become ready after logon
WScript.Sleep 15000

Dim healthy, i
healthy = False
For i = 1 To 10
  healthy = CheckHealth()
  If healthy Then Exit For
  If i = 1 Then
    ' Backend is down: start it silently (hidden window), overwrite log each boot
    sh.Run "cmd /c python -m uvicorn main:app --app-dir f:\fund\server --port 8000 > ""f:\fund\_probe\uv_autostart.log"" 2>&1", 0, False
  End If
  WScript.Sleep 3000
Next

' Open the dashboard in the default browser (page recovers once backend finishes booting)
sh.Run BASE_URL

Function CheckHealth()
  On Error Resume Next
  Err.Clear
  Dim http
  Set http = CreateObject("MSXML2.ServerXMLHTTP")
  http.setTimeouts 2000, 2000, 2000, 2000
  http.Open "GET", BASE_URL & "/api/health", False
  http.Send ""
  If Err.Number <> 0 Then
    CheckHealth = False
  Else
    CheckHealth = (http.Status = 200)
  End If
  On Error GoTo 0
End Function
