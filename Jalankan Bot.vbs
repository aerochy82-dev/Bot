' Launcher untuk "Trading Bot Monitor" -- versi 2, FIX bug window native
' tidak kelihatan.
'
' PENYEBAB BUG SEBELUMNYA:
' VBS memanggil "python bot.py --gui" dengan windowStyle=0 (hidden) untuk
' menyembunyikan konsol hitam. Tapi di sebagian sistem Windows, window
' NATIVE yang dibuat pywebview ikut ke-hide juga sebagai efek samping,
' walau secara teori seharusnya window terpisah -- proses berjalan penuh
' (dashboard ke-poll terus, kelihatan di trading_bot.log), CUMA window-nya
' tidak pernah kelihatan di layar.
'
' FIX: pakai pythonw.exe (Python TANPA konsol sama sekali dari awal,
' beda dari python.exe biasa) -- jadi tidak perlu "sembunyikan" apapun
' lewat VBS, dan window native pywebview tidak ikut kena efek hide.
'
' CARA PAKAI: sama seperti sebelumnya -- taruh di folder yang sama dengan
' bot.py, buat shortcut ke Desktop, ganti icon-nya pakai bot_icon.ico.

Dim fso, scriptDir, objShell

Set fso = CreateObject("Scripting.FileSystemObject")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)

Set objShell = CreateObject("WScript.Shell")
objShell.CurrentDirectory = scriptDir

On Error Resume Next
objShell.Run "pythonw bot.py --gui", 1, False
If Err.Number <> 0 Then
    MsgBox "Gagal menjalankan bot." & vbCrLf & vbCrLf & _
        "Kemungkinan 'pythonw' tidak ditemukan di PATH sistem kamu." & vbCrLf & _
        "Cek: buka Command Prompt, ketik 'where pythonw' -- kalau tidak" & vbCrLf & _
        "ketemu, install ulang Python dari python.org dan CENTANG opsi" & vbCrLf & _
        "'Add python.exe to PATH' saat instalasi." & vbCrLf & vbCrLf & _
        "Detail error: " & Err.Description, vbCritical, "Trading Bot Monitor"
End If
On Error Goto 0
