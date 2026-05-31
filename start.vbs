Dim sh
Set sh = CreateObject("WScript.Shell")
sh.Run "cmd /c cd /d C:\spicetify-sync && node server.js", 0, False
sh.Run "cmd /c ngrok http 3000", 0, False
Set sh = Nothing
