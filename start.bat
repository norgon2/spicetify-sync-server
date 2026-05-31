@echo off
cd /d "C:\spicetify-sync"
start "Spicetify Sync Server" cmd /k node server.js
start "Ngrok Tunnel" cmd /k ngrok http 3000
