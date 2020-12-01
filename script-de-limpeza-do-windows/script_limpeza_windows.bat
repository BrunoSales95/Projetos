@echo off
COLOR 0C

ECHO verificando arquivos de lixo da pasta "%temp%"
timeout /t 0
del /f /s /q "%temp%"

ECHO  Arquivos deletados
timeout /t 0

ECHO  verificando arquivos de lixo da pasta temp
timeout /t 0
del /f /s /q "%systemroot%\temp"
ECHO Arquivos deletados

ECHO  verificando arquivos de lixo da pasta Prefetch
timeout /t 0
del /f /s /q "%systemroot%\Prefetch"
ECHO Arquivos deletados

ECHO  verificando arquivos de lixo da pasta Recent
timeout /t 0
del /f /s /q "%systemroot%\Recent"
ECHO Arquivos deletados

ECHO  verificando arquivos de lixo da pasta Recent do Windows
timeout /t 0
del /f /s /q "C:\Users\%username%\AppData\Roaming\Microsoft\Windows\Recent" Items*.* /Q pause
ECHO Arquivos deletados
timeout /t 0
