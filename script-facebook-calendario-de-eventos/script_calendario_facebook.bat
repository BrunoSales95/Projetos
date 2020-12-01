
# Script para formatar a data atual
@echo off

for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x

for /f %%x in ('wmic path win32_localtime get /format:list ^| findstr "="') do set %%x

set fmonth=00%Month%
set fday=00%Day%
set today=%Year%-%fmonth:~-2%-%fday:~-2%

@echo on
COLOR 02
echo Data atual: %today%

echo ID da cidade de SP PELO GoogleMaps -  112047398814697

echo Iniciando o navegador para acessar o facebook

start "" https://www.facebook.com/events/discovery/?suggestion_token={"time":"{\"start\":\"%today%\",\"end\":\"%today%\"}","city":"112047398814697"}