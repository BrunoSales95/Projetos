import pyotp
import pyperclip
import time
import os
#print('Forçando o fechamento do aplicativo da VPN: "f5fpclientW.exe"')
#os.system('start cmd /c "taskkill /IM f5fpclientW.exe /F')
#time.sleep(10)

for i in range(5):
    print("Gerando código TOTP, aguarde.")
    time.sleep(2)
    totp = pyotp.TOTP('123QRCODE')
    pyperclip.copy(totp.now())
    codigo = pyperclip.paste()
    print(f"Código {codigo} copiado para o seu clipboard.")    
    print("Aguarde 15s para gerar novo código.")
    time.sleep(20)
