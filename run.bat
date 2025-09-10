@echo off
REM Determina o diretório onde o script está localizado
set "SCRIPT_DIR=%~dp0"
REM Remove a barra final do diretório
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Navega até o diretório do script
cd /d "%SCRIPT_DIR%" || exit /b

REM Verifica se o ambiente virtual já existe
if not exist "venv" (
    echo Criando o ambiente virtual...
    python -m venv venv
)

REM Ativa o ambiente virtual
echo Ativando o ambiente virtual...
call venv\Scripts\activate.bat

REM Instala as dependências
if exist "requirements.txt" (
    echo Instalando dependências...
    pip install -r requirements.txt
) else (
    echo Arquivo requirements.txt não encontrado. Pulando a instalação de dependências.
)

REM Executa o script principal
echo Executando o script ETL...
python main.py

REM Desativa o ambiente virtual
echo Desativando o ambiente virtual...
deactivate
