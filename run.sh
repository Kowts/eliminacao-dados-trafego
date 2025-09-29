#!/bin/bash

# Determina o diretório onde o script está localizado
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

# Navega até o diretório do script
cd "$SCRIPT_DIR" || exit

# Verifica se o ambiente virtual já existe
if [ ! -d "venv" ]; then
    echo "Criando o ambiente virtual..."
    python3 -m venv venv
fi

# Ativa o ambiente virtual
echo "Ativando o ambiente virtual..."
source venv/bin/activate

# Instala as dependências
if [ -f "requirements.txt" ]; then
    echo "Instalando dependências..."
    pip install -r requirements.txt
else
    echo "Arquivo requirements.txt não encontrado. Pulando a instalação de dependências."
fi

# Executa o script principal
echo "Executando o script ETL..."
python main.py --auto

# Desativa o ambiente virtual
echo "Desativando o ambiente virtual..."
deactivate
