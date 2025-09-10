# 🔧 Guia de Configuração - ETL016

Guia completo para configuração do sistema de retenção de dados de tráfego.

## 📋 Índice

1. [Pré-requisitos](#pr%C3%A9-requisitos)
2. [Configurações Base](#configura%C3%A7%C3%B5es-base)
3. [Configurações Avançadas](#configura%C3%A7%C3%B5es-avan%C3%A7adas)
4. [Segurança](#seguran%C3%A7a)
5. [Ambientes](#ambientes)
6. [Validação](#valida%C3%A7%C3%A3o)

---

## 🛠️ Pré-requisitos

### **Sistema Operativo**

* ✅ Windows 10/11 ou Windows Server 2016+
* ✅ Linux (Ubuntu 18.04+, CentOS 7+)
* ✅ macOS 10.14+

### **Software Base**

* 🐍  **Python** : 3.8+ (recomendado 3.9+)
* 🗄️  **SQL Server** : 2016+ com ODBC Driver 17
* 📧  **SMTP Server** : Acesso ao servidor de email CVT
* 🌐  **Conectividade** : Acesso às bases de dados alvo

### **Permissões de Base de Dados**

```sql
-- Permissões mínimas requeridas
GRANT SELECT, DELETE ON f_trafegoc01 TO [talend_user];
GRANT CREATE TABLE, INSERT ON error_logs TO [talend_user];
GRANT EXECUTE ON SCHEMA::dbo TO [talend_user];
```

---

## ⚙️ Configurações Base

### **1. configs.json - Configuração Principal**

```json
{
    "process": {
        "name": "Retenção de Dados Tráfego",
        "identifier": "ETL016",
        "description": "Purge diário que assegura que a tabela f_trafegoc01 conserva apenas registos de tráfego dos últimos 3 meses",
        "version": "0.1.0",
        "execMode": "daily",
        "execDay": 2,
        "execTime": "06:00"
    },
    "database": {
        "webmail": {
            "table": "platforms",
            "where": {
                "clause": "identifier = %s",
                "params": "CVT_Webmail"
            }
        }
    },
    "report": {
        "from_mail": "dsi-suporte-rpa@cvt.cv",
        "to": "DMK-BI@cvt.cv",
        "cc": ["alertas.rpa@cvt.cv"],
        "subject": "Sucesso: Retenção Dados Tráfego ETL016"
    },
    "error_report": {
        "from_mail": "dsi-suporte-rpa@cvt.cv",
        "to": "alertas.rpa@cvt.cv",
        "subject": "Erro: Retenção Dados Tráfego ETL016",
        "error_dashboard_url": "http://dashboard.cvt.cv/etl/errors"
    }
}
```

#### **Campos Obrigatórios**

* `process.identifier`: ID único do processo
* `report.to`: Destinatário dos relatórios de sucesso
* `error_report.to`: Destinatário dos alertas de erro

#### **Campos Opcionais**

* `process.execDay`: Dia da semana (0-6, onde 0=domingo)
* `error_report.error_dashboard_url`: URL do dashboard de erros

### **2. config.ini - Conectividade**

```ini
# Configuração SQL Server Principal
[CVTVMDWBI]
SERVER=192.168.81.150
PORT=1433
DATABASE=DMETL
LOGIN=talend_user
PASSWORD=Pass#1234
SHEMA=dbo

# Configuração SMTP
[SMTP]
server=192.168.83.8
port=25
username=
password=

# Configuração BRM (se necessário)
[BRM]
HOST=10.16.10.103
PORT=1536
DBNAME=PIN
USER=PIN
PASSWORD=PIN
SID=CVTBRPRD3

# Configuração PostgreSQL (se necessário)
[POSTGRESQL]
HOST=localhost
PORT=5432
DBNAME=automationhub
USER=postgres
PASSWORD=P$23fg#98

# Configurações do processo
[DATABASE]
table_trafego=f_trafegoc01
```

---

## 🔧 Configurações Avançadas

### **1. Parâmetros de Retenção**

Editar em `traffic_retention.py`:

```python
class TrafficRetention:
    def __init__(self):
        # Configurações personalizáveis
        self.table_name = "f_trafegoc01"           # Tabela alvo
        self.date_column = "[Dia]"                # Coluna de data
        self.retention_months = 3                 # Meses a manter
        self.min_records_safety = 1000            # Mínimo de registos
        self.max_delete_batch = 100000            # Máximo por operação
```

### **2. Configurações de Logging**

```python
# helpers/logger_manager.py - Personalização
class LoggerManager:
    def __init__(self, log_dir='logs', log_level=logging.DEBUG):
        self.log_dir = log_dir                    # Diretório de logs
        self.log_level = log_level                # Nível de detalhe

    def add_rotating_file_handler(self, max_bytes=10485760, backup_count=5):
        # max_bytes: Tamanho máximo por ficheiro (10MB)
        # backup_count: Número de ficheiros de backup
```

### **3. Configurações de Email**

```python
# helpers/email_sender.py - Templates personalizados
ALERT_COLORS = {
    'success': '#28a745',    # Verde
    'warning': '#ffc107',    # Amarelo
    'danger': '#dc3545',     # Vermelho
    'info': '#17a2b8'        # Azul
}
```

### **4. Configurações de Performance**

```python
# database_connection/sqlserver_client.py
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server},{port};"
    f"DATABASE={database};"
    f"Connection Pooling=true;"
    f"Max Pool Size=100;"           # Pool de conexões
    f"timeout=30;"                  # Timeout de queries
)
```

---

## 🔐 Segurança

### **1. Gestão de Passwords**

#### **Opção A: Ficheiro .env**

```bash
# .env
DB_PASSWORD=your_secure_password
SMTP_PASSWORD=your_smtp_password
ERROR_DASHBOARD_TOKEN=your_token
```

#### **Opção B: Variáveis de Sistema**

```bash
# Linux/Mac
export ETL016_DB_PASSWORD="secure_password"
export ETL016_SMTP_PASSWORD="smtp_password"

# Windows
set ETL016_DB_PASSWORD=secure_password
set ETL016_SMTP_PASSWORD=smtp_password
```

#### **Opção C: Azure Key Vault / Secrets Manager**

```python
# Exemplo de integração com Azure Key Vault
from azure.keyvault.secrets import SecretClient

def get_secret(secret_name):
    client = SecretClient(vault_url="https://cvt-keyvault.vault.azure.net/")
    return client.get_secret(secret_name).value
```

### **2. Encriptação de Ficheiros**

```python
# Exemplo com cryptography
from cryptography.fernet import Fernet

def encrypt_config(config_data, key):
    f = Fernet(key)
    encrypted_data = f.encrypt(config_data.encode())
    return encrypted_data

def decrypt_config(encrypted_data, key):
    f = Fernet(key)
    decrypted_data = f.decrypt(encrypted_data)
    return decrypted_data.decode()
```

### **3. Permissões de Ficheiros**

```bash
# Linux/Mac - Restringir acesso aos ficheiros de config
chmod 600 config.ini
chmod 600 .env
chmod 755 *.py

# Windows - Usar Properties > Security para restringir acesso
```

---

## 🌍 Configuração por Ambientes

### **Desenvolvimento**

```json
{
    "process": {
        "identifier": "ETL016-DEV",
        "execMode": "manual"
    },
    "report": {
        "to": "dev-team@cvt.cv"
    },
    "retention_months": 1,
    "min_records_safety": 100
}
```

### **Teste**

```json
{
    "process": {
        "identifier": "ETL016-TEST",
        "execMode": "daily",
        "execTime": "02:00"
    },
    "report": {
        "to": "qa-team@cvt.cv"
    },
    "retention_months": 2,
    "min_records_safety": 500
}
```

### **Produção**

```json
{
    "process": {
        "identifier": "ETL016-PROD",
        "execMode": "daily",
        "execTime": "06:00"
    },
    "report": {
        "to": "DMK-BI@cvt.cv",
        "cc": ["alertas.rpa@cvt.cv", "manager@cvt.cv"]
    },
    "retention_months": 3,
    "min_records_safety": 1000
}
```

### **Gestão Multi-Ambiente**

```python
# config_manager.py
import os

class ConfigManager:
    def __init__(self):
        self.environment = os.getenv('ETL_ENV', 'production')

    def get_config_file(self):
        config_files = {
            'development': 'configs-dev.json',
            'testing': 'configs-test.json',
            'production': 'configs.json'
        }
        return config_files.get(self.environment, 'configs.json')
```

---

## ✅ Validação da Configuração

### **1. Script de Validação**

```python
# validate_config.py
import json
from helpers.configuration import load_json_config, load_ini_config

def validate_configs():
    """Validar todas as configurações necessárias."""

    # Validar configs.json
    try:
        config = load_json_config()
        required_fields = [
            'process.identifier',
            'report.to',
            'error_report.to'
        ]

        for field in required_fields:
            keys = field.split('.')
            value = config
            for key in keys:
                value = value[key]
            print(f"✅ {field}: {value}")

    except Exception as e:
        print(f"❌ Erro em configs.json: {e}")

    # Validar config.ini
    try:
        db_config = load_ini_config('CVTVMDWBI')
        smtp_config = load_ini_config('SMTP')

        print(f"✅ Database: {db_config['SERVER']}:{db_config['PORT']}")
        print(f"✅ SMTP: {smtp_config['server']}:{smtp_config['port']}")

    except Exception as e:
        print(f"❌ Erro em config.ini: {e}")

if __name__ == "__main__":
    validate_configs()
```

### **2. Teste de Conectividade**

```python
# test_connectivity.py
from database_connection import DatabaseFactory
from helpers.email_sender import EmailSender

def test_database_connection():
    """Testar conexão à base de dados."""
    try:
        db_config = load_ini_config('CVTVMDWBI')
        client = DatabaseFactory.get_database('sqlserver', db_config)
        client.connect()

        # Teste simples
        result = client.execute_query("SELECT 1 as test")
        print(f"✅ Base de dados: Conexão OK - {result}")

        client.disconnect()

    except Exception as e:
        print(f"❌ Base de dados: {e}")

def test_email_connectivity():
    """Testar envio de email."""
    try:
        smtp_config = load_ini_config('SMTP')
        sender = EmailSender(smtp_config)

        # Email de teste
        success = sender.send_email(
            to="test@cvt.cv",
            subject="Teste ETL016",
            message_body="Teste de conectividade do sistema ETL016"
        )

        if success:
            print("✅ Email: Envio OK")
        else:
            print("❌ Email: Falha no envio")

    except Exception as e:
        print(f"❌ Email: {e}")

if __name__ == "__main__":
    test_database_connection()
    test_email_connectivity()
```

### **3. Checklist de Configuração**

```markdown
## ✅ Checklist Pré-Execução

### Ficheiros de Configuração
- [ ] configs.json existe e válido
- [ ] config.ini existe e válido
- [ ] .env configurado (se aplicável)
- [ ] requirements.txt atualizado

### Conectividade
- [ ] Acesso à base de dados CVTVMDWBI
- [ ] Tabela f_trafegoc01 existe e acessível
- [ ] Servidor SMTP acessível
- [ ] Permissões de escrita no diretório logs/

### Segurança
- [ ] Passwords não estão hardcoded
- [ ] Ficheiros de config com permissões restritas
- [ ] Logs não expõem informação sensível

### Ambiente
- [ ] Python 3.8+ instalado
- [ ] Dependências instaladas (pip install -r requirements.txt)
- [ ] ODBC Driver 17 para SQL Server instalado
- [ ] Ambiente virtual ativado

### Funcional
- [ ] Script de validação executado com sucesso
- [ ] Teste de conectividade passou
- [ ] Logs a ser gerados corretamente
- [ ] Emails de teste enviados com sucesso
```

---

## 📞 Suporte de Configuração

### **Contactos Técnicos**

* 🔧  **DevOps** : devops@cvt.cv
* 🗄️  **DBA** : dba@cvt.cv
* 🔐  **Segurança** : security@cvt.cv
* 📧  **Email Admin** : email-admin@cvt.cv

### **Recursos Adicionais**

* 📚 [Documentação SQL Server](https://docs.microsoft.com/sql/)
* 🔧 [Python ODBC Setup](https://docs.microsoft.com/en-us/sql/connect/python/)
* 📧 [SMTP Configuration Guide]()

---

 **💡 Dica** : Execute sempre `python validate_config.py` antes de colocar em produção!
