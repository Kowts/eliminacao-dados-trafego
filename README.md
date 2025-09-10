# Retenção de Dados Tráfego

Sistema automatizado de purge diário que assegura que a tabela `f_trafegoc01` conserva apenas registos de tráfego dos últimos 3 meses, eliminando dados mais antigos.

## 📋 Informações do Processo

| Campo                        | Valor                        |
| ---------------------------- | ---------------------------- |
| **Nome**               | Retenção de Dados Tráfego |
| **Identificador**      | ETL016                       |
| **Versão**            | 0.1.0                        |
| **Modo de Execução** | Diário                      |
| **Horário**           | 06:00                        |
| **Base de Dados**      | SQL Server (CVTVMDWBI)       |
| **Tabela Alvo**        | f_trafegoc01                 |

## 🎯 Objetivo

Manter apenas os últimos **3 meses** de dados de tráfego na tabela `f_trafegoc01`, eliminando automaticamente registos mais antigos para:

* ✅ Otimizar performance da base de dados
* ✅ Controlar crescimento do storage
* ✅ Manter histórico relevante para análise
* ✅ Cumprir políticas de retenção de dados

## 🏗️ Arquitetura

```
📁 Projeto ETL016
├── 📄 main.py                     # Script principal
├── 📄 traffic_retention.py        # Classe de retenção
├── 📁 helpers/
│   ├── 📄 configuration.py        # Gestão de configurações
│   ├── 📄 email_sender.py         # Sistema de emails
│   ├── 📄 exception_handler.py    # Gestão de exceções
│   ├── 📄 logger_manager.py       # Sistema de logging
│   └── 📄 utils.py                # Utilitários gerais
├── 📁 database_connection/
│   ├── 📄 sqlserver_client.py     # Cliente SQL Server
│   ├── 📄 sqlserver_generic_crud.py # CRUD genérico
│   └── 📄 database_factory.py     # Factory pattern
├── 📄 configs.json               # Configurações do processo
├── 📄 config.ini                 # Configurações de conectividade
├── 📄 requirements.txt           # Dependências Python
├── 📄 run.sh                     # Script de execução Linux/Mac
└── 📄 run.bat                    # Script de execução Windows
```

## ⚙️ Configuração

### 1. **Ficheiros de Configuração**

#### `configs.json` - Configurações do Processo

```json
{
    "process": {
        "name": "Retenção de Dados Tráfego",
        "identifier": "ETL016",
        "execMode": "daily",
        "execTime": "06:00"
    },
    "report": {
        "from_mail": "dsi-suporte-rpa@cvt.cv",
        "to": "DMK-BI@cvt.cv",
        "cc": ["alertas.rpa@cvt.cv"],
        "subject": "Sucesso: Retenção Dados Tráfego"
    },
    "error_report": {
        "from_mail": "dsi-suporte-rpa@cvt.cv",
        "to": "alertas.rpa@cvt.cv",
        "subject": "Erro: Retenção Dados Tráfego"
    }
}
```

#### `config.ini` - Conectividade

```ini
[CVTVMDWBI]
SERVER=192.168.81.150
PORT=1433
DATABASE=DMETL
LOGIN=talend_user
PASSWORD=Pass#1234

[SMTP]
server=192.168.83.8
port=25
```

### 2. **Variáveis de Ambiente (Opcional)**

```bash
# .env
DB_PASSWORD=your_secure_password
SMTP_PASSWORD=your_smtp_password
```

## 🚀 Instalação e Execução

### **Método 1: Scripts Automáticos**

#### Linux/Mac

```bash
chmod +x run.sh
./run.sh
```

#### Windows

```cmd
run.bat
```

### **Método 2: Manual**

#### 1. **Criar Ambiente Virtual**

```bash
python -m venv venv

# Linux/Mac
source venv/bin/activate

# Windows
venv\Scripts\activate
```

#### 2. **Instalar Dependências**

```bash
pip install -r requirements.txt
```

#### 3. **Executar Processo**

```bash
python main.py
```

## 🛡️ Funcionalidades de Segurança

### **Safety Nets Implementados**

| Validação                     | Limite  | Descrição                                       |
| ------------------------------- | ------- | ------------------------------------------------- |
| **Registos Mínimos**     | 1,000   | Garante que permanecem registos suficientes       |
| **Batch Máximo**         | 100,000 | Limita eliminações por operação (performance) |
| **Validação de Tabela** | ✅      | Verifica existência antes de operar              |
| **Transações**          | ✅      | Rollback automático em caso de erro              |
| **Formato de Data**       | ✅      | Validação robusta do campo `[Dia]`            |

### **Critérios de Retenção**

* 📅  **Campo de Referência** : `[Dia]` (formato: d/m/yyyy)
* ⏰  **Período de Retenção** : 3 meses
* 📊  **Cálculo** : Data atual - 3 meses (preservando o dia)
* 🔍  **Exemplo** : 9/9/2025 → Data de corte: 9/6/2025

## 📊 Sistema de Relatórios

### **Relatório de Sucesso**

* ✅ Enviado para equipa de BI
* 📈 Métricas detalhadas de operação
* ⏱️ Tempo de execução
* 📋 Tabela com resumo estatístico

### **Relatório de Erro**

* ❌ Enviado para equipa de alertas
* 🔍 Detalhes técnicos completos
* 📝 Stack trace e contexto
* 🗃️ Armazenamento em `error_logs`

## 📝 Logs Detalhados

### **Localização**

* 📁 Diretório: `logs/`
* 📄 Formato: `agt003dsi_YYYYMMDDHHMISS.log`
* 🔄 Rotação automática

### **Níveis de Log**

```
INFO  - Operações normais
WARN  - Situações de atenção
ERROR - Erros recuperáveis
DEBUG - Informação detalhada
```

## 🔧 Resolução de Problemas

### **Problemas Comuns**

#### 1. **Erro de Conectividade**

```
ERROR: Error connecting to SQL Server Database
```

 **Solução** : Verificar config.ini e conectividade de rede

#### 2. **Tabela Não Encontrada**

```
ERROR: Tabela f_trafegoc01 não encontrada
```

 **Solução** : Validar nome da tabela e permissões

#### 3. **Poucos Registos**

```
ERROR: Muito poucos registos iriam permanecer
```

 **Solução** : Ajustar `min_records_safety` ou verificar dados

#### 4. **Formato de Data Inválido**

```
WARN: Erro ao converter data '32/13/2025'
```

 **Solução** : Limpar dados inconsistentes na tabela

### **Logs de Diagnóstico**

```bash
# Ver logs recentes
tail -f logs/agt003dsi_*.log

# Procurar erros
grep "ERROR" logs/agt003dsi_*.log

# Métricas de performance
grep "took.*seconds" logs/agt003dsi_*.log
```

## 📈 Monitorização

### **Métricas Chave**

* 📊 Registos eliminados vs mantidos
* ⏱️ Tempo de execução
* 💾 Espaço libertado
* 📧 Taxa de sucesso de emails

### **Alertas Automatizados**

* 🔴 Falhas de execução
* 🟡 Performance degradada (>60s)
* 🟢 Execuções bem-sucedidas

## 🔄 Agendamento

### **Cron (Linux/Mac)**

```bash
# Executar diariamente às 06:00
0 6 * * * /path/to/project/run.sh >> /var/log/etl016.log 2>&1
```

### **Task Scheduler (Windows)**

```
Programa: C:\path\to\project\run.bat
Trigger: Diário às 06:00
```

### **Sistema de Orquestração**

* 🔧 Apache Airflow
* 🔧 Azure Data Factory
* 🔧 Talend Job Scheduler

## 📞 Suporte

### **Contactos**

* 🏢  **Equipa** : DSI-RPA CVT
* 📧  **Email** : dsi-suporte-rpa@cvt.cv
* 📧  **Alertas** : alertas.rpa@cvt.cv

### **Documentação Adicional**

* 📚 [Guia de Configuração]()
* 🔧 [Manual de Troubleshooting]()
* 📊 [Especificações Técnicas]()

---

## 🏷️ Versioning

| Versão | Data       | Alterações                  |
| ------- | ---------- | ----------------------------- |
| 0.1.0   | 2025-09-10 | ✨ Versão inicial do sistema |

## 📄 Licença

Propriedade de **CVT - Cabo Verde Telecom**

Uso interno exclusivo.

---

 **💡 Dica** : Para execução manual de teste, use `python main.py` no ambiente virtual ativado.
