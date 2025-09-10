
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

<style>#mermaid-1757494591101{font-family:sans-serif;font-size:16px;fill:#333;}#mermaid-1757494591101 .error-icon{fill:#552222;}#mermaid-1757494591101 .error-text{fill:#552222;stroke:#552222;}#mermaid-1757494591101 .edge-thickness-normal{stroke-width:2px;}#mermaid-1757494591101 .edge-thickness-thick{stroke-width:3.5px;}#mermaid-1757494591101 .edge-pattern-solid{stroke-dasharray:0;}#mermaid-1757494591101 .edge-pattern-dashed{stroke-dasharray:3;}#mermaid-1757494591101 .edge-pattern-dotted{stroke-dasharray:2;}#mermaid-1757494591101 .marker{fill:#333333;}#mermaid-1757494591101 .marker.cross{stroke:#333333;}#mermaid-1757494591101 svg{font-family:sans-serif;font-size:16px;}#mermaid-1757494591101 .label{font-family:sans-serif;color:#333;}#mermaid-1757494591101 .label text{fill:#333;}#mermaid-1757494591101 .node rect,#mermaid-1757494591101 .node circle,#mermaid-1757494591101 .node ellipse,#mermaid-1757494591101 .node polygon,#mermaid-1757494591101 .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#mermaid-1757494591101 .node .label{text-align:center;}#mermaid-1757494591101 .node.clickable{cursor:pointer;}#mermaid-1757494591101 .arrowheadPath{fill:#333333;}#mermaid-1757494591101 .edgePath .path{stroke:#333333;stroke-width:1.5px;}#mermaid-1757494591101 .flowchart-link{stroke:#333333;fill:none;}#mermaid-1757494591101 .edgeLabel{background-color:#e8e8e8;text-align:center;}#mermaid-1757494591101 .edgeLabel rect{opacity:0.5;background-color:#e8e8e8;fill:#e8e8e8;}#mermaid-1757494591101 .cluster rect{fill:#ffffde;stroke:#aaaa33;stroke-width:1px;}#mermaid-1757494591101 .cluster text{fill:#333;}#mermaid-1757494591101 div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:sans-serif;font-size:12px;background:hsl(80,100%,96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#mermaid-1757494591101:root{--mermaid-font-family:sans-serif;}#mermaid-1757494591101:root{--mermaid-alt-font-family:sans-serif;}#mermaid-1757494591101 flowchart-v2{fill:apa;}</style>
