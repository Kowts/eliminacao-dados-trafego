# 🔍 Manual de Troubleshooting - ETL016

Guia completo para diagnóstico e resolução de problemas do sistema de retenção de dados.

## 📋 Índice

1. [Problemas de Conectividade](#problemas-de-conectividade)
2. [Erros de Configuração](#erros-de-configura%C3%A7%C3%A3o)
3. [Problemas de Performance](#problemas-de-performance)
4. [Erros de Email](#erros-de-email)
5. [Problemas de Dados](#problemas-de-dados)
6. [Diagnóstico Avançado](#diagn%C3%B3stico-avan%C3%A7ado)

---

## 🌐 Problemas de Conectividade

### **❌ Erro: "Error connecting to SQL Server Database"**

#### **Diagnóstico**

##### **1. Verificar Distribuição de Dados**

```sql
-- Analisar distribuição por mês
SELECT
    YEAR(TRY_CONVERT(DATE, [Dia], 103)) as Ano,
    MONTH(TRY_CONVERT(DATE, [Dia], 103)) as Mes,
    COUNT(*) as Total_Registos
FROM f_trafegoc01
WHERE [Dia] IS NOT NULL
GROUP BY YEAR(TRY_CONVERT(DATE, [Dia], 103)), MONTH(TRY_CONVERT(DATE, [Dia], 103))
ORDER BY Ano DESC, Mes DESC;
```

##### **2. Verificar Qualidade dos Dados**

```sql
-- Verificar registos com datas inválidas
SELECT
    COUNT(*) as Total,
    COUNT(CASE WHEN [Dia] IS NULL THEN 1 END) as Nulos,
    COUNT(CASE WHEN TRY_CONVERT(DATE, [Dia], 103) IS NULL AND [Dia] IS NOT NULL THEN 1 END) as Invalidos
FROM f_trafegoc01;
```

#### **Soluções**

##### **1. Ajustar Safety Net**

```python
# traffic_retention.py - Reduzir temporariamente
self.min_records_safety = 500  # Em vez de 1000

# Ou fazer bypass para situações específicas
def _validate_safety_constraints_flexible(self, records_to_keep: int, records_to_delete: int) -> bool:
    """Validação flexível com override manual."""

    # Verificar variável de ambiente para bypass
    if os.getenv('ETL016_FORCE_PURGE', '').lower() == 'true':
        self.logger.warning("BYPASS: Safety constraints ignorados por ETL016_FORCE_PURGE")
        return records_to_delete > 0

    return self._validate_safety_constraints(records_to_keep, records_to_delete)
```

##### **2. Purge Incremental**

```python
def _calculate_incremental_cutoff(self) -> datetime:
    """Calcular data de corte incremental (mais conservadora)."""

    today = datetime.now()
    # Começar com 4 meses em vez de 3
    cutoff_date = today - timedelta(days=120)

    self.logger.info(f"Data de corte incremental: {cutoff_date.strftime('%d/%m/%Y')}")
    return cutoff_date
```

### **❌ Erro: "Datas inválidas encontradas"**

#### **Sintomas**

```
WARNING: Erro ao converter data '32/13/2025': day is out of range for month
```

#### **Diagnóstico**

##### **1. Identificar Registos Problemáticos**

```sql
-- Encontrar datas inválidas
SELECT TOP 100 [Dia], COUNT(*) as Ocorrencias
FROM f_trafegoc01
WHERE [Dia] IS NOT NULL
AND TRY_CONVERT(DATE, [Dia], 103) IS NULL
GROUP BY [Dia]
ORDER BY Ocorrencias DESC;
```

##### **2. Padrões Comuns de Erro**

```sql
-- Verificar padrões específicos
SELECT
    [Dia],
    CASE
        WHEN [Dia] LIKE '%/%/%' AND LEN([Dia]) > 10 THEN 'Muito_Longo'
        WHEN [Dia] LIKE '%-%' THEN 'Formato_Errado'
        WHEN [Dia] LIKE '%.%' THEN 'Pontos'
        WHEN ISNUMERIC([Dia]) = 1 THEN 'So_Numeros'
        ELSE 'Outro'
    END as Tipo_Erro,
    COUNT(*) as Total
FROM f_trafegoc01
WHERE [Dia] IS NOT NULL
AND TRY_CONVERT(DATE, [Dia], 103) IS NULL
GROUP BY [Dia],
    CASE
        WHEN [Dia] LIKE '%/%/%' AND LEN([Dia]) > 10 THEN 'Muito_Longo'
        WHEN [Dia] LIKE '%-%' THEN 'Formato_Errado'
        WHEN [Dia] LIKE '%.%' THEN 'Pontos'
        WHEN ISNUMERIC([Dia]) = 1 THEN 'So_Numeros'
        ELSE 'Outro'
    END
ORDER BY Total DESC;
```

#### **Soluções**

##### **1. Parser Melhorado de Datas**

```python
def _parse_date_string_robust(self, date_str: str) -> Optional[datetime]:
    """Parser robusto que tenta múltiplos formatos."""

    if not date_str or not isinstance(date_str, str):
        return None

    try:
        # Limpar string
        cleaned = date_str.strip()

        # Tentar múltiplos formatos
        formats = [
            '%d/%m/%Y',    # 09/06/2025
            '%d-%m-%Y',    # 09-06-2025
            '%Y-%m-%d',    # 2025-06-09
            '%m/%d/%Y',    # 06/09/2025 (formato americano)
            '%d.%m.%Y',    # 09.06.2025
        ]

        for fmt in formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue

        # Tentar parsing manual para d/m/yyyy
        parts = re.split(r'[/\-\.]', cleaned)
        if len(parts) == 3:
            try:
                # Assumir d/m/yyyy se dia <= 12, senão m/d/yyyy
                if int(parts[0]) <= 12:
                    day, month, year = parts
                else:
                    month, day, year = parts

                return datetime(int(year), int(month), int(day))
            except ValueError:
                pass

        return None

    except Exception as e:
        self.logger.warning(f"Erro ao converter data '{date_str}': {e}")
        return None
```

##### **2. Limpeza de Dados Pré-Purge**

```python
def _cleanup_invalid_dates(self) -> int:
    """Limpar registos com datas inválidas antes do purge."""

    try:
        # Marcar datas inválidas como NULL
        cleanup_query = f"""
        UPDATE {self.table_name}
        SET {self.date_column} = NULL
        WHERE {self.date_column} IS NOT NULL
        AND TRY_CONVERT(DATE, {self.date_column}, 103) IS NULL
        """

        result = self.db_client.execute_query(cleanup_query)
        cleaned_records = result if isinstance(result, int) else 0

        self.logger.info(f"Limpeza: {cleaned_records:,} registos com datas inválidas marcados como NULL")
        return cleaned_records

    except Exception as e:
        self.logger.error(f"Erro na limpeza de datas: {e}")
        return 0
```

---

## 🔬 Diagnóstico Avançado

### **🔍 Script de Diagnóstico Completo**

```python
# diagnostics.py
import json
import logging
from datetime import datetime
from helpers.configuration import load_json_config, load_ini_config
from database_connection import DatabaseFactory

class ETL016Diagnostics:
    """Classe para diagnóstico completo do sistema ETL016."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def run_full_diagnostics(self):
        """Executar diagnóstico completo."""

        print("🔍 ETL016 - DIAGNÓSTICO COMPLETO")
        print("=" * 50)

        # 1. Verificar configurações
        self._check_configurations()

        # 2. Verificar conectividade
        self._check_connectivity()

        # 3. Verificar dados
        self._check_data_quality()

        # 4. Verificar performance
        self._check_performance()

        # 5. Verificar sistema
        self._check_system_resources()

        print("\n✅ Diagnóstico concluído!")

    def _check_configurations(self):
        """Verificar todas as configurações."""
        print("\n📋 Verificando Configurações...")

        try:
            # configs.json
            config = load_json_config()
            print(f"✅ configs.json: {config['process']['identifier']}")

            # config.ini
            db_config = load_ini_config('CVTVMDWBI')
            print(f"✅ config.ini [CVTVMDWBI]: {db_config['SERVER']}")

            smtp_config = load_ini_config('SMTP')
            print(f"✅ config.ini [SMTP]: {smtp_config['server']}")

        except Exception as e:
            print(f"❌ Erro de configuração: {e}")

    def _check_connectivity(self):
        """Verificar conectividade."""
        print("\n🌐 Verificando Conectividade...")

        try:
            db_config = load_ini_config('CVTVMDWBI')
            client = DatabaseFactory.get_database('sqlserver', db_config)
            client.connect()

            # Teste básico
            result = client.execute_query("SELECT @@VERSION", fetch_as_dict=True)
            version = result[0][''] if result else "Unknown"
            print(f"✅ SQL Server: {version[:50]}...")

            # Verificar tabela
            table_check = client.execute_query(
                "SELECT COUNT(*) as total FROM f_trafegoc01",
                fetch_as_dict=True
            )
            total = table_check[0]['total']
            print(f"✅ Tabela f_trafegoc01: {total:,} registos")

            client.disconnect()

        except Exception as e:
            print(f"❌ Erro de conectividade: {e}")

    def _check_data_quality(self):
        """Verificar qualidade dos dados."""
        print("\n🗄️ Verificando Qualidade dos Dados...")

        try:
            db_config = load_ini_config('CVTVMDWBI')
            client = DatabaseFactory.get_database('sqlserver', db_config)
            client.connect()

            # Análise geral
            quality_query = """
            SELECT
                COUNT(*) as Total,
                COUNT([Dia]) as Com_Data,
                COUNT(*) - COUNT([Dia]) as Sem_Data,
                COUNT(CASE WHEN TRY_CONVERT(DATE, [Dia], 103) IS NOT NULL THEN 1 END) as Datas_Validas,
                COUNT(CASE WHEN [Dia] IS NOT NULL AND TRY_CONVERT(DATE, [Dia], 103) IS NULL THEN 1 END) as Datas_Invalidas
            FROM f_trafegoc01
            """

            result = client.execute_query(quality_query, fetch_as_dict=True)[0]

            print(f"📊 Total de registos: {result['Total']:,}")
            print(f"📅 Com data: {result['Com_Data']:,} ({(result['Com_Data']/result['Total']*100):.1f}%)")
            print(f"❌ Sem data: {result['Sem_Data']:,}")
            print(f"✅ Datas válidas: {result['Datas_Validas']:,}")
            print(f"⚠️ Datas inválidas: {result['Datas_Invalidas']:,}")

            # Distribuição temporal
            temporal_query = """
            SELECT TOP 5
                YEAR(TRY_CONVERT(DATE, [Dia], 103)) as Ano,
                MONTH(TRY_CONVERT(DATE, [Dia], 103)) as Mes,
                COUNT(*) as Registos
            FROM f_trafegoc01
            WHERE TRY_CONVERT(DATE, [Dia], 103) IS NOT NULL
            GROUP BY YEAR(TRY_CONVERT(DATE, [Dia], 103)), MONTH(TRY_CONVERT(DATE, [Dia], 103))
            ORDER BY Ano DESC, Mes DESC
            """

            temporal_data = client.execute_query(temporal_query, fetch_as_dict=True)
            print("\n📅 Distribuição temporal (últimos 5 meses):")
            for row in temporal_data:
                print(f"   {row['Ano']}-{row['Mes']:02d}: {row['Registos']:,} registos")

            client.disconnect()

        except Exception as e:
            print(f"❌ Erro na análise de dados: {e}")

    def _check_performance(self):
        """Verificar performance de queries críticas."""
        print("\n⚡ Verificando Performance...")

        try:
            db_config = load_ini_config('CVTVMDWBI')
            client = DatabaseFactory.get_database('sqlserver', db_config)
            client.connect()

            # Teste de performance - contagem
            start_time = datetime.now()

            count_query = "SELECT COUNT(*) as total FROM f_trafegoc01"
            result = client.execute_query(count_query, fetch_as_dict=True)

            count_time = (datetime.now() - start_time).total_seconds()
            total_records = result[0]['total']

            print(f"⏱️ Contagem total: {count_time:.2f}s para {total_records:,} registos")

            # Teste de performance - filtro por data
            start_time = datetime.now()

            filter_query = """
            SELECT COUNT(*) as filtered
            FROM f_trafegoc01
            WHERE TRY_CONVERT(DATE, [Dia], 103) < '2025-06-09'
            """
            result = client.execute_query(filter_query, fetch_as_dict=True)

            filter_time = (datetime.now() - start_time).total_seconds()
            filtered_records = result[0]['filtered']

            print(f"🔍 Filtro por data: {filter_time:.2f}s para {filtered_records:,} registos")

            # Análise de performance
            if count_time > 10:
                print("⚠️ Performance de contagem lenta - considerar índices")
            if filter_time > 30:
                print("⚠️ Performance de filtro lenta - criar índice em [Dia]")

            client.disconnect()

        except Exception as e:
            print(f"❌ Erro no teste de performance: {e}")

    def _check_system_resources(self):
        """Verificar recursos do sistema."""
        print("\n💻 Verificando Recursos do Sistema...")

        try:
            import psutil
            import os

            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            print(f"🖥️ CPU: {cpu_percent}%")

            # Memória
            memory = psutil.virtual_memory()
            print(f"💾 Memória: {memory.percent}% usada ({memory.used // (1024**3):.1f}GB / {memory.total // (1024**3):.1f}GB)")

            # Disco
            disk = psutil.disk_usage('.')
            print(f"💿 Disco: {disk.percent}% usado ({disk.used // (1024**3):.1f}GB / {disk.total // (1024**3):.1f}GB)")

            # Verificar diretório de logs
            if os.path.exists('logs'):
                log_files = os.listdir('logs')
                print(f"📄 Ficheiros de log: {len(log_files)}")
            else:
                print("⚠️ Diretório 'logs' não encontrado")

        except ImportError:
            print("ℹ️ psutil não instalado - saltar verificação de recursos")
        except Exception as e:
            print(f"❌ Erro na verificação de recursos: {e}")

if __name__ == "__main__":
    diagnostics = ETL016Diagnostics()
    diagnostics.run_full_diagnostics()
```

### **📊 Script de Monitorização Contínua**

```python
# monitor.py
import time
import json
from datetime import datetime, timedelta
from traffic_retention import TrafficRetention

class ETL016Monitor:
    """Monitor contínuo para o processo ETL016."""

    def __init__(self):
        self.alerts_sent = set()

    def monitor_logs(self, log_dir='logs', interval=60):
        """Monitorizar logs em tempo real."""

        print(f"🔍 Iniciando monitorização de logs em {log_dir}")
        print(f"⏱️ Intervalo: {interval} segundos")

        while True:
            try:
                self._check_recent_logs(log_dir)
                time.sleep(interval)

            except KeyboardInterrupt:
                print("\n⏹️ Monitorização interrompida")
                break
            except Exception as e:
                print(f"❌ Erro na monitorização: {e}")
                time.sleep(interval)

    def _check_recent_logs(self, log_dir):
        """Verificar logs recentes."""

        try:
            import os
            import glob

            # Encontrar log mais recente
            log_files = glob.glob(f"{log_dir}/agt003dsi_*.log")
            if not log_files:
                return

            latest_log = max(log_files, key=os.path.getctime)

            # Ler últimas linhas
            with open(latest_log, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                recent_lines = lines[-100:]  # Últimas 100 linhas

            # Procurar por padrões críticos
            for line in recent_lines:
                self._analyze_log_line(line)

        except Exception as e:
            print(f"Erro ao verificar logs: {e}")

    def _analyze_log_line(self, line):
        """Analisar linha de log."""

        current_time = datetime.now().strftime('%H:%M:%S')

        # Padrões críticos
        critical_patterns = {
            'ERROR': '🔴',
            'CRITICAL': '💥',
            'WARNING': '🟡',
            'timeout': '⏰',
            'Connection refused': '🚫',
            'rollback': '↩️'
        }

        for pattern, emoji in critical_patterns.items():
            if pattern.lower() in line.lower():
                alert_key = f"{pattern}_{hash(line)}"

                if alert_key not in self.alerts_sent:
                    print(f"{current_time} {emoji} {pattern}: {line.strip()}")
                    self.alerts_sent.add(alert_key)

    def health_check(self):
        """Verificação de saúde do sistema."""

        print("🏥 ETL016 Health Check")
        print("-" * 30)

        try:
            # Simular execução rápida
            with TrafficRetention() as retention:
                # Apenas verificar conectividade
                if retention._validate_table_exists():
                    print("✅ Conectividade: OK")
                else:
                    print("❌ Conectividade: FALHA")

                # Verificar última execução
                last_run = self._check_last_execution()
                if last_run:
                    hours_ago = (datetime.now() - last_run).total_seconds() / 3600
                    print(f"📅 Última execução: {hours_ago:.1f}h atrás")

                    if hours_ago > 25:  # Mais de 25h
                        print("⚠️ Execução atrasada!")
                else:
                    print("❓ Última execução: Desconhecida")

        except Exception as e:
            print(f"❌ Health check falhou: {e}")

    def _check_last_execution(self):
        """Verificar última execução nos logs."""

        try:
            import glob
            import os

            log_files = glob.glob("logs/agt003dsi_*.log")
            if not log_files:
                return None

            # Procurar por padrão de conclusão
            for log_file in sorted(log_files, reverse=True):
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if "PROCESSO CONCLUÍDO COM SUCESSO" in line:
                            # Extrair timestamp do log
                            timestamp_str = line.split(' - ')[0]
                            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')

            return None

        except Exception:
            return None

if __name__ == "__main__":
    import sys

    monitor = ETL016Monitor()

    if len(sys.argv) > 1:
        if sys.argv[1] == "logs":
            monitor.monitor_logs()
        elif sys.argv[1] == "health":
            monitor.health_check()
    else:
        print("Uso: python monitor.py [logs|health]")
```

### **🚀 Scripts de Recuperação**

```python
# recovery.py
import os
import shutil
from datetime import datetime
from traffic_retention import TrafficRetention

class ETL016Recovery:
    """Sistema de recuperação para o ETL016."""

    def backup_configs(self):
        """Backup das configurações."""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = f"backup_{timestamp}"

        try:
            os.makedirs(backup_dir, exist_ok=True)

            # Backup dos ficheiros críticos
            files_to_backup = [
                'configs.json',
                'config.ini',
                'traffic_retention.py',
                'main.py'
            ]

            for file in files_to_backup:
                if os.path.exists(file):
                    shutil.copy2(file, backup_dir)
                    print(f"✅ Backup: {file}")

            print(f"📦 Backup criado em: {backup_dir}")

        except Exception as e:
            print(f"❌ Erro no backup: {e}")

    def restore_configs(self, backup_dir):
        """Restaurar configurações de backup."""

        try:
            if not os.path.exists(backup_dir):
                print(f"❌ Diretório de backup não encontrado: {backup_dir}")
                return

            files = os.listdir(backup_dir)

            for file in files:
                src = os.path.join(backup_dir, file)
                dst = file

                # Confirmar antes de sobrescrever
                if os.path.exists(dst):
                    response = input(f"Sobrescrever {file}? (y/N): ")
                    if response.lower() != 'y':
                        continue

                shutil.copy2(src, dst)
                print(f"✅ Restaurado: {file}")

        except Exception as e:
            print(f"❌ Erro no restore: {e}")

    def emergency_purge(self, cutoff_days=90):
        """Purge de emergência com parâmetros seguros."""

        print(f"🚨 PURGE DE EMERGÊNCIA - {cutoff_days} dias")
        response = input("Confirmar? Esta operação é irreversível! (yes/NO): ")

        if response != "yes":
            print("❌ Operação cancelada")
            return

        try:
            with TrafficRetention() as retention:
                # Override dos safety constraints
                retention.min_records_safety = 100
                retention.retention_months = cutoff_days // 30

                # Executar purge
                metrics = retention.execute_retention_process()

                print(f"✅ Purge concluído: {metrics['records_deleted']:,} registos eliminados")

        except Exception as e:
            print(f"❌ Erro no purge de emergência: {e}")

if __name__ == "__main__":
    import sys

    recovery = ETL016Recovery()

    if len(sys.argv) > 1:
        if sys.argv[1] == "backup":
            recovery.backup_configs()
        elif sys.argv[1] == "restore" and len(sys.argv) > 2:
            recovery.restore_configs(sys.argv[2])
        elif sys.argv[1] == "emergency-purge":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
            recovery.emergency_purge(days)
    else:
        print("Uso: python recovery.py [backup|restore <dir>|emergency-purge <days>]")
```

---

## 📞 Escalar Problemas

### **Níveis de Escalamento**

| Nível       | Tipo de Problema           | Contacto               | SLA |
| ------------ | -------------------------- | ---------------------- | --- |
| **L1** | Configuração, logs       | dsi-suporte-rpa@cvt.cv | 4h  |
| **L2** | Base de dados, performance | dba@cvt.cv             | 8h  |
| **L3** | Infraestrutura, rede       | infra@cvt.cv           | 12h |
| **L4** | Problemas críticos        | manager@cvt.cv         | 2h  |

### **Informações para Escalamento**

Sempre incluir:

* ✅ ID do processo: ETL016
* ✅ Timestamp do erro
* ✅ Logs relevantes (últimas 50 linhas)
* ✅ Configuração atual (sem passwords)
* ✅ Resultados do script de diagnóstico

---

 **💡 Dica** : Execute `python diagnostics.py` antes de escalar problemas para obter informação completa!Sintomas**

```
ERROR: Error connecting to SQL Server Database: (IM002) [Microsoft][ODBC Driver Manager] Data source name not found
```

#### **Causas Possíveis**

1. ODBC Driver não instalado
2. String de conexão incorreta
3. Firewall a bloquear conexão
4. Credenciais inválidas

#### **Soluções**

##### **1. Verificar ODBC Driver**

```bash
# Windows
odbcad32.exe

# Linux
odbcinst -j
dpkg -l | grep odbc
```

##### **2. Instalar ODBC Driver 17**

```bash
# Windows
# Descarregar de: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17

# CentOS/RHEL
curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
yum remove unixODBC-utf16 unixODBC-utf16-devel
ACCEPT_EULA=Y yum install -y msodbcsql17
```

##### **3. Testar Conectividade**

```python
# test_connection.py
import pyodbc

def test_sql_connection():
    try:
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=192.168.81.150,1433;"
            "DATABASE=DMETL;"
            "UID=talend_user;"
            "PWD=Pass#1234;"
        )

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        result = cursor.fetchone()
        print(f"✅ Conexão OK: {result[0]}")

    except Exception as e:
        print(f"❌ Erro: {e}")

test_sql_connection()
```

### **❌ Erro: "Connection timeout"**

#### **Sintomas**

```
ERROR: Query timeout occurred: SQLSTATE_TIMEOUT
```

#### **Soluções**

```python
# Aumentar timeout na configuração
connection_string = (
    "..."
    "timeout=60;"           # Aumentar para 60 segundos
    "Connection Timeout=60;"
)
```

---

## ⚙️ Erros de Configuração

### **❌ Erro: "Configuration file 'configs.json' not found"**

#### **Sintomas**

```
FileNotFoundError: Configuration file 'configs.json' not found
```

#### **Soluções**

##### **1. Verificar Estrutura de Ficheiros**

```bash
# Verificar se o ficheiro existe
ls -la configs.json

# Verificar diretório de trabalho
pwd
python -c "import os; print(os.getcwd())"
```

##### **2. Criar configs.json em Falta**

```bash
# Copiar template
cp configs.json.template configs.json

# Ou criar manualmente
cat > configs.json << 'EOF'
{
    "process": {
        "name": "Retenção de Dados Tráfego",
        "identifier": "ETL016"
    },
    "report": {
        "to": "DMK-BI@cvt.cv"
    },
    "error_report": {
        "to": "alertas.rpa@cvt.cv"
    }
}
EOF
```

### **❌ Erro: "Invalid JSON format"**

#### **Sintomas**

```
ValueError: Invalid JSON format in 'configs.json'
```

#### **Soluções**

##### **1. Validar JSON**

```bash
# Usar jq para validar
jq . configs.json

# Ou Python
python -m json.tool configs.json
```

##### **2. Problemas Comuns de JSON**

```json
// ❌ Vírgula extra
{
    "process": {
        "name": "Test",
    }
}

// ✅ Correto
{
    "process": {
        "name": "Test"
    }
}

// ❌ Aspas simples
{
    'process': 'Test'
}

// ✅ Correto
{
    "process": "Test"
}
```

### **❌ Erro: "Section 'CVTVMDWBI' not found"**

#### **Sintomas**

```
ValueError: Section 'CVTVMDWBI' not found in 'config.ini'
```

#### **Soluções**

##### **1. Verificar Secções Disponíveis**

```python
# debug_config.py
from configparser import ConfigParser

conf = ConfigParser()
conf.read('config.ini')
print("Secções disponíveis:", conf.sections())

for section in conf.sections():
    print(f"\n[{section}]")
    for key, value in conf.items(section):
        print(f"{key} = {value}")
```

##### **2. Corrigir config.ini**

```ini
# Verificar se a secção existe
[CVTVMDWBI]
SERVER=192.168.81.150
PORT=1433
DATABASE=DMETL
LOGIN=talend_user
PASSWORD=Pass#1234
```

---

## ⚡ Problemas de Performance

### **❌ Erro: "Query took too long"**

#### **Sintomas**

```
WARNING: Query took too long (125.34 seconds)
```

#### **Diagnóstico**

##### **1. Analisar Query Plan**

```sql
-- Verificar plano de execução
SET SHOWPLAN_ALL ON
DELETE FROM f_trafegoc01
WHERE TRY_CONVERT(DATE, [Dia], 103) < TRY_CONVERT(DATE, '9/6/2025', 103)
AND [Dia] IS NOT NULL;
```

##### **2. Verificar Índices**

```sql
-- Verificar índices existentes
SELECT
    i.name AS IndexName,
    c.name AS ColumnName,
    i.type_desc
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('f_trafegoc01')
ORDER BY i.name, ic.key_ordinal;
```

#### **Soluções**

##### **1. Criar Índice na Coluna Data**

```sql
-- Criar índice para melhorar performance
CREATE NONCLUSTERED INDEX IX_f_trafegoc01_Dia
ON f_trafegoc01 ([Dia])
WHERE [Dia] IS NOT NULL;
```

##### **2. Implementar Delete por Lotes**

```python
def _execute_purge_batched(self, cutoff_date: datetime, batch_size: int = 10000) -> int:
    """Executar purge em lotes para melhor performance."""

    total_deleted = 0
    cutoff_str = f"{cutoff_date.day}/{cutoff_date.month}/{cutoff_date.year}"

    while True:
        delete_query = f"""
        DELETE TOP ({batch_size}) FROM {self.table_name}
        WHERE TRY_CONVERT(DATE, {self.date_column}, 103) < TRY_CONVERT(DATE, ?, 103)
        AND {self.date_column} IS NOT NULL
        """

        result = self.db_client.execute_query(delete_query, (cutoff_str,))
        deleted_in_batch = result if isinstance(result, int) else 0

        total_deleted += deleted_in_batch
        self.logger.info(f"Eliminados {deleted_in_batch:,} registos neste lote")

        if deleted_in_batch < batch_size:
            break  # Não há mais registos para eliminar

        time.sleep(1)  # Pausa entre lotes

    return total_deleted
```

### **❌ Erro: "Memory allocation failure"**

#### **Sintomas**

```
ERROR: Not enough memory to execute query
```

#### **Soluções**

##### **1. Reduzir Batch Size**

```python
# Reduzir tamanho dos lotes
self.max_delete_batch = 10000  # Em vez de 100000
```

##### **2. Implementar Streaming**

```python
def _get_record_counts_streaming(self, cutoff_date: datetime) -> Tuple[int, int, int]:
    """Contar registos usando cursor streaming."""

    cutoff_str = f"{cutoff_date.day}/{cutoff_date.month}/{cutoff_date.year}"

    # Usar cursor server-side
    query = f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN TRY_CONVERT(DATE, {self.date_column}, 103) < TRY_CONVERT(DATE, ?, 103)
                 THEN 1 ELSE 0 END) as to_delete
    FROM {self.table_name}
    WHERE {self.date_column} IS NOT NULL
    """

    result = self.db_client.execute_query(query, (cutoff_str,), fetch_as_dict=True)
    total = result[0]['total']
    to_delete = result[0]['to_delete']
    to_keep = total - to_delete

    return total, to_keep, to_delete
```

---

## 📧 Erros de Email

### **❌ Erro: "Failed to send email"**

#### **Sintomas**

```
ERROR: Failed to send email: [Errno 111] Connection refused
```

#### **Diagnóstico**

##### **1. Testar Conectividade SMTP**

```python
# test_smtp.py
import smtplib
from email.mime.text import MIMEText

def test_smtp():
    try:
        server = smtplib.SMTP('192.168.83.8', 25)
        server.ehlo()
        print("✅ Conexão SMTP OK")

        # Teste de envio
        msg = MIMEText("Teste ETL016")
        msg['Subject'] = "Teste"
        msg['From'] = "dsi-suporte-rpa@cvt.cv"
        msg['To'] = "test@cvt.cv"

        server.sendmail("dsi-suporte-rpa@cvt.cv", ["test@cvt.cv"], msg.as_string())
        server.quit()
        print("✅ Email enviado")

    except Exception as e:
        print(f"❌ Erro SMTP: {e}")

test_smtp()
```

##### **2. Verificar Firewall**

```bash
# Linux - testar porta SMTP
telnet 192.168.83.8 25

# Windows
Test-NetConnection -ComputerName 192.168.83.8 -Port 25
```

#### **Soluções**

##### **1. Configuração SMTP com Autenticação**

```ini
[SMTP]
server=192.168.83.8
port=587
username=smtp_user
password=smtp_password
use_tls=true
```

##### **2. SMTP com SSL**

```python
def _connect_smtp(self) -> smtplib.SMTP:
    """Conectar com suporte SSL/TLS."""
    try:
        if self.smtp_configs.get('use_ssl', False):
            server = smtplib.SMTP_SSL(self.smtp_configs['server'], int(self.smtp_configs['port']))
        elif self.smtp_configs.get('use_tls', False):
            server = smtplib.SMTP(self.smtp_configs['server'], int(self.smtp_configs['port']))
            server.starttls()
        else:
            server = smtplib.SMTP(self.smtp_configs['server'], int(self.smtp_configs['port']))

        if self.smtp_configs.get('username'):
            server.login(self.smtp_configs['username'], self.smtp_configs['password'])

        return server

    except Exception as e:
        raise SMTPConnectionError(f"Falha na conexão SMTP: {e}")
```

### **❌ Erro: "Email template not found"**

#### **Sintomas**

```
ERROR: Template not found: alert_template.html
```

#### **Soluções**

##### **1. Verificar Estrutura de Templates**

```bash
# Verificar se existe
ls -la template/alert_template.html

# Estrutura esperada
project/
├── template/
│   └── alert_template.html
├── helpers/
│   └── email_sender.py
└── main.py
```

##### **2. Criar Template Básico**

```html
<!-- template/alert_template.html -->
<!DOCTYPE html>
<html>
<head>
    <title>{{html_title}}</title>
</head>
<body>
    <div style="border-left: 4px solid {{alert_color}}; padding: 20px;">
        <h2>{{alert_title}}</h2>
        <p>{{alert_message}}</p>

        {% if table_data %}
        <table border="1">
            <tr>
                {% for header in table_headers %}
                <th>{{header}}</th>
                {% endfor %}
            </tr>
            {% for row in table_data %}
            <tr>
                {% for value in row.values() %}
                <td>{{value}}</td>
                {% endfor %}
            </tr>
            {% endfor %}
        </table>
        {% endif %}

        <p><small>Timestamp: {{timestamp}}</small></p>
    </div>
</body>
</html>
```

---

## 🗄️ Problemas de Dados

### **❌ Erro: "Muito poucos registos iriam permanecer"**

#### **Sintomas**

```
ERROR: Muito poucos registos iriam permanecer (850). Mínimo requerido: 1000
```

#### **
