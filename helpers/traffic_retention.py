import time
from datetime import datetime, timedelta
from typing import Any, Dict
from helpers.configuration import load_ini_config
from helpers.database.database_factory import DatabaseFactory
from helpers.database.sqlserver_generic_crud import SQLServerGenericCRUD
from helpers.utils import setup_logger

# Initialize the logger manager for tracking and debugging information.
logger = setup_logger(__name__)

def calcular_data_corte():
    """Calcular 3 meses atrás menos 1 dia sem dependências."""
    hoje = datetime.now()

    # Calcular 3 meses atrás manualmente
    ano = hoje.year
    mes = hoje.month - 3
    dia = hoje.day

    if mes <= 0:
        mes += 12
        ano -= 1

    # Ajustar dia se não existir no mês anterior
    import calendar
    ultimo_dia_mes = calendar.monthrange(ano, mes)[1]
    if dia > ultimo_dia_mes:
        dia = ultimo_dia_mes

    tres_meses_atras = datetime(ano, mes, dia)
    data_corte = tres_meses_atras - timedelta(days=1)

    return data_corte

def executar_purge(auto_confirm=False, crud: SQLServerGenericCRUD=None) -> bool:
    """Purge automático - 3 meses atrás menos 1 dia usando CRUD."""

    # Carregar nome da tabela do config.ini
    db_settings = load_ini_config('DATABASE')
    table_name = db_settings.get('table_tafego', 'f_trafegoc01')

    logger.info(f"Tabela alvo: {table_name}")

    # Calcular data de corte
    data_corte = calcular_data_corte()

    # Extrair componentes da data
    cutoff_year = data_corte.year
    cutoff_month = data_corte.month
    cutoff_day = data_corte.day

    logger.info(f"Data de corte: {cutoff_day}/{cutoff_month}/{cutoff_year}")

    db_client = None

    try:
        # Configurar base de dados
        db_config = load_ini_config('CVTVMDWBI')
        db_client = DatabaseFactory.get_database('sqlserver', db_config)
        db_client.connect()

        # Contagem inicial para informar o utilizador
        logger.info("Analisando registos...")

        count_query = f"""
        SELECT COUNT(*) as to_delete
        FROM {table_name}
        WHERE [Dia] IS NOT NULL
        AND (
            YEAR(CONVERT(DATE, [Dia], 103)) < {cutoff_year}
            OR (YEAR(CONVERT(DATE, [Dia], 103)) = {cutoff_year}
                AND MONTH(CONVERT(DATE, [Dia], 103)) < {cutoff_month})
            OR (YEAR(CONVERT(DATE, [Dia], 103)) = {cutoff_year}
                AND MONTH(CONVERT(DATE, [Dia], 103)) = {cutoff_month}
                AND DAY(CONVERT(DATE, [Dia], 103)) < {cutoff_day})
        )
        """

        count_result = crud.execute_raw_query(count_query)
        records_to_delete = count_result[0]['to_delete'] if count_result else 0

        # Total de registos
        total_query = f"SELECT COUNT(*) as total FROM {table_name}"
        total_result = crud.execute_raw_query(total_query)
        total_records = total_result[0]['total'] if total_result else 0

        logger.info(f"Total na tabela: {total_records:,}")
        logger.info(f"A eliminar: {records_to_delete:,}")
        logger.info(f"A manter: {total_records - records_to_delete:,}")

        if records_to_delete == 0:
            logger.warning("Não há registos para eliminar!")
            return True

        # Confirmação apenas se não for modo automático
        if not auto_confirm:
            resposta = input(f"\nConfirma eliminação de {records_to_delete:,} registos? (SIM/não): ")
            if resposta.upper() != "SIM":
                return False

        # Prosseguir com eliminação usando a função original
        metrics, success = executar_eliminacao(crud, cutoff_year, cutoff_month, cutoff_day, records_to_delete, table_name)

        if success:
            # Enviar relatório detalhado por email
            send_success_report(metrics)
            return True
        else:
            return False

    except Exception as e:
        logger.error(f"Erro: {e}")
        return False

    finally:
        if db_client:
            try:
                db_client.disconnect()
            except:
                pass

def send_success_report(metrics: Dict[str, Any]):
    """
    Enviar relatório de sucesso por email.

    Args:
        metrics (Dict[str, Any]): Métricas do processo
    """
    try:
        from helpers.configuration import load_json_config
        from helpers.email_sender import EmailSender

        config = load_json_config()
        smtp_config = load_ini_config('SMTP')
        email_sender = EmailSender(smtp_config)

        report_config = config.get('report', {})
        process_info = config.get('process', {})

        # Preparar dados para o template
        alert_title = f"Sucesso: {process_info.get('name', 'Processo ETL')}"
        alert_message = f"O processo de retenção de dados foi executado com sucesso. Aqui estão as métricas detalhadas:"

        # Dados tabulares para melhor visualização
        table_data = [
            {'Métrica': 'Registos Totais (inicial)', 'Valor': f"{metrics['total_records']:,}"},
            {'Métrica': 'Registos Eliminados', 'Valor': f"{metrics['records_deleted']:,}"},
            {'Métrica': 'Registos Mantidos', 'Valor': f"{metrics['records_kept']:,}"},
            {'Métrica': 'Data de Corte', 'Valor': metrics['cutoff_date']},
            {'Métrica': 'Tempo de Execução', 'Valor': f"{metrics['execution_time']:.2f}s"},
        ]

        success = email_sender.send_template_email(
            report_config=report_config,
            alert_type='success',
            alert_title=alert_title,
            alert_message=alert_message,
            table_data=table_data,
            environment="PRODUCTION",
            timestamp=datetime.now().isoformat()
        )

        if success:
            logger.info("Relatório de sucesso enviado")
        else:
            logger.warning("Falha ao enviar relatório de sucesso")

    except Exception as e:
        logger.error(f"Erro ao enviar relatório de sucesso: {e}")

def executar_eliminacao(crud, cutoff_year, cutoff_month, cutoff_day, expected_deletes, table_name):
    """Executar eliminação usando CRUD."""

    batch_size = 5000
    total_deleted = 0
    batch_num = 1

    delete_query = f"""
    DELETE TOP ({batch_size})
    FROM {table_name}
    WHERE [Dia] IS NOT NULL
    AND (
        YEAR(CONVERT(DATE, [Dia], 103)) < {cutoff_year}
        OR (YEAR(CONVERT(DATE, [Dia], 103)) = {cutoff_year}
            AND MONTH(CONVERT(DATE, [Dia], 103)) < {cutoff_month})
        OR (YEAR(CONVERT(DATE, [Dia], 103)) = {cutoff_year}
            AND MONTH(CONVERT(DATE, [Dia], 103)) = {cutoff_month}
            AND DAY(CONVERT(DATE, [Dia], 103)) < {cutoff_day})
    )
    """

    start_time = time.time()

    while total_deleted < expected_deletes:
        progress_percent = (total_deleted / expected_deletes) * 100
        logger.info(f"Lote {batch_num} ({progress_percent:.1f}%)... ")

        crud.execute_raw_query(delete_query)

        # Incrementar estimativa
        batch_deleted = min(batch_size, expected_deletes - total_deleted)
        total_deleted += batch_deleted

        logger.info(f"{batch_deleted:,} eliminados")
        batch_num += 1
        time.sleep(0.2)

        if batch_deleted < batch_size:
            break

    elapsed_time = time.time() - start_time

    # Verificação final
    final_query = f"SELECT COUNT(*) as total FROM {table_name}"
    final_result = crud.execute_raw_query(final_query)
    remaining_records = final_result[0]['total'] if final_result else 0

    metrics = {
        'total_records': expected_deletes + remaining_records,
        'records_deleted': total_deleted,
        'records_kept': remaining_records,
        'execution_time': elapsed_time,
        'cutoff_date': f"{cutoff_day}/{cutoff_month}/{cutoff_year}",
    }

    logger.info(f"Eliminados: {total_deleted:,}")
    logger.info(f"Restantes: {remaining_records:,}")
    logger.info(f"Tempo: {elapsed_time:.1f}s")

    return metrics, True
