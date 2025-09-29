import time
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple
from tqdm import tqdm

from helpers.configuration import load_ini_config
from helpers.database.database_factory import DatabaseFactory
from helpers.database.sqlserver_generic_crud import SQLServerGenericCRUD
from helpers.utils import setup_logger

# Initialize the logger manager for tracking and debugging information.
logger = setup_logger(__name__)


def calcular_data_corte(meses: int = 3) -> datetime:
    """
    Calcular data de corte baseada em meses atrás menos 1 dia.

    Args:
        meses: Número de meses a subtrair (default: 3)

    Returns:
        Data de corte calculada
    """
    hoje = datetime.now()

    # Calcular meses atrás
    ano = hoje.year
    mes = hoje.month - meses
    dia = hoje.day

    while mes <= 0:
        mes += 12
        ano -= 1

    # Ajustar dia se não existir no mês calculado
    import calendar
    ultimo_dia_mes = calendar.monthrange(ano, mes)[1]
    if dia > ultimo_dia_mes:
        dia = ultimo_dia_mes

    data_base = datetime(ano, mes, dia)
    data_corte = data_base - timedelta(days=1)

    logger.debug(f"Cálculo: hoje={hoje.strftime('%d/%m/%Y')}, "
                f"base={data_base.strftime('%d/%m/%Y')}, "
                f"corte={data_corte.strftime('%d/%m/%Y')}")

    return data_corte


def _construir_query_eliminacao(table_name: str, batch_size: int,
                                 cutoff_year: int, cutoff_month: int,
                                 cutoff_day: int) -> str:
    """
    Construir query SQL para eliminação de registos.

    Args:
        table_name: Nome da tabela
        batch_size: Tamanho do lote
        cutoff_year: Ano de corte
        cutoff_month: Mês de corte
        cutoff_day: Dia de corte

    Returns:
        Query SQL formatada
    """
    return f"""
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


def _analisar_registos(crud: SQLServerGenericCRUD, table_name: str,
                       data_corte: datetime) -> Tuple[int, int]:
    """
    Analisar quantidade de registos a eliminar e total.

    Args:
        crud: Instância do CRUD
        table_name: Nome da tabela
        data_corte: Data de corte

    Returns:
        Tupla (total_records, records_to_delete)
    """
    logger.info("Analisando registos...")

    cutoff_year = data_corte.year
    cutoff_month = data_corte.month
    cutoff_day = data_corte.day

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

    return total_records, records_to_delete


def executar_eliminacao(crud: SQLServerGenericCRUD, cutoff_year: int,
                       cutoff_month: int, cutoff_day: int,
                       expected_deletes: int, table_name: str,
                       batch_size: int = 5000) -> Tuple[Dict[str, Any], bool]:
    """
    Executar eliminação de registos em lotes com barra de progresso.

    Args:
        crud: Instância do CRUD
        cutoff_year: Ano de corte
        cutoff_month: Mês de corte
        cutoff_day: Dia de corte
        expected_deletes: Número esperado de eliminações
        table_name: Nome da tabela
        batch_size: Tamanho do lote (default: 5000)

    Returns:
        Tupla (metrics, success)
    """
    total_deleted = 0

    delete_query = _construir_query_eliminacao(
        table_name, batch_size, cutoff_year, cutoff_month, cutoff_day
    )

    start_time = time.time()

    # Barra de progresso com informação detalhada
    with tqdm(
        total=expected_deletes,
        desc="Eliminando registos",
        unit="reg",
        unit_scale=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    ) as pbar:

        while total_deleted < expected_deletes:
            try:
                crud.execute_raw_query(delete_query)

                # Incrementar estimativa
                batch_deleted = min(batch_size, expected_deletes - total_deleted)
                total_deleted += batch_deleted

                # Atualizar barra de progresso
                pbar.update(batch_deleted)

                # Pausa para não sobrecarregar o servidor
                time.sleep(0.2)

                # Se eliminou menos que o batch_size, não há mais registos
                if batch_deleted < batch_size:
                    break

            except Exception as e:
                logger.error(f"Erro durante eliminação: {e}")
                pbar.close()
                return {}, False

    elapsed_time = time.time() - start_time

    try:
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

    except Exception as e:
        logger.error(f"Erro na verificação final: {e}")
        return {}, False


def send_success_report(metrics: Dict[str, Any]) -> None:
    """
    Enviar relatório de sucesso por email.

    Args:
        metrics: Métricas do processo
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
        alert_message = (
            "O processo de retenção de dados foi executado com sucesso. "
            "Aqui estão as métricas detalhadas:"
        )

        # Dados tabulares para melhor visualização
        table_data = [
            {
                'Métrica': 'Registos Totais (inicial)',
                'Valor': f"{metrics['total_records']:,}"
            },
            {
                'Métrica': 'Registos Eliminados',
                'Valor': f"{metrics['records_deleted']:,}"
            },
            {
                'Métrica': 'Registos Mantidos',
                'Valor': f"{metrics['records_kept']:,}"
            },
            {
                'Métrica': 'Data de Corte',
                'Valor': metrics['cutoff_date']
            },
            {
                'Métrica': 'Tempo de Execução',
                'Valor': f"{metrics['execution_time']:.2f}s"
            },
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


def executar_purge(auto_confirm: bool = False,
                   crud: SQLServerGenericCRUD = None) -> bool:
    """
    Purge automático - 3 meses atrás menos 1 dia usando CRUD.

    Args:
        auto_confirm: Se True, não pede confirmação do utilizador
        crud: Instância do CRUD (se None, será criada)

    Returns:
        True se sucesso, False caso contrário
    """
    # Carregar nome da tabela do config.ini
    db_settings = load_ini_config('DATABASE')
    table_name = db_settings.get('table_tafego', 'f_trafegoc01')

    logger.info(f"Tabela alvo: {table_name}")

    # Calcular data de corte
    data_corte = calcular_data_corte()

    logger.info(f"Data de corte: {data_corte.strftime('%d/%m/%Y')}")

    db_client = None

    try:
        # Configurar base de dados se necessário
        if crud is None:
            db_config = load_ini_config('CVTVMDWBI')
            db_client = DatabaseFactory.get_database('sqlserver', db_config)
            db_client.connect()
            crud = SQLServerGenericCRUD(db_client)

        # Analisar registos
        total_records, records_to_delete = _analisar_registos(
            crud, table_name, data_corte
        )

        logger.info(f"Total na tabela: {total_records:,}")
        logger.info(f"A eliminar: {records_to_delete:,}")
        logger.info(f"A manter: {total_records - records_to_delete:,}")

        if records_to_delete == 0:
            logger.warning("Não há registos para eliminar")
            return True

        # Confirmação apenas se não for modo automático
        if not auto_confirm:
            resposta = input(
                f"\nConfirma eliminação de {records_to_delete:,} registos? (SIM/não): "
            )
            if resposta.upper() != "SIM":
                logger.info("Operação cancelada pelo utilizador")
                return False

        # Executar eliminação
        metrics, success = executar_eliminacao(
            crud,
            data_corte.year,
            data_corte.month,
            data_corte.day,
            records_to_delete,
            table_name
        )

        if success:
            send_success_report(metrics)
            return True
        else:
            logger.error("Falha durante eliminação")
            return False

    except Exception as e:
        logger.error(f"Erro durante purge: {e}", exc_info=True)
        return False

    finally:
        if db_client:
            try:
                db_client.disconnect()
            except Exception as e:
                logger.warning(f"Erro ao desconectar: {e}")
