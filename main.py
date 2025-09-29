"""
Script principal para execução do processo de retenção de dados de tráfego.

Processo: Retenção de Dados Tráfego
ID: ETL026
Descrição: Purge diário que assegura que a tabela "f_trafegoc01" conserva apenas registos de tráfego dos últimos 3 meses.

Autor: Joselito Coutinho - CVT00985
Email: joselito.coutinho@cvt.cv
Data Criação: 2025-09-26
Versão: 2.1.0
"""
import re
import sys
import unicodedata
from datetime import datetime
from helpers.email_sender import EmailSender
from helpers.exception_handler import ExceptionHandler
from helpers.operations import close_connections, ensure_control_table_exists, managed_resources, update_last_processed_date
from helpers.traffic_retention import executar_purge
from helpers.utils import setup_logger, timed

@timed
def main():
    """Função principal do processo ETL."""

    # Initialize the logger manager for tracking and debugging information.
    logger = setup_logger(__name__)

    # Verificar se é execução automática
    auto_mode = len(sys.argv) > 1 and sys.argv[1] == "--auto"

    with managed_resources() as (config, dmkbi_crud, postgresql_crud, dbs):  # Initialize resources

        # Unpack the database connections
        dmkbi_db, postgresql_db = dbs

        # Initialize email sender
        email_sender = EmailSender(config.smtp_configs)

        # Initialize the exception handler for database operations.
        exception_handler = ExceptionHandler(
            crud=postgresql_crud,
            email_sender=email_sender,
            config=config.error_report
        )

        try:

            logger.info("=" * 60)
            logger.info("INICIANDO PROCESSO - RETENÇÃO DE DADOS TRÁFEGO")
            if auto_mode:
                logger.info("MODO: Automático (sem confirmações)")
            logger.info("=" * 60)

            processed_date = datetime.now()

            # Genarate process name
            raw_name = config.process.get('name')

            # Normalizar e remover acentos
            normalized = unicodedata.normalize('NFD', raw_name)
            without_accents = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')

            # Substituir tudo que não é letra, número ou underscore por _
            process_name = re.sub(r'\W+', '_', without_accents.lower()).strip('_') + '_etl'
            logger.info(f"Process name: {process_name}")

            # Ensure control table exists
            ensure_control_table_exists(postgresql_crud, config.table_control)

            # Executar processo de retenção
            purging_result = executar_purge(auto_confirm=auto_mode, crud=dmkbi_crud)

            if purging_result:
                update_last_processed_date(postgresql_crud, processed_date, config.table_control, process_name) # Update the last processed date in the control table
                logger.info("PROCESSO CONCLUÍDO COM SUCESSO")
                return 0
            else:
                logger.error("PROCESSO FALHOU OU FOI CANCELADO")
                return 1

        except KeyboardInterrupt as e:
            logger.warning("Processo interrompido pelo utilizador")
            exception_handler.get_exception(e)  # Store or send the exception details to the reporting system
            return 130

        except Exception as e:
            logger.error(f"Erro crítico : {e}")
            exception_handler.get_exception(e)  # Store or send the exception details to the reporting system
            return 1

        finally:
            close_connections(dmkbi_db, postgresql_db)
            logger.info("=" * 60)
            logger.info("PROCESSO ETL FINALIZADO")
            logger.info("=" * 60)

if __name__ == "__main__":
    sys.exit(main())
