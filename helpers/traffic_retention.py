import re
import calendar
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from helpers.configuration import load_json_config, load_ini_config
from helpers.database.database_factory import DatabaseFactory
from helpers.database.sqlserver_generic_crud import SQLServerGenericCRUD
from helpers.email_sender import EmailSender
from helpers.exception_handler import ExceptionHandler
from helpers.utils import setup_logger, timed

class TrafficRetention:
    """
    Classe responsável pela retenção de dados de tráfego.
    Remove registos com mais de 3 meses da tabela f_trafegoc01.
    """

    def __init__(self):
        """Inicializar o processo de retenção de dados."""
        self.logger = setup_logger(__name__)
        self.config = None
        self.db_config = None
        self.db_client = None
        self.crud = None
        self.email_sender = None
        self.exception_handler = None

        # Configurações do processo
        self.table_name = "f_trafegoc01"
        self.date_column = "[Dia]"
        self.retention_months = 3
        self.min_records_safety = 1000
        self.max_delete_batch = 100000

        self._initialize_components()

    def _initialize_components(self):
        """Inicializar todos os componentes necessários."""
        try:
            # Carregar configurações
            self.config = load_json_config()
            self.db_config = load_ini_config('CVTVMDWBI')
            smtp_config = load_ini_config('SMTP')

            # Inicializar cliente de base de dados
            self.db_client = DatabaseFactory.get_database('sqlserver', self.db_config)
            self.db_client.connect()
            self.crud = SQLServerGenericCRUD(self.db_client)

            # Inicializar email sender
            self.email_sender = EmailSender(smtp_config)

            # Inicializar exception handler
            self.exception_handler = ExceptionHandler(
                crud=self.crud,
                email_sender=self.email_sender,
                config=self.config.get('error_report', {})
            )

            self.logger.info("Componentes inicializados com sucesso")

        except Exception as e:
            self.logger.error(f"Erro na inicialização dos componentes: {e}")
            raise

    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """
        Converter string de data no formato d/m/yyyy para datetime.

        Args:
            date_str (str): Data no formato 7/9/2025 ou 07/09/2025

        Returns:
            datetime ou None se inválida
        """
        if not date_str or not isinstance(date_str, str):
            return None

        try:
            # Limpar a string
            cleaned_date = date_str.strip()

            # Padrão para d/m/yyyy ou dd/mm/yyyy
            pattern = r'^(\d{1,2})/(\d{1,2})/(\d{4})$'
            match = re.match(pattern, cleaned_date)

            if not match:
                return None

            day, month, year = map(int, match.groups())
            return datetime(year, month, day)

        except (ValueError, AttributeError) as e:
            self.logger.warning(f"Erro ao converter data '{date_str}': {e}")
            return None

    def _calculate_cutoff_date(self) -> datetime:
        """
        Calcular a data de corte (3 meses atrás).

        Returns:
            datetime: Data limite para manter registos
        """
        today = datetime.now()

        # Calcular 3 meses atrás mantendo o dia atual
        if today.month > self.retention_months:
            cutoff_month = today.month - self.retention_months
            cutoff_year = today.year
        else:
            cutoff_month = 12 - (self.retention_months - today.month)
            cutoff_year = today.year - 1

        # Manter o dia atual, mas ajustar se não existir no mês de destino
        try:
            cutoff_date = datetime(cutoff_year, cutoff_month, today.day)
        except ValueError:
            # Se o dia não existir no mês (ex: 31 em fevereiro), usar último dia do mês
            last_day = calendar.monthrange(cutoff_year, cutoff_month)[1]
            cutoff_date = datetime(cutoff_year, cutoff_month, last_day)

        self.logger.info(f"Data de corte calculada: {cutoff_date.strftime('%d/%m/%Y')}")
        return cutoff_date

    def _validate_table_exists(self) -> bool:
        """
        Verificar se a tabela existe na base de dados.

        Returns:
            bool: True se a tabela existe
        """
        try:
            exists = self.crud.table_exists(self.table_name)
            if exists:
                self.logger.info(f"Tabela {self.table_name} encontrada")
            else:
                self.logger.error(f"Tabela {self.table_name} não encontrada")
            return exists
        except Exception as e:
            self.logger.error(f"Erro ao verificar existência da tabela: {e}")
            return False

    def _get_record_counts(self, cutoff_date: datetime) -> Tuple[int, int, int]:
        """
        Obter contagens de registos total, a manter e a eliminar.

        Args:
            cutoff_date (datetime): Data de corte

        Returns:
            Tuple[int, int, int]: (total, manter, eliminar)
        """
        try:
            # Contagem total
            total_query = f"SELECT COUNT(*) as total FROM {self.table_name}"
            total_result = self.db_client.execute_query(total_query, fetch_as_dict=True)
            total_records = total_result[0]['total'] if total_result else 0

            # Contagem a eliminar (registos antigos)
            # Usar formato compatível com Windows (sem %-d)
            day = cutoff_date.day
            month = cutoff_date.month
            year = cutoff_date.year
            cutoff_str = f"{day}/{month}/{year}"  # Formato d/m/yyyy sem zeros

            delete_query = f"""
            SELECT COUNT(*) as to_delete
            FROM {self.table_name}
            WHERE TRY_CONVERT(DATE, {self.date_column}, 103) < TRY_CONVERT(DATE, ?, 103)
            AND {self.date_column} IS NOT NULL
            """
            delete_result = self.db_client.execute_query(delete_query, (cutoff_str,), fetch_as_dict=True)
            records_to_delete = delete_result[0]['to_delete'] if delete_result else 0

            records_to_keep = total_records - records_to_delete

            self.logger.info(f"Registos - Total: {total_records:,}, Manter: {records_to_keep:,}, Eliminar: {records_to_delete:,}")

            return total_records, records_to_keep, records_to_delete

        except Exception as e:
            self.logger.error(f"Erro ao obter contagens: {e}")
            raise

    def _validate_safety_constraints(self, records_to_keep: int, records_to_delete: int) -> bool:
        """
        Validar restrições de segurança antes do delete.

        Args:
            records_to_keep (int): Registos que vão permanecer
            records_to_delete (int): Registos a eliminar

        Returns:
            bool: True se é seguro proceder
        """
        # Verificar se ficam registos suficientes
        if records_to_keep < self.min_records_safety:
            self.logger.error(
                f"Muito poucos registos iriam permanecer ({records_to_keep:,}). "
                f"Mínimo requerido: {self.min_records_safety:,}"
            )
            return False

        # Verificar se não estamos a eliminar demasiados de uma vez
        if records_to_delete > self.max_delete_batch:
            self.logger.error(
                f"Demasiados registos para eliminar numa operação ({records_to_delete:,}). "
                f"Máximo permitido: {self.max_delete_batch:,}"
            )
            return False

        # Verificar se há registos para eliminar
        if records_to_delete == 0:
            self.logger.info("Nenhum registo antigo encontrado para eliminar")
            return False

        return True

    @timed
    def _execute_purge(self, cutoff_date: datetime) -> int:
        """
        Executar o purge dos dados antigos.

        Args:
            cutoff_date (datetime): Data de corte

        Returns:
            int: Número de registos eliminados
        """
        try:
            self.logger.info("Iniciando transação para purge de dados")

            # Começar transação
            self.db_client.begin_transaction()

            try:
                cutoff_str = f"{cutoff_date.day}/{cutoff_date.month}/{cutoff_date.year}"

                # Query de delete
                delete_query = f"""
                DELETE FROM {self.table_name}
                WHERE TRY_CONVERT(DATE, {self.date_column}, 103) < TRY_CONVERT(DATE, ?, 103)
                AND {self.date_column} IS NOT NULL
                """

                # Executar delete
                result = self.db_client.execute_query(delete_query, (cutoff_str,))
                records_deleted = result if isinstance(result, int) else 0

                # Commit da transação
                self.db_client.commit_transaction()

                self.logger.info(f"Purge concluído com sucesso. Registos eliminados: {records_deleted:,}")
                return records_deleted

            except Exception as e:
                # Rollback em caso de erro
                self.db_client.rollback_transaction()
                self.logger.error(f"Erro durante o purge, rollback executado: {e}")
                raise

        except Exception as e:
            self.logger.error(f"Erro na transação de purge: {e}")
            raise

    def _send_success_report(self, metrics: Dict[str, Any]):
        """
        Enviar relatório de sucesso por email.

        Args:
            metrics (Dict[str, Any]): Métricas do processo
        """
        try:
            report_config = self.config.get('report', {})
            process_info = self.config.get('process', {})

            # Preparar dados para o template
            alert_title = f"Sucesso: {process_info.get('name', 'Processo ETL')}"
            alert_message = f"""
            O processo de retenção de dados foi executado com sucesso.

            Resumo da operação:
            • Registos eliminados: {metrics['records_deleted']:,}
            • Registos mantidos: {metrics['records_kept']:,}
            • Tempo de execução: {metrics['execution_time']:.2f} segundos
            • Data de corte: {metrics['cutoff_date']}
            """

            # Dados tabulares para melhor visualização
            table_data = [
                {'Métrica': 'Registos Totais (inicial)', 'Valor': f"{metrics['total_records']:,}"},
                {'Métrica': 'Registos Eliminados', 'Valor': f"{metrics['records_deleted']:,}"},
                {'Métrica': 'Registos Mantidos', 'Valor': f"{metrics['records_kept']:,}"},
                {'Métrica': 'Data de Corte', 'Valor': metrics['cutoff_date']},
                {'Métrica': 'Tempo de Execução', 'Valor': f"{metrics['execution_time']:.2f}s"},
            ]

            success = self.email_sender.send_template_email(
                report_config=report_config,
                alert_type='success',
                alert_title=alert_title,
                alert_message=alert_message,
                table_data=table_data,
                environment=process_info.get('identifier', 'ETL016'),
                timestamp=datetime.now().isoformat()
            )

            if success:
                self.logger.info("Relatório de sucesso enviado")
            else:
                self.logger.warning("Falha ao enviar relatório de sucesso")

        except Exception as e:
            self.logger.error(f"Erro ao enviar relatório de sucesso: {e}")

    def _send_error_report(self, error: Exception, context: str = ""):
        """
        Enviar relatório de erro por email.

        Args:
            error (Exception): Exceção ocorrida
            context (str): Contexto adicional do erro
        """
        try:
            error_info = self.exception_handler.get_exception(error, send_email=False)

            # Usar o exception handler para envio consistente
            self.exception_handler.send_error_report(error_info)

            self.logger.info("Relatório de erro enviado")

        except Exception as e:
            self.logger.error(f"Erro ao enviar relatório de erro: {e}")

    @timed
    def execute_retention_process(self) -> Dict[str, Any]:
        """
        Executar o processo completo de retenção de dados.

        Returns:
            Dict[str, Any]: Métricas do processo executado
        """
        start_time = datetime.now()
        metrics = {
            'success': False,
            'start_time': start_time.isoformat(),
            'records_deleted': 0,
            'execution_time': 0.0
        }

        try:
            self.logger.info("=== INICIANDO PROCESSO DE RETENÇÃO DE DADOS ===")

            # 1. Validar tabela
            if not self._validate_table_exists():
                raise Exception(f"Tabela {self.table_name} não encontrada")

            # 2. Calcular data de corte
            cutoff_date = self._calculate_cutoff_date()
            metrics['cutoff_date'] = cutoff_date.strftime('%d/%m/%Y')

            # 3. Obter contagens
            total_records, records_to_keep, records_to_delete = self._get_record_counts(cutoff_date)
            metrics.update({
                'total_records': total_records,
                'records_to_keep': records_to_keep,
                'records_to_delete': records_to_delete
            })

            # 4. Validar restrições de segurança
            if not self._validate_safety_constraints(records_to_keep, records_to_delete):
                metrics['success'] = True  # Não é erro, apenas não há trabalho
                metrics['records_kept'] = total_records
                self.logger.info("Processo concluído - nenhuma ação necessária")
                return metrics

            # 5. Executar purge
            records_deleted = self._execute_purge(cutoff_date)
            metrics['records_deleted'] = records_deleted
            metrics['records_kept'] = total_records - records_deleted

            # 6. Calcular tempo de execução
            end_time = datetime.now()
            metrics['execution_time'] = (end_time - start_time).total_seconds()
            metrics['end_time'] = end_time.isoformat()
            metrics['success'] = True

            self.logger.info("=== PROCESSO DE RETENÇÃO CONCLUÍDO COM SUCESSO ===")

            # 7. Enviar relatório de sucesso
            self._send_success_report(metrics)

            return metrics

        except Exception as e:
            # Calcular tempo até ao erro
            metrics['execution_time'] = (datetime.now() - start_time).total_seconds()
            metrics['error'] = str(e)

            self.logger.error(f"Erro no processo de retenção: {e}")

            # Enviar relatório de erro
            self._send_error_report(e, "processo_retencao")

            raise

        finally:
            # Cleanup
            if self.db_client:
                try:
                    self.db_client.disconnect()
                except Exception as e:
                    self.logger.warning(f"Erro ao desconectar da base de dados: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit com cleanup."""
        if self.db_client:
            try:
                self.db_client.disconnect()
            except Exception as e:
                self.logger.warning(f"Erro no cleanup: {e}")
