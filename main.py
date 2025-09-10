#!/usr/bin/env python3
"""
Script principal para execução do processo de retenção de dados de tráfego.

Processo: Retenção de Dados Tráfego
ID: ETL016
Descrição: Purge diário que assegura que a tabela f_trafegoc01 conserva
           apenas registos de tráfego dos últimos 3 meses.

Autor: Sistema ETL CVT
Versão: 0.1.0
"""

import sys
import os
from pathlib import Path

# Adicionar o diretório do projeto ao Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from helpers.traffic_retention import TrafficRetention
from helpers.utils import setup_logger

def main():
    """Função principal do processo ETL."""
    logger = setup_logger('main')

    try:
        logger.info("=" * 60)
        logger.info("INICIANDO PROCESSO - RETENÇÃO DE DADOS TRÁFEGO")
        logger.info("=" * 60)

        # Executar processo de retenção usando context manager
        with TrafficRetention() as retention_process:
            metrics = retention_process.execute_retention_process()

            # Log final com resumo
            if metrics['success']:
                logger.info(f"✓ PROCESSO CONCLUÍDO COM SUCESSO")
                logger.info(f"  • Registos eliminados: {metrics['records_deleted']:,}")
                logger.info(f"  • Registos mantidos: {metrics.get('records_kept', 0):,}")
                logger.info(f"  • Tempo execução: {metrics['execution_time']:.2f}s")
                exit_code = 0
            else:
                logger.error("✗ PROCESSO FALHOU")
                exit_code = 1

    except KeyboardInterrupt:
        logger.warning("Processo interrompido pelo utilizador")
        exit_code = 130

    except Exception as e:
        logger.error(f"Erro crítico no processo principal: {e}")
        logger.error("Verifique os logs detalhados e configurações")
        exit_code = 1

    finally:
        logger.info("=" * 60)
        logger.info("PROCESSO ETL016 FINALIZADO")
        logger.info("=" * 60)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
