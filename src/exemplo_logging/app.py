import logging
import logging.config
import json
from pathlib import Path

from pacote1.processador import ProcessadorDeDados
from pacote2.validador import validar_email

def configurar_logging_de_arquivo(config_path='logging_config.json'):
    """Configura o logging a partir de um arquivo de configuração JSON."""
    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
        logging.info("Logging configurado a partir de arquivo.")
    else:
        # Fallback para configuração básica se o arquivo não existir
        logging.basicConfig(level=logging.INFO)
        logging.warning(f"Arquivo de configuração de log não encontrado em '{config_path}'. Usando config básica.")


# --- Ponto de Entrada da Aplicação ---
if __name__ == "__main__":
    # A primeira coisa a fazer é configurar o log!
    configurar_logging_de_arquivo()

    logging.info("Aplicação iniciada.")

    dados_para_processar = ["item1", "item2", "item3", "item4"]
    processador = ProcessadorDeDados(dados_para_processar)
    processador.processar()

    validar_email("contato@exemplo.com")
    validar_email("usuario_invalido")

    logging.info("Aplicação finalizada.")