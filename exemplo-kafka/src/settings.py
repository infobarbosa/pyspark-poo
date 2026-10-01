# exemplo-kafka/src/settings.py
"""Carregamento de configuracao da aplicacao.

Espelha o `config/settings.py` do tutorial. Compare com o `settings.py` do
exemplo JDBC: a unica diferenca e a funcao `obter_senha()`, que la existe
porque um banco tem credencial e o nosso broker de aula nao tem.
"""

import logging
import sys
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "settings.yaml"

logger = logging.getLogger(__name__)


def carregar_config(path: Path = CONFIG_PATH) -> dict:
    """Carrega o arquivo de configuracao YAML."""
    with open(path, "r", encoding="utf-8") as arquivo:
        return yaml.safe_load(arquivo)


def configurar_logging() -> None:
    """Aplica a configuracao de logging da aplicacao.

    No tutorial (Passo 8) isto vem do proprio YAML via `dictConfig`. Aqui
    simplificamos para manter o exemplo curto -- o principio e o mesmo:
    QUEM CONFIGURA logging e a aplicacao, nunca os modulos.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
