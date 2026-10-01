# exemplo-sparksession/src/settings.py
"""Carregamento de configuracao da aplicacao.

Espelha o `config/settings.py` do tutorial. Igual ao dos exemplos JDBC e
Kafka -- de proposito: e sempre o mesmo problema, resolvido do mesmo jeito.
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


def obter_perfil(config: dict, nome: str | None = None) -> tuple[str, dict]:
    """Devolve o perfil pedido na linha de comando ou o ativo no YAML."""
    nome_perfil = nome or config["perfil_ativo"]
    return nome_perfil, config["perfis"][nome_perfil]


def configurar_logging() -> None:
    """Aplica a configuracao de logging da aplicacao."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # py4j loga cada chamada a JVM em INFO. Sem isto, o log da sua aplicacao
    # fica ilegivel. Silenciar bibliotecas barulhentas e parte de configurar
    # logging (Passo 8 do tutorial).
    logging.getLogger("py4j").setLevel(logging.WARNING)
