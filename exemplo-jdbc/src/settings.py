# exemplo-jdbc/src/settings.py
"""Carregamento de configuracao e resolucao de segredos.

Espelha o `config/settings.py` do tutorial: nenhuma outra parte da aplicacao
le arquivo de configuracao nem variavel de ambiente. Tudo passa por aqui.
"""

import logging
import os
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
    # py4j loga cada chamada a JVM em INFO e deixa o log ilegivel.
    logging.getLogger("py4j").setLevel(logging.WARNING)


def obter_senha(nome_variavel: str) -> str:
    """Le a senha do ambiente e falha cedo, com mensagem clara, se faltar.

    Segredo nao entra no codigo, nao entra no YAML e nao entra no git.
    Em producao esta funcao seria trocada por uma chamada ao cofre de
    segredos (AWS Secrets Manager, Vault, etc.) -- e SO ela mudaria.
    """
    senha = os.environ.get(nome_variavel)
    if not senha:
        raise RuntimeError(
            f"Variavel de ambiente '{nome_variavel}' nao definida. "
            f"Execute: export {nome_variavel}='sua-senha'"
        )
    return senha
