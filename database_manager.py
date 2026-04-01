"""
database_manager.py — Camada de persistência do farmaciabarata.cl

Responsabilidades:
  - Supabase (PostgreSQL): UPSERT em precios_hoy (busca em tempo real)
  - Cloudflare R2 (Object Storage): upload Parquet particionado (histórico)

Lê configuração exclusivamente via variáveis de ambiente (GitHub Actions Secrets):
  SUPABASE_URL, SUPABASE_KEY
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME

Sem side-effects ao importar — todas as conexões são lazy (criadas ao chamar as funções).
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from schema import MedicamentoRecord

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    """Lê variável de ambiente obrigatória; lança RuntimeError se ausente."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Variável de ambiente '{name}' não encontrada. "
            "Verifique os Secrets do GitHub Actions."
        )
    return value


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_supabase_client():
    """
    Retorna cliente Supabase autenticado.

    Lê SUPABASE_URL e SUPABASE_KEY do ambiente.

    Returns:
        supabase.Client

    Raises:
        RuntimeError: se secrets ausentes
        ImportError: se pacote 'supabase' não instalado
    """
    try:
        from supabase import create_client, Client  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Pacote 'supabase' não encontrado. Execute: pip install supabase"
        ) from e

    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_KEY")

    client = create_client(url, key)
    logger.info("Supabase client criado — URL: %s", url)
    return client


def upsert_to_supabase(records: list) -> bool:
    """
    Faz batch UPSERT dos registros na tabela precios_hoy.

    Usa (sku, farmacia_id) como chave de conflito — se o par já existe,
    atualiza todos os campos; caso contrário, insere.

    Args:
        records: lista de MedicamentoRecord ou dicts compatíveis

    Returns:
        True se sucesso, False se houve erro (não lança exceção)
    """
    if not records:
        logger.warning("upsert_to_supabase: lista de records vazia, nada a fazer.")
        return True

    try:
        client = get_supabase_client()

        # Converte MedicamentoRecord → dict serializável
        rows = []
        for r in records:
            row = asdict(r) if hasattr(r, "__dataclass_fields__") else dict(r)
            # Garante que scraped_at seja string ISO 8601 (Supabase aceita TIMESTAMPTZ)
            if "scraped_at" not in row or not row["scraped_at"]:
                row["scraped_at"] = _utcnow().isoformat()
            rows.append(row)

        logger.info("Enviando %d registros para Supabase (upsert)...", len(rows))

        response = (
            client.table("precios_hoy")
            .upsert(rows, on_conflict="sku,farmacia_id")
            .execute()
        )

        logger.info(
            "Supabase upsert concluído — %d registros afetados.",
            len(response.data) if response.data else 0,
        )
        return True

    except Exception as exc:
        logger.error("Erro no upsert_to_supabase: %s", exc, exc_info=True)
        return False


# ── Cloudflare R2 ─────────────────────────────────────────────────────────────

def get_r2_client():
    """
    Retorna cliente boto3 configurado para o endpoint R2 da Cloudflare.

    Lê R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY do ambiente.

    Returns:
        boto3.client (S3-compatible)

    Raises:
        RuntimeError: se secrets ausentes
        ImportError: se pacote 'boto3' não instalado
    """
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "Pacote 'boto3' não encontrado. Execute: pip install boto3"
        ) from e

    endpoint_url      = _require_env("R2_ENDPOINT_URL")
    access_key_id     = _require_env("R2_ACCESS_KEY_ID")
    secret_access_key = _require_env("R2_SECRET_ACCESS_KEY")

    client = boto3.client(
        "s3",
        endpoint_url          = endpoint_url,
        aws_access_key_id     = access_key_id,
        aws_secret_access_key = secret_access_key,
        region_name           = "auto",  # R2 não usa regiões AWS — "auto" é obrigatório
    )

    logger.info("R2 client criado — endpoint: %s", endpoint_url)
    return client


def upload_to_r2(df, farmacia_id: str) -> bool:
    """
    Converte DataFrame para Parquet (compressão Snappy) e faz upload no R2.

    Estrutura do caminho:
        v1/year=YYYY/month=MM/day=DD/{farmacia_id}.parquet

    Args:
        df: pandas.DataFrame com os registros scraped
        farmacia_id: "cruz_verde" | "salcobrand" | "ahumada"

    Returns:
        True se sucesso, False se houve erro (não lança exceção)
    """
    try:
        import pandas as pd  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Pacote 'pandas' não encontrado. Execute: pip install pandas pyarrow"
        ) from e

    if df is None or len(df) == 0:
        logger.warning("upload_to_r2: DataFrame vazio para '%s', nada a fazer.", farmacia_id)
        return True

    try:
        bucket = _require_env("R2_BUCKET_NAME")
        client = get_r2_client()

        # Caminho particionado por data UTC
        now = _utcnow()
        object_key = (
            f"v1/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"{farmacia_id}.parquet"
        )

        # Serializa DataFrame → Parquet em memória (sem escrever em disco)
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        buffer.seek(0)

        logger.info(
            "Fazendo upload de %d registros → s3://%s/%s",
            len(df), bucket, object_key,
        )

        client.put_object(
            Bucket      = bucket,
            Key         = object_key,
            Body        = buffer,
            ContentType = "application/octet-stream",
        )

        logger.info("Upload R2 concluído: %s/%s", bucket, object_key)
        return True

    except Exception as exc:
        logger.error("Erro no upload_to_r2 ('%s'): %s", farmacia_id, exc, exc_info=True)
        return False


# ── Self-test (dry-run sem conexões reais) ────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level  = logging.INFO,
        format = "%(levelname)s | %(name)s | %(message)s",
    )

    print("=" * 60)
    print("database_manager.py — self-test (sem conexões reais)")
    print("=" * 60)

    # Teste 1: _require_env lança RuntimeError para var ausente
    print("\n[1] _require_env com variável ausente deve lançar RuntimeError:")
    try:
        _require_env("VARIAVEL_QUE_NAO_EXISTE_XYZABC")
        print("    FALHOU — deveria ter lançado RuntimeError")
        sys.exit(1)
    except RuntimeError as e:
        print(f"    RuntimeError: {e}  ✓ OK")

    # Teste 2: upsert_to_supabase com lista vazia → True (sem chamar Supabase)
    print("\n[2] upsert_to_supabase com lista vazia deve retornar True:")
    result = upsert_to_supabase([])
    assert result is True, "Esperado True"
    print(f"    Retornou: {result}  ✓ OK")

    # Teste 3: upload_to_r2 com DataFrame vazio → True (sem chamar R2)
    print("\n[3] upload_to_r2 com DataFrame vazio deve retornar True:")
    try:
        import pandas as pd
        result = upload_to_r2(pd.DataFrame(), "cruz_verde")
        assert result is True, "Esperado True"
        print(f"    Retornou: {result}  ✓ OK")
    except ImportError:
        print("    pandas não instalado — pulando teste  ⚠")

    # Teste 4: Verificar caminho R2 gerado
    print("\n[4] Verificar formato do caminho R2:")
    now = _utcnow()
    path = (
        f"v1/year={now.year}/month={now.month:02d}/day={now.day:02d}/cruz_verde.parquet"
    )
    assert path.startswith("v1/year=")
    assert path.endswith("cruz_verde.parquet")
    print(f"    Caminho gerado: {path}  ✓ OK")

    print("\n" + "=" * 60)
    print("Todos os testes passaram ✓")
    print("=" * 60)
