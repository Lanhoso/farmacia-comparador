"""
schema.py — Schema canônico para farmaciabarata.cl

Fonte da verdade para todos os scrapers e pipeline de dados.
Define MedicamentoRecord (16 campos), validate_record() e empty_record().

Sem dependências externas — apenas stdlib.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional


# ── Constantes ────────────────────────────────────────────────────────────────

FARMACIAS_VALIDAS = ["cruz_verde", "salcobrand", "ahumada"]


# ── Dataclass ─────────────────────────────────────────────────────────────────

@dataclass
class MedicamentoRecord:
    """Registro canônico de um medicamento scrapeado."""

    # Identificação do produto
    sku:              Optional[str]   # Identificador interno da loja (URL ou meta tag)
    ean_code:         Optional[str]   # Código de barras EAN-13

    # Dados do produto
    nombre_producto:  Optional[str]   # Nome comercial. Ex: "Glaupax"
    principio_activo: Optional[str]   # Composto químico. Ex: "Metformina"
    laboratorio:      Optional[str]   # Fabricante. Ex: "Laboratorio Chile"
    presentacion:     Optional[str]   # Forma farmacêutica. Ex: "Comprimidos recubiertos"
    cantidad:         Optional[int]   # Volume ou unidades
    dosis:            Optional[str]   # Concentração. Ex: "850mg", "5mg/ml"

    # Atributos regulatórios
    is_bioequivalente: bool           # True se houver selo Bioequivalente
    requiere_receta:   bool           # True se venda sob receita

    # Farmácia e preços
    farmacia_id:      str             # "cruz_verde" | "salcobrand" | "ahumada"
    precio_original:  Optional[int]   # Preço de tabela sem desconto (CLP)
    precio_actual:    Optional[int]   # Menor preço disponível (CLP)

    # URLs
    url_product:      Optional[str]   # URL completa da página do produto
    url_image:        Optional[str]   # URL da imagem principal

    # Metadados
    scraped_at:       str             # Timestamp ISO 8601 UTC


# ── Helpers internos ──────────────────────────────────────────────────────────

def _utcnow_iso() -> str:
    """Retorna timestamp UTC atual em formato ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _coerce_str(value) -> Optional[str]:
    """Converte para str limpo ou None (nunca string vazia)."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _coerce_int(value) -> Optional[int]:
    """Converte para int ou None se ausente/inválido."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        # Remove caracteres não numéricos (ex: "$ 5.032" → 5032)
        import re
        digits = re.sub(r"[^\d]", "", str(value))
        return int(digits) if digits else None
    except (ValueError, TypeError):
        return None


def _coerce_bool(value) -> bool:
    """Converte para bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "sí", "si")
    return bool(value)


def _validate_scraped_at(value) -> str:
    """Valida ou gera timestamp ISO 8601 UTC."""
    if not value:
        return _utcnow_iso()
    s = str(value).strip()
    # Verificação básica de formato ISO 8601
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return s
    except ValueError:
        return _utcnow_iso()


# ── API pública ───────────────────────────────────────────────────────────────

def empty_record(farmacia_id: str) -> dict:
    """
    Retorna um dict com todos os 16 campos em valores nulos/default.
    Útil quando o scraper não consegue extrair um campo específico.

    Args:
        farmacia_id: "cruz_verde", "salcobrand" ou "ahumada"

    Returns:
        dict com todos os campos do schema
    """
    fid = farmacia_id.lower().strip()
    if fid not in FARMACIAS_VALIDAS:
        raise ValueError(
            f"farmacia_id inválida: '{farmacia_id}'. "
            f"Valores aceitos: {FARMACIAS_VALIDAS}"
        )
    return {
        "sku":               None,
        "ean_code":          None,
        "nombre_producto":   None,
        "principio_activo":  None,
        "laboratorio":       None,
        "presentacion":      None,
        "cantidad":          None,
        "dosis":             None,
        "is_bioequivalente": False,
        "requiere_receta":   False,
        "farmacia_id":       fid,
        "precio_original":   None,
        "precio_actual":     None,
        "url_product":       None,
        "url_image":         None,
        "scraped_at":        _utcnow_iso(),
    }


def validate_record(record: dict) -> MedicamentoRecord:
    """
    Valida e coerce os tipos de um dict bruto para MedicamentoRecord.

    Regras:
    - farmacia_id: normalizado para lowercase, deve estar em FARMACIAS_VALIDAS
    - Campos str: nunca string vazia (converte para None)
    - Campos int: aceita strings com formatação (ex: "$ 5.032" → 5032)
    - Campos bool: aceita "true"/"false", 0/1, etc.
    - scraped_at: gerado automaticamente se ausente ou inválido
    - precio_actual nunca pode ser maior que precio_original

    Args:
        record: dict com os dados brutos do scraper

    Returns:
        MedicamentoRecord validado e tipado

    Raises:
        ValueError: se farmacia_id inválida ou precio_actual > precio_original
        KeyError: se farmacia_id ausente
    """
    # ── farmacia_id ──
    raw_fid = record.get("farmacia_id")
    if raw_fid is None:
        raise KeyError("'farmacia_id' é obrigatório e está ausente no record")
    farmacia_id = str(raw_fid).lower().strip()
    if farmacia_id not in FARMACIAS_VALIDAS:
        raise ValueError(
            f"farmacia_id inválida: '{raw_fid}'. "
            f"Valores aceitos: {FARMACIAS_VALIDAS}"
        )

    # ── Preços ──
    precio_original = _coerce_int(record.get("precio_original"))
    precio_actual   = _coerce_int(record.get("precio_actual"))

    if precio_actual is not None and precio_original is not None:
        if precio_actual > precio_original:
            raise ValueError(
                f"precio_actual ({precio_actual}) não pode ser maior que "
                f"precio_original ({precio_original})"
            )

    return MedicamentoRecord(
        sku              = _coerce_str(record.get("sku")),
        ean_code         = _coerce_str(record.get("ean_code")),
        nombre_producto  = _coerce_str(record.get("nombre_producto")),
        principio_activo = _coerce_str(record.get("principio_activo")),
        laboratorio      = _coerce_str(record.get("laboratorio")),
        presentacion     = _coerce_str(record.get("presentacion")),
        cantidad         = _coerce_int(record.get("cantidad")),
        dosis            = _coerce_str(record.get("dosis")),
        is_bioequivalente= _coerce_bool(record.get("is_bioequivalente", False)),
        requiere_receta  = _coerce_bool(record.get("requiere_receta", False)),
        farmacia_id      = farmacia_id,
        precio_original  = precio_original,
        precio_actual    = precio_actual,
        url_product      = _coerce_str(record.get("url_product")),
        url_image        = _coerce_str(record.get("url_image")),
        scraped_at       = _validate_scraped_at(record.get("scraped_at")),
    )


def record_to_dict(record: MedicamentoRecord) -> dict:
    """Converte MedicamentoRecord para dict serializável em JSON."""
    return asdict(record)


def infer_from_nombre(nombre_producto: str) -> dict:
    """
    Infere principio_activo, dosis, cantidad e presentacion a partir do
    nombre_producto usando regex — sem acesso à página do produto.

    Útil como fallback quando os scrapers não conseguem extrair esses
    campos das páginas individuais (SPAs com dados não acessíveis via CSS).

    Padrões reconhecidos (exemplos):
        "Metformina 850 mg 30 Comprimidos"
            → principio_activo="Metformina", dosis="850 mg", cantidad=30,
              presentacion="Comprimidos"
        "Glafornil Metformina 850 mg 60 Comprimidos Recubiertos"
            → principio_activo="Metformina" (última palavra antes do número),
              dosis="850 mg", cantidad=60, presentacion="Comprimidos Recubiertos"
        "Amoxicilina 500 mg 21 Cápsulas"
            → principio_activo="Amoxicilina", dosis="500 mg", cantidad=21,
              presentacion="Cápsulas"
        "Ibuprofeno 5mg/ml Solución 100 ml"
            → principio_activo="Ibuprofeno", dosis="5mg/ml", cantidad=100,
              presentacion="Solución"

    Returns:
        dict com chaves: principio_activo, dosis, cantidad, presentacion.
        Campos não encontrados → None.
        Nunca lança exceção.
    """
    import re as _re

    result: dict = {
        "principio_activo": None,
        "dosis":            None,
        "cantidad":         None,
        "presentacion":     None,
    }

    if not nombre_producto or not str(nombre_producto).strip():
        return result

    try:
        nombre = str(nombre_producto).strip()

        # ── dosis ─────────────────────────────────────────────────────────────
        # Captura: número + unidade (mg, mcg, g, ml, UI, %) com opcional /via
        # Ex: "850 mg", "5mg/ml", "500mg", "10 mcg/dosis", "0,5 mg"
        dosis_match = _re.search(
            r"\b(\d+(?:[.,]\d+)?\s*(?:mg|mcg|µg|g(?!\w)|ml|UI|ui|%)"
            r"(?:/(?:ml|g|kg|comp(?:rimido)?|tab(?:leta)?|amp(?:olla)?|dosi?s?))?)",
            nombre, _re.IGNORECASE,
        )
        if dosis_match:
            result["dosis"] = dosis_match.group(1).strip()

        # ── cantidad + presentacion ────────────────────────────────────────────
        # Captura: número seguido de palavra de apresentação farmacêutica
        # Incluir formas compostas ("Comprimidos Recubiertos") via grupo extra
        _PRES = (
            r"comprimidos?\s+recubiertos?|comprimidos?\s+masticables?|"
            r"comprimidos?\s+efervescentes?|comprimidos?\s+dispersables?|"
            r"comprimidos?\s+de\s+liberaci[oó]n\s+prolongada|"
            r"comprimidos?|"
            r"c[aá]psulas?\s+blandas?|c[aá]psulas?|capsulas?|"
            r"tabletas?\s+recubiertas?|tabletas?|grageas?|"
            r"sobres?|ampollas?|viales?|frascos?|"
            r"jarabe|soluci[oó]n|suspensi[oó]n|"
            r"crema|gel|ung[üu]ento|pomada|parche|"
            r"gotas?|spray|aerosol|inhalador|"
            r"supositorios?|[oó]vulos?"
        )
        cant_match = _re.search(
            rf"\b(\d+)\s+({_PRES})\b",
            nombre, _re.IGNORECASE,
        )
        if cant_match:
            result["cantidad"]     = int(cant_match.group(1))
            result["presentacion"] = cant_match.group(2).strip()

        # ── principio_activo ──────────────────────────────────────────────────
        # Estratégia: pegar tudo antes da primeira ocorrência de dígito,
        # depois extrair a ÚLTIMA palavra (ignora nome comercial que precede).
        # Ex: "Glafornil Metformina 850 mg..." → words=["Glafornil","Metformina"]
        #     → última = "Metformina"
        first_digit = _re.search(r"\b\d", nombre)
        if first_digit:
            before = nombre[: first_digit.start()].strip()
            if before:
                words = [w.strip("(),;") for w in before.split() if w.strip("(),;")]
                if words:
                    result["principio_activo"] = words[-1] or None

    except Exception:
        pass  # Tolerante a qualquer falha

    return result


# ── Self-test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    print("=" * 60)
    print("schema.py — self-test")
    print("=" * 60)

    # Teste 1: empty_record
    print("\n[1] empty_record('cruz_verde'):")
    er = empty_record("cruz_verde")
    print(json.dumps(er, indent=2, ensure_ascii=False))
    assert er["farmacia_id"] == "cruz_verde"
    assert er["is_bioequivalente"] is False
    assert er["precio_actual"] is None
    print("    ✓ OK")

    # Teste 2: validate_record com dados completos
    print("\n[2] validate_record — registro completo:")
    raw = {
        "sku": "270505",
        "ean_code": "7802000270505",
        "nombre_producto": "Metformina 850 mg",
        "principio_activo": "Metformina",
        "laboratorio": "Laboratorio Chile",
        "presentacion": "Comprimidos",
        "cantidad": "30",
        "dosis": "850mg",
        "is_bioequivalente": "true",
        "requiere_receta": False,
        "farmacia_id": "Cruz_Verde",   # maiúscula → deve normalizar
        "precio_original": "$ 3.390",  # string com formatação → int 3390
        "precio_actual": "$ 2.712",    # string com formatação → int 2712
        "url_product": "https://www.cruzverde.cl/metformina-850-mg-30/270505.html",
        "url_image": "https://cdn.cruzverde.cl/270505.jpg",
        "scraped_at": None,            # ausente → deve gerar automaticamente
    }
    rec = validate_record(raw)
    assert rec.farmacia_id == "cruz_verde"
    assert rec.precio_original == 3390
    assert rec.precio_actual == 2712
    assert rec.cantidad == 30
    assert rec.is_bioequivalente is True
    assert rec.scraped_at.endswith("Z")
    print(f"    farmacia_id    : {rec.farmacia_id}")
    print(f"    precio_original: {rec.precio_original}")
    print(f"    precio_actual  : {rec.precio_actual}")
    print(f"    is_bioequiv.   : {rec.is_bioequivalente}")
    print(f"    scraped_at     : {rec.scraped_at}")
    print("    ✓ OK")

    # Teste 3: string vazia → None
    print("\n[3] string vazia deve virar None:")
    raw2 = {**raw, "ean_code": "   ", "farmacia_id": "salcobrand"}
    rec2 = validate_record(raw2)
    assert rec2.ean_code is None
    print(f"    ean_code = {rec2.ean_code!r}  ✓ OK")

    # Teste 4: precio_actual > precio_original → ValueError
    print("\n[4] precio_actual > precio_original deve lançar ValueError:")
    try:
        validate_record({**raw, "precio_actual": 9999, "precio_original": 100})
        assert False, "Deveria ter lançado ValueError"
    except ValueError as e:
        print(f"    ValueError: {e}  ✓ OK")

    # Teste 5: farmacia_id inválida → ValueError
    print("\n[5] farmacia_id inválida deve lançar ValueError:")
    try:
        validate_record({**raw, "farmacia_id": "farmacia_xyz"})
        assert False, "Deveria ter lançado ValueError"
    except ValueError as e:
        print(f"    ValueError: {e}  ✓ OK")

    # Teste 6: farmacia_id ausente → KeyError
    print("\n[6] farmacia_id ausente deve lançar KeyError:")
    try:
        raw_no_fid = {k: v for k, v in raw.items() if k != "farmacia_id"}
        validate_record(raw_no_fid)
        assert False, "Deveria ter lançado KeyError"
    except KeyError as e:
        print(f"    KeyError: {e}  ✓ OK")

    # Teste 7: empty_record com farmacia_id inválida → ValueError
    print("\n[7] empty_record com farmacia_id inválida deve lançar ValueError:")
    try:
        empty_record("farmacia_xyz")
        assert False, "Deveria ter lançado ValueError"
    except ValueError as e:
        print(f"    ValueError: {e}  ✓ OK")

    # Teste 8: infer_from_nombre — casos comuns
    print("\n[8] infer_from_nombre — casos variados:")
    cases = [
        (
            "Metformina 850 mg 30 Comprimidos",
            {"principio_activo": "Metformina", "dosis": "850 mg",
             "cantidad": 30, "presentacion": "Comprimidos"},
        ),
        (
            "Glafornil Metformina 850 mg 60 Comprimidos Recubiertos",
            {"principio_activo": "Metformina", "dosis": "850 mg",
             "cantidad": 60, "presentacion": "Comprimidos Recubiertos"},
        ),
        (
            "Amoxicilina 500 mg 21 Cápsulas",
            {"principio_activo": "Amoxicilina", "dosis": "500 mg",
             "cantidad": 21, "presentacion": "Cápsulas"},
        ),
        (
            "Ibuprofeno 5mg/ml Solución 100 ml",
            {"principio_activo": "Ibuprofeno", "dosis": "5mg/ml",
             "cantidad": None, "presentacion": None},
        ),
        (
            "Producto sin datos",
            {"principio_activo": None, "dosis": None,
             "cantidad": None, "presentacion": None},
        ),
    ]
    all_ok = True
    for nombre, expected in cases:
        got = infer_from_nombre(nombre)
        ok = all(got.get(k) == v for k, v in expected.items())
        status = "✓" if ok else "✗"
        print(f"    {status} '{nombre}'")
        if not ok:
            all_ok = False
            for k, v in expected.items():
                if got.get(k) != v:
                    print(f"        {k}: esperado={v!r}, obtido={got.get(k)!r}")
    assert all_ok, "Alguns casos de infer_from_nombre falharam"

    # Teste 9: infer_from_nombre — tolerância a falhas
    print("\n[9] infer_from_nombre com entradas inválidas (nunca lança exceção):")
    for bad in [None, "", "   ", 123, []]:
        r = infer_from_nombre(bad)
        assert r == {"principio_activo": None, "dosis": None, "cantidad": None, "presentacion": None}
    print("    ✓ OK — todas as entradas inválidas retornaram dict com None")

    print("\n" + "=" * 60)
    print("Todos os testes passaram ✓")
    print("=" * 60)
