"""
Symbol classification helpers for FxPM.

The broker snapshot is authoritative when it carries an explicit class or MT5
path. Symbol-name inference is a fallback only, used to avoid repeating the
crypto-vs-FX mistake in audit and regime code.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


FIAT_CODES = {
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    "NOK", "SEK", "MXN", "ZAR", "SGD", "PLN", "TRY", "HKD",
    "CNH", "DKK", "CZK", "HUF",
}

ASSET_CLASSES = {"fx", "metal", "energy", "crypto", "index", "equity", "unknown"}

_ASSET_CLASS_ALIASES = {
    "forex": "fx",
    "currency": "fx",
    "currencies": "fx",
    "metals": "metal",
    "metal": "metal",
    "energies": "energy",
    "energy": "energy",
    "oil": "energy",
    "gas": "energy",
    "crypto": "crypto",
    "cryptocurrency": "crypto",
    "cryptocurrencies": "crypto",
    "indices": "index",
    "index": "index",
    "stocks": "equity",
    "stock": "equity",
    "shares": "equity",
    "share": "equity",
    "equities": "equity",
    "equity": "equity",
}

_METAL_PREFIXES = ("XAU", "XAG", "XPT", "XPD")
_ENERGY_PREFIXES = ("XTI", "XBR", "XNG", "WTI", "BRENT")
_CRYPTO_PREFIXES = (
    "BTC", "ETH", "LTC", "XRP", "SOL", "ADA", "DOT", "DOGE", "DOG",
    "BCH", "TRX", "TON", "BNB", "AVAX", "LINK", "MATIC",
)
_INDEX_TICKERS = {
    "SP500", "SPX500", "US500", "US30", "US100", "NAS100", "USTEC",
    "DAX", "DE30", "DE40", "GER30", "GER40", "FTSE", "UK100",
    "NIKKEI", "JP225", "AUS200", "AU200", "HK50", "CHINA50",
    "EU50", "ES35", "FR40",
}


def normalize_symbol(symbol: Optional[str]) -> str:
    """Return a suffix-stripped, alphanumeric uppercase symbol key."""
    if symbol is None:
        return ""
    raw = str(symbol).strip().upper()
    if not raw:
        return ""
    raw = raw.split(".")[0].split("#")[0]
    return "".join(ch for ch in raw if ch.isalnum())


def _coerce_asset_class(value: Any) -> Optional[str]:
    if value is None:
        return None
    key = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    if not key:
        return None
    key = _ASSET_CLASS_ALIASES.get(key, key)
    if key in ASSET_CLASSES and key != "unknown":
        return key
    return None


def _broker_text(broker_data: Optional[Dict[str, Any]]) -> str:
    if not isinstance(broker_data, dict):
        return ""
    parts = []
    for key in ("path", "group", "category", "description", "sector", "industry"):
        value = broker_data.get(key)
        if value:
            parts.append(str(value))
    return " ".join(parts).lower()


def is_fiat_fx_symbol(symbol: Optional[str]) -> bool:
    """True only for fiat/fiat symbols; crypto crosses such as BTCETH are false."""
    sym = normalize_symbol(symbol)
    return len(sym) == 6 and sym[:3] in FIAT_CODES and sym[3:] in FIAT_CODES


def infer_asset_class(
    symbol: Optional[str],
    broker_data: Optional[Dict[str, Any]] = None,
    *,
    default: str = "unknown",
) -> str:
    """Infer an asset class from explicit broker metadata, path, then symbol.

    `default` lets regime code preserve its historical FX fallback while broker
    readiness/spec code can keep unknown symbols visibly unknown.
    """
    explicit = _coerce_asset_class((broker_data or {}).get("asset_class"))
    if explicit:
        return explicit

    sym = normalize_symbol(symbol)
    text = _broker_text(broker_data)

    if sym.startswith(_METAL_PREFIXES) or sym in {"GOLD", "SILVER"}:
        return "metal"
    if sym.startswith(_ENERGY_PREFIXES):
        return "energy"
    if sym.startswith(_CRYPTO_PREFIXES):
        return "crypto"
    if sym in _INDEX_TICKERS or "INDEX" in sym:
        return "index"
    if is_fiat_fx_symbol(sym):
        return "fx"

    if text:
        if any(token in text for token in ("crypto", "coin")):
            return "crypto"
        if any(token in text for token in ("metal", "gold", "silver")):
            return "metal"
        if any(token in text for token in ("energy", "oil", "gas", "brent", "wti")):
            return "energy"
        if any(token in text for token in ("indice", "index")):
            return "index"
        if any(token in text for token in ("stock", "share", "equity", "nyse", "nasdaq")):
            return "equity"
        if any(token in text for token in ("forex", "currency", "currencies", "fx")):
            return "fx"

    resolved_default = _coerce_asset_class(default)
    return resolved_default or ("unknown" if str(default).lower() == "unknown" else "unknown")
