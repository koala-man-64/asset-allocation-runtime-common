from __future__ import annotations

import re
from collections import defaultdict
from typing import get_args

from asset_allocation_contracts.symbol_identity import (
    SYMBOL_ALIAS_RULES,
    SYMBOL_ALIAS_RULESET_VERSION,
    SymbolIdentityDomain,
    SymbolIdentityProvider,
    SymbolResolutionResult,
)


_VALID_PROVIDERS = frozenset(get_args(SymbolIdentityProvider))
_VALID_DOMAINS = frozenset(get_args(SymbolIdentityDomain))
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9._^:-]{1,32}$")
_PLACEHOLDER_SYMBOLS = frozenset({"-", "--", "N/A", "NA", "NULL", "NONE", "UNKNOWN"})


class SymbolIdentityResolutionError(ValueError):
    """Base error for strict provider/canonical symbol resolution failures."""

    def __init__(self, result: SymbolResolutionResult) -> None:
        message = result.error.message if result.error is not None else f"Symbol resolution failed: {result.status}"
        super().__init__(message)
        self.result = result
        self.provider = result.provider
        self.domain = result.domain
        self.input_symbol = result.inputSymbol
        self.mapping_version = result.mappingVersion


class InvalidSymbolInputError(SymbolIdentityResolutionError):
    """Raised when a symbol is blank, placeholder, or malformed."""


class UnsupportedProviderSymbolError(SymbolIdentityResolutionError):
    """Raised when the provider/domain pair explicitly rejects the symbol."""


class AmbiguousSymbolMappingError(SymbolIdentityResolutionError):
    """Raised when alias rules do not produce a single deterministic mapping."""


def _normalize_provider(provider: str) -> SymbolIdentityProvider:
    normalized = str(provider or "").strip().lower()
    if normalized not in _VALID_PROVIDERS:
        raise ValueError(f"Unsupported symbol identity provider={provider!r}")
    return normalized  # type: ignore[return-value]


def _normalize_domain(domain: str) -> SymbolIdentityDomain:
    normalized = str(domain or "").strip().lower()
    if normalized not in _VALID_DOMAINS:
        raise ValueError(f"Unsupported symbol identity domain={domain!r}")
    return normalized  # type: ignore[return-value]


def _clean_symbol(raw_symbol: object) -> str:
    return str(raw_symbol or "").strip().upper()


def _make_error_result(
    *,
    status: str,
    provider: SymbolIdentityProvider,
    domain: SymbolIdentityDomain,
    input_symbol: str,
    message: str,
) -> SymbolResolutionResult:
    return SymbolResolutionResult(
        status=status,  # type: ignore[arg-type]
        provider=provider,
        domain=domain,
        inputSymbol=input_symbol[:32],
        mappingVersion=SYMBOL_ALIAS_RULESET_VERSION,
        error={
            "code": status,
            "message": message,
            "provider": provider,
            "domain": domain,
            "inputSymbol": input_symbol[:32],
        },
    )


def _raise_invalid(*, provider: SymbolIdentityProvider, domain: SymbolIdentityDomain, symbol: str, reason: str) -> None:
    raise InvalidSymbolInputError(
        _make_error_result(
            status="invalid",
            provider=provider,
            domain=domain,
            input_symbol=symbol,
            message=reason,
        )
    )


def _validate_symbol(provider: SymbolIdentityProvider, domain: SymbolIdentityDomain, symbol: str) -> None:
    if not symbol:
        _raise_invalid(provider=provider, domain=domain, symbol=symbol, reason="Symbol is blank.")
    if symbol in _PLACEHOLDER_SYMBOLS:
        _raise_invalid(provider=provider, domain=domain, symbol=symbol, reason="Symbol is a placeholder.")
    if not _SYMBOL_PATTERN.fullmatch(symbol):
        _raise_invalid(
            provider=provider,
            domain=domain,
            symbol=symbol,
            reason="Symbol contains unsupported characters.",
        )


def _provider_to_canonical_rules() -> dict[tuple[str, str, str], set[str]]:
    out: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for rule in SYMBOL_ALIAS_RULES:
        out[(rule.provider, rule.domain, rule.providerSymbol.upper())].add(rule.canonicalSymbol.upper())
    return dict(out)


def _canonical_to_provider_rules() -> dict[tuple[str, str, str], set[str]]:
    out: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for rule in SYMBOL_ALIAS_RULES:
        out[(rule.provider, rule.domain, rule.canonicalSymbol.upper())].add(rule.providerSymbol.upper())
    return dict(out)


def _unsupported_provider_symbols() -> set[tuple[str, str, str]]:
    unsupported: set[tuple[str, str, str]] = set()
    for rule in SYMBOL_ALIAS_RULES:
        canonical = rule.canonicalSymbol.upper()
        if canonical.startswith("^"):
            unsupported.add((rule.provider, rule.domain, canonical[1:]))
    return unsupported


_PROVIDER_TO_CANONICAL = _provider_to_canonical_rules()
_CANONICAL_TO_PROVIDER = _canonical_to_provider_rules()
_UNSUPPORTED_PROVIDER_SYMBOLS = _unsupported_provider_symbols()


def _looks_like_provider_alias(provider: SymbolIdentityProvider, symbol: str) -> bool:
    return provider == "massive" and ":" in symbol


def canonicalize_provider_symbol(provider: str, domain: str, raw_symbol: object) -> str:
    """Resolve a provider-facing symbol into the canonical medallion symbol."""
    normalized_provider = _normalize_provider(provider)
    normalized_domain = _normalize_domain(domain)
    symbol = _clean_symbol(raw_symbol)
    _validate_symbol(normalized_provider, normalized_domain, symbol)

    key = (normalized_provider, normalized_domain, symbol)
    if key in _UNSUPPORTED_PROVIDER_SYMBOLS:
        raise UnsupportedProviderSymbolError(
            _make_error_result(
                status="unsupported",
                provider=normalized_provider,
                domain=normalized_domain,
                input_symbol=symbol,
                message=f"Unsupported provider symbol {symbol!r} for {normalized_provider}/{normalized_domain}.",
            )
        )

    matches = _PROVIDER_TO_CANONICAL.get(key)
    if matches is None:
        if _looks_like_provider_alias(normalized_provider, symbol):
            raise UnsupportedProviderSymbolError(
                _make_error_result(
                    status="unsupported",
                    provider=normalized_provider,
                    domain=normalized_domain,
                    input_symbol=symbol,
                    message=f"Unknown provider alias {symbol!r} for {normalized_provider}/{normalized_domain}.",
                )
            )
        return symbol
    if len(matches) != 1:
        raise AmbiguousSymbolMappingError(
            _make_error_result(
                status="ambiguous",
                provider=normalized_provider,
                domain=normalized_domain,
                input_symbol=symbol,
                message=f"Ambiguous provider symbol {symbol!r} for {normalized_provider}/{normalized_domain}.",
            )
        )
    return next(iter(matches))


def provider_symbol_for_query(provider: str, domain: str, canonical_symbol: object) -> str:
    """Resolve a canonical medallion symbol into the provider query symbol."""
    normalized_provider = _normalize_provider(provider)
    normalized_domain = _normalize_domain(domain)
    symbol = _clean_symbol(canonical_symbol)
    _validate_symbol(normalized_provider, normalized_domain, symbol)

    matches = _CANONICAL_TO_PROVIDER.get((normalized_provider, normalized_domain, symbol))
    if matches is None:
        return symbol
    if len(matches) != 1:
        raise AmbiguousSymbolMappingError(
            _make_error_result(
                status="ambiguous",
                provider=normalized_provider,
                domain=normalized_domain,
                input_symbol=symbol,
                message=f"Ambiguous canonical symbol {symbol!r} for {normalized_provider}/{normalized_domain}.",
            )
        )
    return next(iter(matches))
