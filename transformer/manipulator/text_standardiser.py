"""
Cleans up the text columns: whitespace, casing, postal codes.

Every text column gets trimmed and internal whitespace collapsed, because
'Main  St ' and 'Main St' are the same street and should group together.

Casing is where I am deliberately inconsistent, and it is worth explaining.
Geographic and address text gets title cased so it groups. Province and country
are two character codes so they go upper. Business names are left exactly as
they arrive.

That last one is the decision people argue about. Title casing 'MILANO GLOBAL
DEVELOPMENT CORP.' gives you 'Milano Global Development Corp.', which looks
tidier and is wrong: it is not the registered name, and the same rule turns
'ABC Holdings ULC' into 'Abc Holdings Ulc'. A business name is identity, so it
keeps whatever the registry has. For matching and grouping there is a derived
business_name_key, which is uppercased with punctuation stripped. That way you
get a join key without corrupting the real value.
"""

from __future__ import annotations

import logging

import pandas as pd

from transformer.configs import business_licences_configs as cfg

log = logging.getLogger(__name__)


def _clean_whitespace(series: pd.Series) -> pd.Series:
    return (series.astype("string")
                  .str.strip()
                  .str.replace(r"\s+", " ", regex=True)
                  .replace("", pd.NA))


def standardise_text(frame: pd.DataFrame) -> pd.DataFrame:
    """Trim, collapse and case the text columns, and build the name join key."""
    out = frame.copy()

    text_columns = (cfg.TITLE_CASE_COLUMNS + cfg.UPPER_CASE_COLUMNS
                    + cfg.PRESERVE_CASE_COLUMNS + (cfg.POSTAL_CODE_COLUMN,)
                    + ("unit", "house"))
    for name in text_columns:
        if name in out.columns:
            out[name] = _clean_whitespace(out[name])

    for name in cfg.TITLE_CASE_COLUMNS:
        if name in out.columns:
            out[name] = out[name].str.title()

    for name in cfg.UPPER_CASE_COLUMNS:
        if name in out.columns:
            out[name] = out[name].str.upper()

    if cfg.NAME_KEY_SOURCE in out.columns:
        out[cfg.NAME_KEY_COLUMN] = (
            out[cfg.NAME_KEY_SOURCE]
            .str.upper()
            .str.replace(r"[^A-Z0-9 ]", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .replace("", pd.NA)
        )

    return out


def standardise_postal_codes(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Normalise to 'A1A 1A1' and flag anything that is not a Canadian postal code.

    Returns the frame plus how many failed the pattern.

    I keep the cleaned value either way rather than nulling it. An out of town
    licence holder can legitimately have a US zip or something else entirely, and
    throwing that away to make the column tidy loses real information. The row
    carries a flag instead, so anyone who needs strictly Canadian codes can
    filter on it and anyone who just wants the address still has it.
    """
    out = frame.copy()
    name = cfg.POSTAL_CODE_COLUMN
    if name not in out.columns:
        return out, 0

    compact = (out[name].str.upper()
                        .str.replace(r"[^A-Z0-9]", "", regex=True)
                        .replace("", pd.NA))
    six = compact.str.len() == 6
    out[name] = compact.where(~six, compact.str.slice(0, 3) + " " + compact.str.slice(3, 6))

    valid = out[name].str.match(cfg.POSTAL_CODE_PATTERN).fillna(False)
    invalid_count = int((out[name].notna() & ~valid).sum())
    out["postal_code_valid"] = valid.where(out[name].notna(), pd.NA).astype("boolean")

    if invalid_count:
        log.warning("%d postal code(s) do not match the Canadian pattern, kept and flagged",
                    invalid_count)
    return out, invalid_count
