"""Supported export format constants and format resolution helpers."""

from __future__ import annotations

from enum import Enum
from typing import Callable, List

from pipewatch.reporter import PipelineReport


class ExportFormat(str, Enum):
    """Enumeration of supported export formats."""

    JSON = "json"
    CSV = "csv"
    TEXT = "text"

    @classmethod
    def choices(cls) -> List[str]:
        """Return a sorted list of valid format name strings."""
        return sorted(member.value for member in cls)


def get_exporter(fmt: ExportFormat) -> Callable[[List[PipelineReport]], str]:
    """Return the exporter function corresponding to *fmt*.

    Raises
    ------
    ValueError
        If *fmt* is not a recognised ExportFormat.
    """
    # Import here to avoid circular imports at module load time.
    from pipewatch.exporter import export_csv, export_json, export_text

    _registry: dict[ExportFormat, Callable[[List[PipelineReport]], str]] = {
        ExportFormat.JSON: export_json,
        ExportFormat.CSV: export_csv,
        ExportFormat.TEXT: export_text,
    }

    try:
        return _registry[ExportFormat(fmt)]
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"Unknown export format {fmt!r}. "
            f"Valid choices: {ExportFormat.choices()}"
        ) from exc


def resolve_format(name: str) -> ExportFormat:
    """Case-insensitively resolve a format name string to an ExportFormat.

    Raises
    ------
    ValueError
        If *name* does not match any known format.
    """
    try:
        return ExportFormat(name.lower())
    except ValueError:
        raise ValueError(
            f"Unrecognised format {name!r}. "
            f"Valid choices: {ExportFormat.choices()}"
        )
