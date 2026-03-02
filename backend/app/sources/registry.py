from __future__ import annotations

from importlib import import_module
from pkgutil import iter_modules

import app.sources
from app.sources.base import SourceDefinition

IGNORED_MODULES = {
    "__init__",
    "base",
    "factory",
    "registry",
    "rss_common",
}


def load_sources() -> list[SourceDefinition]:
    discovered: list[SourceDefinition] = []
    package_prefix = f"{app.sources.__name__}."

    for module_info in iter_modules(app.sources.__path__):  # type: ignore[arg-type]
        if module_info.name in IGNORED_MODULES:
            continue
        module = import_module(package_prefix + module_info.name)
        source = getattr(module, "SOURCE", None)
        if isinstance(source, SourceDefinition):
            discovered.append(source)
            continue

        source_list = getattr(module, "SOURCES", None)
        if isinstance(source_list, list):
            discovered.extend(
                entry for entry in source_list if isinstance(entry, SourceDefinition)
            )

    return sorted(discovered, key=lambda source: source.name)


SOURCES: list[SourceDefinition] = load_sources()


def source_map() -> dict[str, SourceDefinition]:
    return {source.name: source for source in SOURCES}
