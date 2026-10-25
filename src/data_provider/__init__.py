"""Shared market-data packs and collection, independent of research and UI."""

if __name__ != "data_provider":
    raise ImportError("Import data_provider with src on sys.path; src.data_provider is unsupported.")

# Explicit reader dependency closure; collection is attributed by acquisition provenance.
READER_MODULES = (
    "data_provider",
    "data_provider.errors",
    "data_provider.data",
    "data_provider.manifest",
    "data_provider.pack_lock",
    "data_provider.update_transaction",
    "data_provider.exchange_data",
)
