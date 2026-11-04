"""Hard limits for the standalone experimental reader."""

MAX_BYTES = 16 * 1024 * 1024
READ_BYTES = 4096
MAX_COMPOUNDS = MAX_BYTES // READ_BYTES + 64
MAX_WORKER_ADDRESS_SPACE = 256 * 1024 * 1024
