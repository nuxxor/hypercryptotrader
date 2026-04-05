import asyncio

import tree_runtime as _tree_runtime
from tree_runtime import *  # noqa: F401,F403

__all__ = [*getattr(_tree_runtime, "__all__", []), "asyncio"]


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
