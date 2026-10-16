# -*- coding: utf-8 -*-
"""Shared exception types for the cfquant RPC clients.

Kept in a dependency-free module so client.py and pipe_client.py can both
import them at module level without pulling in each other's transport stack.
"""


class CfquantError(RuntimeError):
    pass


class CfquantTimeout(TimeoutError):
    pass
