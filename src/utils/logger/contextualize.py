import asyncio

from loguru import logger


def contextualize(*log_args):
    def _extract_context_params(args, kwargs):
        self = args[0] if args else None
        context_params = {}

        for arg in log_args:
            if arg.startswith("self."):
                attrs = arg.split(".")[1:]
                value = self
                for attr in attrs:
                    value = getattr(value, attr, None)
                    if value is None:
                        break
                if value is not None:
                    context_params[attrs[-1]] = value
            elif arg in kwargs:
                context_params[arg] = kwargs[arg]
        return context_params

    def decorator(fn):
        if asyncio.iscoroutinefunction(fn):

            async def wrapper(*args, **kwargs):
                context_params = _extract_context_params(args, kwargs)
                with logger.contextualize(**context_params):
                    return await fn(*args, **kwargs)

        else:

            def wrapper(*args, **kwargs):
                context_params = _extract_context_params(args, kwargs)
                with logger.contextualize(**context_params):
                    return fn(*args, **kwargs)

        return wrapper

    return decorator
