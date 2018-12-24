from utils.tools import get_base_name


def factory_direct(decorator, *args):
    """
    Factory direct

    Allow a factory decorator sans ()'s to directly decorate a function.

    The factory must accept only optional keyword-only arguments. Hence,
    a single positional argument should only occur when the factory is
    decorating a function directly.

    Usage:

    def decorator_factory(*args, kwarg1=None, kwarg2=None):
        def decorator(func):
            # do some setup
            @wrapt.decorator
            def wrapper(func, instance, args, kwargs):
                # do something pre-call
                value = func(*args, **kwargs)
                # do something post-call
                return value
            return wrapper(func)
        return factory_direct(decorator, *args)
    """
    if args:
        func = args[0]
        if len(args) > 1:
            factory_name = get_base_name(decorator)
            raise TypeError(f'{factory_name!r} only accepts keyword arguments as a factory')
        if not callable(func) and not isinstance(func, (classmethod, staticmethod)):
            factory_name = get_base_name(decorator)
            type_name = type(func).__qualname__
            raise TypeError(f'{factory_name!r} must decorate a function, not a {type_name}')
        return decorator(func)
    return decorator
