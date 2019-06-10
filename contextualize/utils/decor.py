from contextualize.utils.tools import get_base_name


def factory_direct(decorator, *args):
    """
    Factory direct

    Allow a decorator factory sans ()'s to directly decorate a callable.

    The factory must accept only optional keyword-only arguments. Hence,
    a single positional argument should only occur when the factory is
    directly decorating a callable, whether a function or class.

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
        decorated = args[0]
        if len(args) > 1:
            factory_name = get_base_name(decorator)
            raise TypeError(f'{factory_name!r} only accepts keyword arguments as a factory')
        if not callable(decorated):
            factory_name = get_base_name(decorator)
            type_name = type(decorated).__qualname__
            raise TypeError(f'{factory_name!r} must decorate a callable, not a {type_name}')
        return decorator(decorated)
    return decorator
