from functools import partial, update_wrapper
import inspect


class WithArgDecorator:
    """
    A impenetrable piece of metaprogramming to easily define a decorator that does
    some setup/teardown around a test case, and optionally passes a named argument
    into the test case.
    """

    def __init__(self, arg_name, setup):
        self.arg_name = arg_name
        self.setup = setup

    def __call__(self, f=None, **target_kwargs):
        if f is None:
            # Handle arguments to the decorator: when called with only kwargs, return a function
            # that when called wth single function argument, invokes this function
            # including the function *and* target_kwargs
            return partial(self, **target_kwargs)

        sig = inspect.signature(f)
        need_arg = self.arg_name in sig.parameters

        def wrapper(*args, **kwargs):
            with self.setup(**target_kwargs) as arg_object:
                if need_arg:
                    kwargs[self.arg_name] = arg_object

                return f(*args, **kwargs)

        update_wrapper(wrapper, f)

        if need_arg:
            # We need the computed signature of the final function to not include the
            # extra argument, since pytest will think it's a fixture.
            # We remove the extra from the function we return using functools.partial.
            #
            # functools.update_wrapper does things we need, like updating __dict__ with
            # the pytest marks from the original function. But it also sets result.__wrapped__
            # to point back to the original function, and this results in inspect.signature
            # using the original function for the signature, bringing back the extra
            # argument.

            result = partial(wrapper, **{self.arg_name: None})
            update_wrapper(result, wrapper)
            del result.__dict__["__wrapped__"]

            return result
        else:
            return wrapper
