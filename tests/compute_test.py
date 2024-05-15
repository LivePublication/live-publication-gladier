from globus_compute_sdk import Executor

# from globus_compute_sdk.sdk.login_manager.whoami import print_whoami_info

# print_whoami_info()

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode

# First, define the function ...
def add_func(a, b):
    return a + b

gcc = Client(code_serialization_strategy=CombinedCode())
with Executor('e8c8b86f-a6fb-4c5b-9bc0-45423336f0c5', client=gcc) as gcx:
    # do something with gcx
    # ... then submit for execution, ...
    future = gcx.submit(add_func, 5, 10)

    # ... and finally, wait for the result
    print(future.result())


