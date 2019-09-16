#!/usr/local/bin/python
import logging
import sys
import time

from exec_wrapper import ExecWrapper


def fun(ew, data):
    print(f"data = {data}")
    print(f"sleeping ...")
    ew.update_status(success_count=0)
    time.sleep(10)
    print("done sleeping")
    ew.update_status(success_count=1)
    time.sleep(10)
    ew.update_status(success_count=2)

    if len(sys.argv) > 1:
        raise RuntimeError(sys.argv[1])


logging.basicConfig(level=logging.DEBUG,
                    format=f"%(asctime)s %(levelname)s: %(message)s")

args = ExecWrapper.make_default_args()
args.process_type_id = 45
args.status_update_interval = 5
args.process_name = 'managed_call_test'
exec_wrapper = ExecWrapper(args=args)

exec_wrapper.managed_call(fun, {'a': 1, 'b': 2})

print('done')
