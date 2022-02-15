from io import StringIO
import sys


def evaluate_code(codesting):
    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()

    eval(codesting)

    sys.stdout = old_stdout
    message = mystdout.getvalue()

    return message

