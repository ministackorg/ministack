import sys, os, json

sys.path.insert(0, os.environ["_LAMBDA_CODE_DIR"])

_REAL_STDOUT = sys.__stdout__
# Match AWS Lambda semantics: logs go to CloudWatch (stderr here),
# while the Invoke response payload must be clean JSON on stdout.
sys.stdout = sys.stderr

for _ld in filter(None, os.environ.get("_LAMBDA_LAYERS_DIRS", "").split(os.pathsep)):
    _py = os.path.join(_ld, "python")
    if os.path.isdir(_py):
        sys.path.insert(0, _py)
    sys.path.insert(0, _ld)

_mod_path = os.environ["_LAMBDA_HANDLER_MODULE"]
_fn_name  = os.environ["_LAMBDA_HANDLER_FUNC"]

event = json.loads(sys.stdin.read())

class LambdaContext:
    function_name        = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
    memory_limit_in_mb   = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128"))
    invoked_function_arn = os.environ.get("_LAMBDA_FUNCTION_ARN", "")
    aws_request_id       = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", "")
    log_group_name       = "/aws/lambda/" + function_name
    log_stream_name      = aws_request_id

    @staticmethod
    def get_remaining_time_in_millis():
        return int(float(os.environ.get("_LAMBDA_TIMEOUT", "3")) * 1000)

_mod = __import__(_mod_path)
for _part in _mod_path.split(".")[1:]:
    _mod = getattr(_mod, _part)
_result = getattr(_mod, _fn_name)(event, LambdaContext())
if _result is not None:
    _REAL_STDOUT.write(json.dumps(_result))
    _REAL_STDOUT.flush()