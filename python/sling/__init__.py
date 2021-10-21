
import os, sys, tempfile, time, json, platform
from subprocess import PIPE, Popen, STDOUT
from typing import Iterable, List, Union
from json import JSONEncoder

# set binary
BIN_FOLDER = os.path.join(os.path.dirname(__file__), 'bin')
if platform.system() == 'Linux':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-linux')
elif platform.system() == 'Windows':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-win.exe')
elif platform.system() == 'Darwin':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-mac')

class JsonEncoder(JSONEncoder):
  def default(self, o):
    return o.__dict__  

class SourceOptions:
  trim_space: bool
  empty_as_null: bool
  header: bool
  compression: str
  null_if: str
  datetime_format: str
  skip_blank_lines: bool
  delimiter: str
  max_decimals: int

  def __init__(self, **kwargs) -> None:
    self.trim_space = kwargs.get('trim_space')
    self.empty_as_null = kwargs.get('empty_as_null')
    self.header = kwargs.get('header')
    self.compression = kwargs.get('compression')
    self.null_if = kwargs.get('null_if')
    self.datetime_format = kwargs.get('datetime_format')
    self.skip_blank_lines = kwargs.get('skip_blank_lines')
    self.delimiter = kwargs.get('delimiter')
    self.max_decimals = kwargs.get('max_decimals')

class Source:
  conn: str
  stream: str
  limit: int
  options: SourceOptions

  def __init__(self, **kwargs) -> None:
    self.conn = kwargs.get('conn')
    self.stream = kwargs.get('stream')
    self.limit = kwargs.get('limit')
    self.options = SourceOptions(options=kwargs.get('options', {}))


class TargetOptions:
  header: bool
  compression: str
  concurrency: int
  datetime_format: str
  delimiter: str
  file_max_rows: int
  max_decimals: int
  use_bulk: bool
  table_ddl: str
  table_tmp: str
  pre_sql: str
  post_sql: str

  def __init__(self, **kwargs) -> None:
    self.header = kwargs.get('header')
    self.compression = kwargs.get('compression')
    self.concurrency = kwargs.get('concurrency')
    self.datetime_format = kwargs.get('datetime_format')
    self.delimiter = kwargs.get('delimiter')
    self.file_max_rows = kwargs.get('file_max_rows')
    self.max_decimals = kwargs.get('max_decimals')
    self.use_bulk = kwargs.get('use_bulk')
    self.table_ddl = kwargs.get('table_ddl')
    self.table_tmp = kwargs.get('table_tmp')
    self.pre_sql = kwargs.get('pre_sql')
    self.post_sql = kwargs.get('post_sql')

class Target:
  conn: str
  object: str
  options: TargetOptions
  mode: str
  primary_key: List[str]
  update_key: str

  def __init__(self, **kwargs) -> None:
    self.conn = kwargs.get('conn')
    self.object = kwargs.get('object')
    self.options = TargetOptions(options=kwargs.get('options', {}))
    self.mode = kwargs.get('mode')
    self.primary_key = kwargs.get('primary_key')
    self.update_key = kwargs.get('update_key')

class Options:
  stdout: bool

  def __init__(self, **kwargs) -> None:
    self.stdout = kwargs.get('stdout')


class Sling:
  """
  Sling represents the main object to define a
  sling task. Call the `run` method to execute the task.

  `source` represent the source object using the `Source` class.
  `target` represent the target object using the `Target` class.
  `options` represent the optinos object using the `Options` class.
  """
  source: Source
  target: Target
  options: Options

  temp_file: str

  def __init__(self, source: Union[Source, dict]={}, target: Union[Target, dict]={}, options: Union[Options, dict]={}) -> None:
    if isinstance(source, dict):
      source = Source(**source)
    self.source = source

    if isinstance(target, dict):
      target = Target(**target)
    self.target = target

    if isinstance(options, dict):
      options = Options(**options)
    self.options = options
  
  def _prep_cmd(self):

    # generate temp file
    ts = time.time_ns()
    temp_dir = tempfile.gettempdir()
    self.temp_file = os.path.join(temp_dir, f'sling-cfg-{ts}.json')

    # dump config
    with open(self.temp_file, 'w') as file:
      config = dict(
        source=self.source, target=self.target, options=self.options,
      )
      json.dump(config, file, cls=JsonEncoder)
    
    cmd = f'{SLING_BIN} run -c "{self.temp_file}"'

    return cmd
  
  def _cleanup(self):
      os.remove(self.temp_file)

  def run(self, return_output=False, env:dict=None):
    """
    Runs the task. Use `return_output` as `True` to return the stdout+stderr output at end. `env` accepts a dictionary which defines the environment.
    """
    cmd = self._prep_cmd()
    lines = []
    try:
      for k,v in os.environ.items():
        env[k] = env.get(k, v)

      for line in _exec_cmd(cmd, env=env):
        if return_output:
          lines.append(line)
        else:
          print(line, flush=True)

    except Exception as E:
      if return_output:
        lines.append(str(E))
        raise Exception('\n'.join(lines))
      raise E

    finally:
      self._cleanup()
    
    return '\n'.join(lines)
  
  def stream(self, env:dict=None, stdin=None) -> Iterable[list]:
    """
    Runs the task and streams the stdout output as iterable. `env` accepts a dictionary which defines the environment. `stdin` can be any stream-like object, which will be used as input stream.
    """
    cmd = self._prep_cmd()

    lines = []
    try:
      for k,v in os.environ.items():
        env[k] = env.get(k, v)
        
      for stdout_line in _exec_cmd(cmd, env=env, stdin=stdin, stderr=PIPE):
        lines.append(stdout_line)
        if len(lines) > 20:
          lines.pop(0) # max size of 100 lines
        yield stdout_line
    
    except Exception as E:
      lines.append(str(E))
      raise Exception('\n'.join(lines))

    finally:
      self._cleanup()

def cli(*args, return_output=False):
  "calls the sling binary with the provided args"
  args = args or sys.argv[1:]
  escape = lambda a: a.replace('"', '\\"')
  cmd = f'''{SLING_BIN} {" ".join([f'"{escape(a)}"' for a in args])}'''
  lines = []
  try:
    stdout = PIPE if return_output else sys.stdout
    stderr = STDOUT if return_output else sys.stderr
    for line in _exec_cmd(cmd, stdin=sys.stdin, stdout=stdout, stderr=stderr):
      if return_output:
        lines.append(line)
      else:
        print(line, flush=True)
  except Exception as E:
    if return_output:
      raise E
  return '\n'.join(lines)

  

def _exec_cmd(cmd, stdin=None, stdout=PIPE, stderr=STDOUT, env:dict=None):
  with Popen(cmd, shell=True, env=env, stdin=stdin, stdout=stdout, stderr=stderr) as proc:
    for line in proc.stdout:
      line = str(line.strip(), 'utf-8')
      yield line

    proc.wait()

    lines = line
    if stderr and stderr != STDOUT:
      lines = '\n'.join(list(proc.stderr))

    if proc.returncode != 0:
      raise Exception(f'Sling command failed:\n{lines}')