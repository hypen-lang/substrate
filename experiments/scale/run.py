#!/usr/bin/env python3
"""Serial, resource-guarded scale matrix. Only creates disposable benchmark data.

macOS process metrics use Apple's proc_pid_rusage v2. CPU/I/O samples for short-
lived PG workers can miss their final activity; summed RSS counts shared pages
more than once. Rust also reports exact process CPU via getrusage. Disk I/O means
OS-attributed bytes, not device traffic or transport bandwidth.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
FIELDS = ['user_time','system_time','pkg_idle_wkups','interrupt_wkups','pageins',
          'wired_size','resident_size','phys_footprint','proc_start_abstime','proc_exit_abstime',
          'child_user_time','child_system_time','child_pkg_idle_wkups','child_interrupt_wkups',
          'child_pageins','child_elapsed_abstime','diskio_bytesread','diskio_byteswritten']
class Usage(ctypes.Structure):
    _fields_ = [('uuid', ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in FIELDS]

class Monitor:
    def __init__(self):
        if platform.system() != 'Darwin':
            raise RuntimeError('This resource sampler currently supports macOS only')
        self.lib = ctypes.CDLL('/usr/lib/libproc.dylib')
        self.lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(Usage)]
        self.lib.proc_pid_rusage.restype = ctypes.c_int
        class Timebase(ctypes.Structure):
            _fields_=[('numer',ctypes.c_uint32),('denom',ctypes.c_uint32)]
        tb=Timebase()
        system=ctypes.CDLL('/usr/lib/libSystem.B.dylib')
        if system.mach_timebase_info(ctypes.byref(tb)) != 0: raise RuntimeError('timebase unavailable')
        self.seconds_per_tick=tb.numer/tb.denom/1e9
        self.history = {}
    def sample(self, roots):
        p = subprocess.run(['ps','-axo','pid=,ppid='], capture_output=True, text=True)
        if p.returncode:
            raise RuntimeError('process metrics unavailable: ' + p.stderr.strip())
        pairs = [tuple(map(int,line.split())) for line in p.stdout.splitlines()]
        selected = set(roots)
        while True:
            added = {pid for pid,parent in pairs if parent in selected} - selected
            if not added: break
            selected.update(added)
        live = {}
        for pid in selected:
            u = Usage()
            if self.lib.proc_pid_rusage(pid, 2, ctypes.byref(u)) == 0:
                values = {name: int(getattr(u,name)) for name in FIELDS}
                live[pid] = values
                self.history[(pid,values['proc_start_abstime'])] = values
        return {'sampled_processes':len(live),
                'rss_sum_bytes':sum(x['resident_size'] for x in live.values()),
                'physical_footprint_sum_bytes':sum(x['phys_footprint'] for x in live.values()),
                'sampled_cpu_seconds':sum(x['user_time']+x['system_time'] for x in self.history.values())*self.seconds_per_tick,
                'sampled_disk_read_bytes':sum(x['diskio_bytesread'] for x in self.history.values()),
                'sampled_disk_write_bytes':sum(x['diskio_byteswritten'] for x in self.history.values())}

def write(f, row):
    f.write(json.dumps(row,separators=(',',':'))+'\n');f.flush()

def measured(command, output, label, args, expected=0):
    start=time.monotonic(); state={'phase':label, 'server':None, 'cluster':None}; failure=None
    monitor=Monitor()
    with open(str(output)+'.jsonl','x',buffering=1) as result, open(str(output)+'.resources.jsonl','x',buffering=1) as samples, open(str(output)+'.stderr','x') as err:
        process=subprocess.Popen(command,cwd=ROOT,stdout=subprocess.PIPE,stderr=err,text=True,start_new_session=True)
        def read():
            for line in process.stdout:
                result.write(line);result.flush()
                try: item=json.loads(line)
                except ValueError: continue
                event=item.get('event',item.get('phase'))
                if event=='phase' or str(event).endswith('_start'):
                    state['phase']={k:v for k,v in item.items() if k in ['event','phase','query','name','writers','readers','mode','clients']}
                if item.get('postmaster_pid'): state['server']=item['postmaster_pid']
                if item.get('cluster'): state['cluster']=item['cluster']
        reader=threading.Thread(target=read);reader.start()
        peak=0; peak_rss=0
        try:
            while process.poll() is None:
                roots=[process.pid]+([state['server']] if state['server'] else [])
                metrics=monitor.sample(roots)
                peak=max(peak,metrics['physical_footprint_sum_bytes']);peak_rss=max(peak_rss,metrics['rss_sum_bytes'])
                free=shutil.disk_usage(ROOT).free
                write(samples,{'elapsed_seconds':time.monotonic()-start,'phase':state['phase'],'disk_free_bytes':free,**metrics})
                if peak>args.max_memory_gib*1024**3: failure='physical footprint resource limit'
                if free<args.min_free_gib*1024**3: failure='free disk resource limit'
                if time.monotonic()-start>args.max_seconds: failure='phase wall-time resource limit'
                if failure:
                    process.terminate()
                    try:process.wait(timeout=15)
                    except subprocess.TimeoutExpired: os.killpg(process.pid,signal.SIGKILL)
                    break
                time.sleep(.5)
        except BaseException:
            process.terminate()
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL)
            raise
        finally:
            process.wait(); reader.join()
            if state['cluster'] and Path(state['cluster']).is_dir():
                # Exact disposable cluster reported by our child, never an existing server.
                subprocess.run(['pg_ctl','-D',state['cluster'],'-m','immediate','-w','stop'],capture_output=True)
        summary={'label':label,'command':command,'returncode':process.returncode,'expected_returncode':expected,'elapsed_seconds':time.monotonic()-start,'resource_limit':failure,'peak_sampled_footprint_bytes':peak,'peak_sampled_rss_sum_bytes':peak_rss,'success':process.returncode==expected and failure is None}
        write(result,{'event':'runner_result',**summary})
    print(json.dumps(summary),flush=True)
    return summary

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rows',nargs='+',type=int,default=[100,1000,10000,100000,1000000,10000000])
    p.add_argument('--backends',nargs='+',choices=['substrate','sqlite','postgres'],default=['substrate','sqlite','postgres'])
    p.add_argument('--clients',nargs='+',type=int,default=[1,2,4,8,16,32,64,128])
    p.add_argument('--seconds',type=float,default=2)
    p.add_argument('--modes',nargs='+',choices=['read','write','mixed'],default=['read','write','mixed'])
    p.add_argument('--checkpoint-commits',type=int)
    p.add_argument('--checkpoint-bytes',type=int)
    p.add_argument('--cache-bytes',type=int)
    p.add_argument('--group-size',type=int)
    p.add_argument('--collection-us',type=int)
    p.add_argument('--point-api',choices=['snapshot','session'],default='snapshot')
    p.add_argument('--postgres-isolation',choices=['read committed','serializable'],default='read committed')
    p.add_argument('--held-read',action='store_true',help='hold SQLite/PostgreSQL read transactions for SQL comparisons')
    p.add_argument('--max-memory-gib',type=float,default=5)
    p.add_argument('--min-free-gib',type=float,default=3)
    p.add_argument('--max-seconds',type=float,default=900)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--binary',type=Path,default=ROOT/'target/release/examples/scale_benchmark')
    args=p.parse_args();args.binary=args.binary.resolve();args.output.mkdir(parents=True,exist_ok=False)
    for option in ['checkpoint_commits','checkpoint_bytes','group_size','collection_us','cache_bytes','point_api']:
        value=getattr(args,option)
        if value is not None:os.environ['SCALE_'+option.upper()]=str(value)
        else:os.environ.pop('SCALE_'+option.upper(),None)
    metadata={'time_unix':time.time(),'platform':platform.platform(),'cpu_count':os.cpu_count(),'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},'durability':'redb Immediate; SQLite WAL FULL fullfsync=ON; PG synchronous_commit=on fsync_writethrough','monitor_interval_seconds':.5}
    (args.output/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    summaries=[]
    for rows in args.rows:
        spec=args.output/f'queries-{rows}.json'
        spec.write_bytes(subprocess.check_output([str(args.binary),'spec','substrate','unused',str(rows)]))
        for backend in args.backends:
            print(f'Start {backend} {rows} rows',flush=True)
            if shutil.disk_usage(ROOT).free<args.min_free_gib*1024**3:
                summaries.append({'backend':backend,'rows':rows,'skipped':'disk safety limit before start'});continue
            with tempfile.TemporaryDirectory(prefix='substrate-scale-') as directory:
                path=Path(directory)/'database'
                if backend=='postgres':
                    command=['python3','experiments/scale/postgres.py','--rows',str(rows),'--seconds',str(max(1,int(args.seconds))),'--clients',*map(str,args.clients),'--queries',str(spec),'--min-free-gib',str(args.min_free_gib),'--modes',*args.modes,'--isolation',args.postgres_isolation]
                    if args.held_read:command.append('--held-read')
                    summaries.append(measured(command,args.output/f'{backend}-{rows}',f'{backend}-{rows}',args))
                else:
                    for phase in ['load','run','crash','recover']:
                        command=[str(args.binary),'run-held' if phase=='run' and args.held_read else phase,backend,str(path),str(rows),str(args.seconds),','.join(map(str,args.clients)),','.join(args.modes)]
                        r=measured(command,args.output/f'{backend}-{rows}-{phase}',f'{backend}-{rows}-{phase}',args,73 if phase=='crash' else 0)
                        summaries.append(r)
                        if not r['success']:break
            (args.output/'summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
    # Fail on output mismatches; an unavailable run is explicit in summary instead.
    coverage={}
    for rows in args.rows:
        hashes={}
        compared={}
        for file in args.output.glob(f'*-{rows}*.jsonl'):
            if '.resources.' in file.name:continue
            for line in file.read_text().splitlines():
                item=json.loads(line)
                if item.get('rows')==rows and 'sha256' in item and 'result_rows' in item:
                    name=item.get('query',item.get('name'));digest=item['sha256']
                    if name in hashes and hashes[name]!=digest:raise AssertionError(f'result mismatch: {rows} {name} {file}')
                    hashes[name]=digest
                    compared.setdefault(name,[]).append(item['backend'])
        coverage[rows]=compared
    (args.output/'query-parity.json').write_text(json.dumps({'completed_query_digests_match':True,'coverage':coverage},indent=2)+'\n')

if __name__=='__main__':main()
