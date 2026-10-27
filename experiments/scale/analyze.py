#!/usr/bin/env python3
"""Normalize retained raw runs. No benchmark executes here; all units explicit."""
import argparse,csv,json
from pathlib import Path


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def analyze(root):
    normalized=[];loads=[];queries=[];recovery=[];coverage=[]
    for file in sorted(root.glob('*.jsonl')):
        if '.resources.' in file.name:continue
        items=records(file)
        samples_file=file.with_suffix('.resources.jsonl')
        samples=records(samples_file) if samples_file.exists() else []
        for item in items:
            event=item.get('event',item.get('phase'));backend=item.get('backend');rows=item.get('rows')
            if event=='runner_result':
                coverage.append(item);continue
            if event=='load':
                loads.append({'backend':backend,'rows':rows,'ingest_seconds':item.get('ingest_seconds',item.get('elapsed_seconds')),'rows_per_second':item.get('rows_per_second',item.get('records_per_second')),'maintenance_seconds':item.get('maintenance_seconds',item.get('analyze_seconds')),'file_bytes':item.get('size',{}).get('file_bytes',item.get('database_bytes')),'allocated_bytes':item.get('size',{}).get('allocated_bytes'),'table_and_index_bytes':item.get('table_and_index_bytes'),'cluster_bytes':item.get('cluster_apparent_bytes'),'load_peak_rss_bytes':item.get('resource',{}).get('process_lifetime_peak_rss_bytes'),'logical_payload_bytes':item.get('logical_payload_bytes'),'source':file.name})
            elif event in ['sql','query']:
                queries.append({'backend':backend,'rows':rows,'query':item.get('query',item.get('name')),'p50_us':item['latency']['p50_us'],'result_rows':item['result_rows'],'sha256':item['sha256'],'source':file.name})
            elif event in ['open','clean_restart','crash_restart','recovery_verified']:
                recovery.append(item|{'source':file.name})
            elif event in ['concurrent','workload']:
                pg=backend=='postgres'
                writers=item.get('writers',item.get('writer_clients'));readers=item.get('readers',item.get('reader_clients'))
                phase_samples=[]
                for sample in samples:
                    phase=sample.get('phase',{})
                    if not isinstance(phase,dict):continue
                    if pg:
                        match=phase.get('phase')=='workload_start' and phase.get('mode')==item['mode'] and phase.get('clients')==item['clients']
                    else:match=phase.get('phase')=='concurrent' and phase.get('writers')==writers and phase.get('readers')==readers
                    if match:phase_samples.append(sample)
                resource={}
                if phase_samples:
                    resource={'sampled_peak_rss_sum_bytes':max(x['rss_sum_bytes'] for x in phase_samples),'sampled_peak_footprint_sum_bytes':max(x['physical_footprint_sum_bytes'] for x in phase_samples),'resource_samples':len(phase_samples)}
                    if len(phase_samples)>1:
                        a,b=phase_samples[0],phase_samples[-1];elapsed=b['elapsed_seconds']-a['elapsed_seconds']
                        resource.update({'sampled_cpu_core_equivalents':(b['sampled_cpu_seconds']-a['sampled_cpu_seconds'])/elapsed,'sampled_disk_read_bytes_per_second':(b['sampled_disk_read_bytes']-a['sampled_disk_read_bytes'])/elapsed,'sampled_disk_write_bytes_per_second':(b['sampled_disk_write_bytes']-a['sampled_disk_write_bytes'])/elapsed})
                if pg:
                    pools=item['pools'];w=pools.get('write',{});r=pools.get('read',{})
                    row={'write_tps':item['common_wall_write_tps'],'read_ops_per_second':item['common_wall_read_tps'],'write_successes':w.get('successes',0),'read_successes':r.get('successes',0),'failed':sum(p['failures'] for p in pools.values()),'write_p50_us':w.get('latency',{}).get('p50_us'),'write_p99_us':w.get('latency',{}).get('p99_us'),'read_p50_us':r.get('latency',{}).get('p50_us'),'read_p99_us':r.get('latency',{}).get('p99_us'),'logical_app_bytes_per_second_estimate':24*item['common_wall_write_tps']+(28+sum(max(0,rows//9+1-10**digits) for digits in range(6,len(str(rows//9+1))))/(rows//9+1))*item['common_wall_read_tps'],'wal_bytes_per_second':item['wal_bytes_per_wall_second'],'workers_without_success':None,'timing':'common wall includes connection setup; PG local IPC; mixed pools not synchronized'}
                else:
                    w=item['write_latency'];r=item['read_latency'];g=item.get('group_commit') or {}
                    row={'write_tps':item['write_tps'],'read_ops_per_second':item['read_ops_per_second'],'write_successes':w['count'],'read_successes':r['count'],'failed':item['failed_latency']['count'],'write_p50_us':w['p50_us'],'write_p99_us':w['p99_us'],'read_p50_us':r['p50_us'],'read_p99_us':r['p99_us'],'logical_app_bytes_per_second':item['logical_app_bytes_per_second'],'cpu_core_equivalents':item['resource']['cpu_core_equivalents'],'workers_without_success':sum(x['succeeded']==0 for x in item['per_worker']),'writes_per_durable_group':g.get('committed_writes',0)/g['durable_groups'] if g.get('durable_groups') else None,'timing':'barrier to final drain; setup excluded; native Substrate API / embedded SQLite SQL'}
                if not pg:
                    for name in ['writer_wait_ns','validation_apply_ns','persistence_ns','publication_ns']:
                        row[name]=g.get(name)
                    for name,value in (item.get('persistence') or {}).items():
                        row['storage_'+name]=value
                normalized.append({'backend':backend,'point_api':item.get('point_api','snapshot' if backend=='substrate' else 'sql'),'rows':rows,'writers':writers,'readers':readers,'source':file.name,**row,**resource})
    data={'loads':loads,'queries':queries,'concurrency':normalized,'recovery':recovery,'coverage':coverage}
    (root/'analysis.json').write_text(json.dumps(data,indent=2)+'\n')
    for name in ['loads','queries','concurrency']:
        rows=data[name]
        if rows:
            keys=list(dict.fromkeys(k for row in rows for k in row))
            with open(root/(name+'.csv'),'w') as f:
                writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(rows)
    return data

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path);a=p.parse_args();data=analyze(a.directory)
    for load in data['loads']:
        b,n=load['backend'],load['rows'];c=[x for x in data['concurrency'] if x['backend']==b and x['rows']==n and x['writers'] and not x['readers']]
        peak=max(c,key=lambda x:x['write_tps']) if c else None
        print(json.dumps({'backend':b,'rows':n,'load_seconds':load['ingest_seconds'],'file_MB':load['file_bytes']/1e6,'peak_write_tps':peak['write_tps'] if peak else None,'peak_clients':peak['writers'] if peak else None}))
