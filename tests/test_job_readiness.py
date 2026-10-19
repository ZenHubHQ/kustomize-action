import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]


def state(kind, assigned=True, extractor=False, job=None, exit_code=None):
    value = {'kind': kind, 'assigned': assigned, 'extractor': extractor, 'job': job}
    if exit_code is not None:
        value['exit_code'] = exit_code
    return value


KUBECTL = r'''#!/usr/bin/env python3
import json,os,pathlib,sys
root=pathlib.Path(os.environ['SCENARIO_DIR']);args=sys.argv[1:]
config=json.loads((root/'scenario.json').read_text())
counter=json.loads((root/'counter.json').read_text()) if (root/'counter.json').exists() else {'index':0,'active':0}
index=min(counter['active'],len(config['states'])-1);s=config['states'][index]
def persist(): (root/'counter.json').write_text(json.dumps(counter))
def event():
 with (root/'history.jsonl').open('a') as f:f.write(json.dumps({'args':args,'state':s})+'\n')
if 'get' in args and ('pod' in args or 'pods' in args):
 index=min(counter['index'],len(config['states'])-1);s=config['states'][index]
 counter['active']=index
 if not s.get('needs_extractor_cleanup'):counter['index']+=1
 persist()
event()
conditions=[]
if s.get('job'):conditions=[{'type':s['job'].capitalize(),'status':'True'}]
job={'spec':{'backoffLimit':config.get('backoffLimit',0),'template':{'spec':{'containers':[{'name':'test-job'}]+([{'name':'extractor'}] if config.get('extractor') else [])}}},'status':{'conditions':conditions}}
if 'wait' in args:
 name='Complete' if '--for=condition=complete' in args else 'Failed'
 sys.exit(0 if any(c['type']==name for c in conditions) else 1)
if 'get' in args and 'job' in args:
 if any('jsonpath' in a for a in args):print('test-job'+(' extractor' if config.get('extractor') else ''))
 else:print(json.dumps(job))
 sys.exit(0)
if 'get' in args and ('pod' in args or 'pods' in args):
 containers=None
 if s['kind'] not in ['missing','null']:
  status={}
  if s['kind']=='running':status={'running':{'startedAt':'2026-10-19T00:00:00Z'}}
  elif s['kind'] in ['success','failure']:status={'terminated':{'reason':'Completed' if s['kind']=='success' else 'Error','exitCode':s.get('exit_code',0 if s['kind']=='success' else 1)}}
  else:status={'waiting':{'reason':'ContainerCreating'}}
  containers=[{'name':'test-job','state':status,'restartCount':s.get('restart_count',0)}]
  if config.get('extractor'):containers.append({'name':'extractor','state':{'waiting':{}} if s.get('extractor_waiting') else {'running':{}}})
 pod={'metadata':{'name':s.get('pod_name','test-job-pod')},'spec':{'nodeName':'node' if s.get('assigned') else ''},'status':{'containerStatuses':containers}}
 if any('containerStatuses' in a for a in args):
  if containers is not None:print(json.dumps(containers))
 elif any('metadata.name' in a for a in args):print(s.get('pod_name','test-job-pod'))
 else:print(json.dumps({'items':[] if s['kind']=='missing' else [pod]}))
 sys.exit(0)
if 'logs' in args:
 # Reproduce the production bug: an empty log probe succeeds even while Pending.
 print('fixture logs' if '-f' in args else '')
 sys.exit(0)
if 'exec' in args:
 if not s.get('assigned'):sys.exit(1)
 if s.get('transient_cleanup_failure') and not counter.get('cleanup_attempts'):
  counter['cleanup_attempts']=1;persist();sys.exit(1)
 if s.get('needs_extractor_cleanup'):
  counter['index']+=1;persist()
 if s.get('cleanup_race'):sys.exit(1)
sys.exit(0)
'''
DATE = r'''#!/usr/bin/env python3
import os,pathlib,sys
p=pathlib.Path(os.environ['SCENARIO_DIR'])/'clock';n=int(p.read_text()) if p.exists() else 0
if '+%s' in sys.argv:print(n)
elif '-d' in sys.argv:print('01/01 00:01')
else:print('01/01 %02d:%02d'%(n//3600,(n//60)%60))
'''
SLEEP = r'''#!/usr/bin/env python3
import os,pathlib,sys
p=pathlib.Path(os.environ['SCENARIO_DIR'])/'clock';n=int(p.read_text()) if p.exists() else 0
p.write_text(str(n+int(sys.argv[1])))
'''


class JobReadiness(unittest.TestCase):
    def run_action(self, states, extractor=False, backoff=0):
        # Execute the actual composite-action shell, not a test implementation.
        source=(ROOT/'action.yaml').read_text()
        source=source.split('    - name: "Handle Job"\n',1)[1].split('    - name: "Clean Namespace"',1)[0]
        script=textwrap.dedent(source.split('      run: |\n',1)[1])
        for key,value in {'waitForJob':'test-job','namespace':'ci-test','timeoutMinuteStartContainer':'1','deleteNamespace':'false'}.items():
            script=script.replace('${{ inputs.'+key+' }}',value)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'scenario.json').write_text(json.dumps({'states':states,'extractor':extractor,'backoffLimit':backoff}))
            for name,content in [('kubectl',KUBECTL),('date',DATE),('sleep',SLEEP)]:
                p=root/name;p.write_text(content);p.chmod(0o755)
            (root/'run.sh').write_text(script)
            result=subprocess.run(['bash','-e','-o','pipefail',str(root/'run.sh')],env={**os.environ,'PATH':str(root)+':'+os.environ['PATH'],'SCENARIO_DIR':str(root)},capture_output=True,text=True,timeout=60)
            history=[json.loads(x) for x in (root/'history.jsonl').read_text().splitlines()]
            return result,history

    def test_pending_empty_status_and_extractor_do_not_count_as_main_running(self):
        result,history=self.run_action([state('missing',False),state('null',False),state('waiting'),state('running'),state('success')],extractor=True)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        streamed=[h for h in history if 'logs' in h['args'] and '-f' in h['args']]
        self.assertTrue(streamed)
        self.assertTrue(all(h['state']['assigned'] and h['state']['kind'] in ['running','success'] for h in streamed))
        self.assertTrue(any('cp' in h['args'] for h in history))
        self.assertTrue(any('exec' in h['args'] for h in history))

    def test_pending_times_out_without_starting_logs_or_extractor(self):
        result,history=self.run_action([state('null',False)],extractor=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('Timeout',result.stdout)
        self.assertFalse(any('-f' in h['args'] or 'exec' in h['args'] for h in history))

    def test_fast_success_before_first_poll_is_still_success(self):
        result,history=self.run_action([state('success',job='complete')])
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertTrue(any('logs' in h['args'] for h in history))

    def test_failed_job_remains_failure_without_artifact_copy(self):
        result,history=self.run_action([state('failure',job='failed')],extractor=True)
        self.assertNotEqual(result.returncode,0)
        self.assertFalse(any('cp' in h['args'] for h in history))

    def test_retry_pod_can_start_after_first_attempt_fails(self):
        result,_=self.run_action([state('failure'),state('null',False),state('running'),state('success',job='complete')],backoff=1)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_empty_status_after_log_stream_returns_is_not_failure(self):
        result,_=self.run_action([state('running'),state('null'),state('success',job='complete')])
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)


    def test_same_pod_container_restart_streams_new_attempt(self):
        restarted=state('running');restarted['restart_count']=1
        result,history=self.run_action([state('running'),state('failure'),restarted,state('success',job='complete')],backoff=1)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual(sum('logs' in h['args'] and '-f' in h['args'] for h in history),2)

    def test_failed_unscheduled_job_never_executes_extractor(self):
        result,history=self.run_action([state('null',False,job='failed')],extractor=True)
        self.assertNotEqual(result.returncode,0)
        self.assertFalse(any('exec' in h['args'] for h in history))


    def test_each_failed_attempt_gets_a_new_bounded_retry_startup_window(self):
        first=state('failure');first['pod_name']='first-attempt'
        second=state('failure');second['pod_name']='second-attempt'
        result,_=self.run_action([first]+[state('null',False)]*9+[second]+[state('null',False)]*9+[state('running'),state('success',job='complete')],backoff=2)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)


    def test_failed_main_must_stop_extractor_before_controller_can_retry(self):
        failed=state('failure');failed['needs_extractor_cleanup']=True;failed['cleanup_race']=True
        result,history=self.run_action([failed,state('null',False),state('running'),state('success')],extractor=True,backoff=1)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual(sum('exec' in h['args'] and h['state']['kind']=='failure' for h in history),1)
        self.assertTrue(any('cp' in h['args'] for h in history))

    def test_transient_extractor_exec_failure_is_retried_while_helper_runs(self):
        failed=state('failure');failed['needs_extractor_cleanup']=True;failed['transient_cleanup_failure']=True
        result,history=self.run_action([failed,state('null',False),state('running'),state('success')],extractor=True,backoff=1)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual(sum('exec' in h['args'] and h['state']['kind']=='failure' for h in history),2)
        self.assertTrue(any('cp' in h['args'] for h in history))

    def test_extractor_starting_after_main_failure_is_still_stopped(self):
        first=state('failure');first['extractor_waiting']=True
        waiting_for_cleanup=state('failure');waiting_for_cleanup['needs_extractor_cleanup']=True
        result,history=self.run_action([first,waiting_for_cleanup,state('failure',job='failed')],extractor=True)
        self.assertNotEqual(result.returncode,0)
        self.assertEqual(sum('exec' in h['args'] for h in history),1)
        self.assertNotIn('Timeout',result.stdout)

    def test_backoff_exhaustion_after_extractor_stop_remains_failure(self):
        failed=state('failure');failed['needs_extractor_cleanup']=True
        result,history=self.run_action([failed,state('failure',job='failed')],extractor=True)
        self.assertNotEqual(result.returncode,0)
        self.assertEqual(sum('exec' in h['args'] for h in history),1)
        self.assertFalse(any('cp' in h['args'] for h in history))


if __name__=='__main__':unittest.main()
