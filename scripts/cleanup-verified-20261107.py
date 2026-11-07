import base64, hashlib, json, os, pathlib, subprocess, tempfile
REPO='Tomodad/cc-switch'
SOURCE='119feb202ff6b55b05a9e295cb75f4d5b951c8d8'
NEW='local-v4.0.27-20261107'
KEEP='local-v4.0.25-20261104'
OLD='local-v4.0.17-20261021'
SELF='codex/cleanup-verified-v4.0.27-20261107'
ARCHIVE='archive/20261107/verified-retention-audit'
TARGETS={
 'codex/integration-v4.0.11-20261014':'89d906329de6f23b9cb2c5d5a65590f8b84ad601',
 'codex/integration-v4.0.15-20261019':'eb478e7c728dd21521625c1acb59bba93b44ec67',
 'codex/integration-v4.0.17-20261021':'5e54da9a136b8060bb793a8558eb03502ee43fe0',
 'codex/integration-v4.0.25-20261104':'e328c06ed685eb18caf9c9d7ec99fb14ad8dfbda',
 'codex/integration-v4.0.27-20261107':SOURCE,
}
# Restore any removed integration branch using this exact map: its commits remain
# reachable from main and published release tags. This script is retained by ARCHIVE.
# Example: gh api -X POST repos/Tomodad/cc-switch/git/refs -f ref=refs/heads/BRANCH -f sha=SHA
# Restore old release: gh api -X PATCH repos/Tomodad/cc-switch/releases/419307958 -F draft=false

def api(path,method='GET',data=None):
    cmd=['gh','api',path,'--method',method]
    if data is not None: cmd+=['--input','-']
    p=subprocess.run(cmd,input=json.dumps(data) if data is not None else None,text=True,capture_output=True)
    if p.returncode: raise RuntimeError(f'{method} {path}: {p.stderr}')
    return json.loads(p.stdout) if p.stdout.strip() else None

def paged(path):
    values=[]
    for page in range(1,100):
        rows=api(path+('&' if '?' in path else '?')+f'per_page=100&page={page}')
        values.extend(rows)
        if len(rows)<100: return values
    raise RuntimeError('Unexpected pagination limit')

def asset_snapshot(r):
    return sorted((a['id'],a['name'],a['size'],a.get('digest'),a['state']) for a in r['assets'])

prefix=f'repos/{REPO}'
assert api(prefix+'/git/ref/heads/main')['object']['sha']==SOURCE
run=api(prefix+'/actions/runs/42372731549')
assert run['head_sha']==SOURCE and run['status']=='completed' and run['conclusion']=='success'
new=api(prefix+'/releases/437593182')
old=api(prefix+'/releases/419307958')
assert new['tag_name']==NEW and not new['draft'] and len(new['assets'])==8
assert old['tag_name']==OLD and not old['draft'] and len(old['assets'])==19
assert api(prefix+'/git/ref/tags/'+NEW)['object']['sha']==SOURCE
releases=paged(prefix+'/releases')
assert {r['tag_name'] for r in releases if not r['draft']}=={NEW,KEEP,OLD}
before_releases={r['id']:r for r in releases}
before_branches={b['name']:b['commit']['sha'] for b in paged(prefix+'/branches')}
before_tags={r['ref']:r['object']['sha'] for r in api(prefix+'/git/refs/tags')}
assert before_branches[SELF]==os.environ['GITHUB_SHA']
for branch,sha in TARGETS.items():
    assert before_branches[branch]==sha,branch
    cmp=api(prefix+f'/compare/{sha}...{SOURCE}')
    assert cmp['status'] in ('ahead','identical') and cmp['behind_by']==0,branch
# Check both head and base references on open fork/upstream PRs; never close any PR.
for repo in (REPO,'farion1231/cc-switch'):
    for pr in paged(f'repos/{repo}/pulls?state=open'):
        for side in ('head','base'):
            ref=pr[side]
            if ref.get('repo') and ref['repo']['full_name']==REPO:
                assert ref['ref'] not in TARGETS and ref['ref']!=SELF,pr['html_url']
with tempfile.TemporaryDirectory() as tmp:
    subprocess.run(['gh','release','download',NEW,'--repo',REPO,'--dir',tmp],check=True)
    for a in new['assets']:
        p=pathlib.Path(tmp,a['name'])
        assert p.stat().st_size==a['size']
        assert 'sha256:'+hashlib.sha256(p.read_bytes()).hexdigest()==a['digest'],a['name']
    manifest=json.loads(pathlib.Path(tmp,'manifest.json').read_text(encoding='utf-8-sig'))
    assert manifest['source_commit']==SOURCE
    assert manifest['installer_payload_verified'] and manifest['payload_embedded_ui_verified']
    for line in pathlib.Path(tmp,'SHA256SUMS.txt').read_text(encoding='utf-8-sig').splitlines():
        digest,name=line.split(None,1)
        name=name.lstrip('*').strip()
        assert pathlib.PurePath(name).name==name
        assert hashlib.sha256(pathlib.Path(tmp,name).read_bytes()).hexdigest()==digest,name
# A durable archival tag preserves this audit, restoration map and workflow commit.
assert 'refs/tags/'+ARCHIVE not in before_tags
api(prefix+'/git/refs','POST',{'ref':'refs/tags/'+ARCHIVE,'sha':os.environ['GITHUB_SHA']})
assert api(prefix+'/git/ref/tags/'+ARCHIVE)['object']['sha']==os.environ['GITHUB_SHA']
# Archive, never delete, the previous published release and every asset.
after_old=api(prefix+'/releases/419307958','PATCH',{'draft':True})
assert after_old['draft'] and asset_snapshot(old)==asset_snapshot(after_old)
# A lease protects against a contributor changing a ref between checks and push.
assert api(prefix+'/git/ref/heads/main')['object']['sha']==SOURCE
remove=dict(TARGETS)
remove[SELF]=os.environ['GITHUB_SHA']
auth=base64.b64encode(('x-access-token:'+os.environ['GH_TOKEN']).encode()).decode()
cmd=['git','-c','http.https://github.com/.extraheader=AUTHORIZATION: basic '+auth,'push','--atomic']
cmd += [f'--force-with-lease=refs/heads/{b}:{sha}' for b,sha in remove.items()]
cmd += ['https://github.com/'+REPO+'.git']+[f':refs/heads/{b}' for b in remove]
p=subprocess.run(cmd,text=True,capture_output=True)
if p.returncode: raise RuntimeError('Atomic leased branch cleanup failed: '+p.stderr.replace(auth,'[redacted]'))
after_branches={b['name']:b['commit']['sha'] for b in paged(prefix+'/branches')}
assert after_branches=={b:s for b,s in before_branches.items() if b not in remove}
after_tags={r['ref']:r['object']['sha'] for r in api(prefix+'/git/refs/tags')}
assert after_tags==dict(before_tags,**{'refs/tags/'+ARCHIVE:os.environ['GITHUB_SHA']})
after_releases={r['id']:r for r in paged(prefix+'/releases')}
assert set(after_releases)==set(before_releases)
for rid,before in before_releases.items():
    after=after_releases[rid]
    assert asset_snapshot(before)==asset_snapshot(after)
    assert before['tag_name']==after['tag_name'] and before['body']==after['body'] and before['name']==after['name']
    assert after['draft']==(True if rid==419307958 else before['draft'])
assert {r['tag_name'] for r in after_releases.values() if not r['draft']}=={NEW,KEEP}
print(json.dumps({'status':'verified','main':SOURCE,'deleted_branches':remove,'preserved_branches':after_branches,'archive_tag':ARCHIVE,'published_releases':[NEW,KEEP],'archived_release_id':419307958,'archived_asset_count':len(after_old['assets'])},indent=2))
