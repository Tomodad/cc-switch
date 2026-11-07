import base64,json,os,subprocess
REPO='Tomodad/cc-switch'
MAIN='119feb202ff6b55b05a9e295cb75f4d5b951c8d8'
SELF='codex/archive-obsolete-branches-20261107'
TARGETS={
 'codex/archive-v4.0.15-20261104':'512b9b30fcea9cde8e39e9ede103cdf4e0012683',
 'codex/publish-verified-v4.0.25-20261104':'4230f5c9edda39505360d3cc9358f8ea71cdf85c',
 'codex/test-isolation-v4.0.25-20261104':'0201c219c6057fdc76b0c6cd6b148db7afc2d891',
 'codex/tool-search-shim-v4.0.8':'440a55d31c89413e41f566d69a6065b95382761a',
}
# Every removed branch is recoverable from archive/20261107/branches/BRANCH.
# Restore with gh api -X POST repos/Tomodad/cc-switch/git/refs
#   -f ref=refs/heads/BRANCH -f sha=THE_SHA_ABOVE .
# Existing tags, releases, PRs and application files are not modified.
def api(path,method='GET',data=None):
 c=['gh','api',path,'--method',method]
 if data is not None:c+=['--input','-']
 p=subprocess.run(c,input=json.dumps(data) if data is not None else None,text=True,capture_output=True)
 if p.returncode:raise RuntimeError(f'{method} {path}: {p.stderr}')
 return json.loads(p.stdout) if p.stdout.strip() else None
def paged(path):
 out=[]
 for page in range(1,100):
  rows=api(path+('&' if '?' in path else '?')+f'per_page=100&page={page}')
  out.extend(rows)
  if len(rows)<100:return out
 raise RuntimeError('Pagination limit')
prefix='repos/'+REPO
before={b['name']:b['commit']['sha'] for b in paged(prefix+'/branches')}
assert before['main']==MAIN and before[SELF]==os.environ['GITHUB_SHA']
for b,sha in TARGETS.items():assert before[b]==sha,b
assert set(before)==set(TARGETS)|{SELF,'main','codex/codex-dot-unified-identity','codex/codex-portable-history'}
releases=paged(prefix+'/releases')
tags={r['ref']:r['object']['sha'] for r in api(prefix+'/git/refs/tags')}
for repo in (REPO,'farion1231/cc-switch'):
 for pr in paged(f'repos/{repo}/pulls?state=open'):
  for side in ('head','base'):
   ref=pr[side]
   if ref.get('repo') and ref['repo']['full_name']==REPO:
    assert ref['ref'] not in TARGETS and ref['ref']!=SELF,pr['html_url']
remove=dict(TARGETS)
remove[SELF]=os.environ['GITHUB_SHA']
created={}
for b,sha in remove.items():
 tag='archive/20261107/branches/'+b
 ref='refs/tags/'+tag
 if ref in tags:assert tags[ref]==sha
 else:api(prefix+'/git/refs','POST',{'ref':ref,'sha':sha})
 assert api(prefix+'/git/ref/tags/'+tag)['object']['sha']==sha
 created[ref]=sha
# Durable remote tags now retain every unique commit and the restore map.
assert api(prefix+'/git/ref/heads/main')['object']['sha']==MAIN
for b,sha in remove.items():assert api(prefix+'/git/ref/heads/'+b)['object']['sha']==sha
header=base64.b64encode(('x-access-token:'+os.environ['GH_TOKEN']).encode()).decode()
c=['git','-c','http.https://github.com/.extraheader=AUTHORIZATION: basic '+header,'push','--atomic']
c += [f'--force-with-lease=refs/heads/{b}:{sha}' for b,sha in remove.items()]
c += ['https://github.com/'+REPO+'.git']+[f':refs/heads/{b}' for b in remove]
p=subprocess.run(c,text=True,capture_output=True)
if p.returncode:raise RuntimeError(p.stderr.replace(header,'[redacted]'))
after={b['name']:b['commit']['sha'] for b in paged(prefix+'/branches')}
assert after=={b:s for b,s in before.items() if b not in remove}
after_tags={r['ref']:r['object']['sha'] for r in api(prefix+'/git/refs/tags')}
assert after_tags==dict(tags,**created)
# Release state, metadata and assets are not part of this cleanup.
after_releases=paged(prefix+'/releases')
fields=('id','tag_name','draft','name','body')
def snap(rs):return sorted((tuple(r.get(k) for k in fields), sorted((a['id'],a['name'],a['size'],a.get('digest')) for a in r['assets'])) for r in rs)
assert snap(releases)==snap(after_releases)
print(json.dumps({'status':'verified','archived_and_removed':remove,'preserved_branches':after,'archive_tags':created},indent=2))
