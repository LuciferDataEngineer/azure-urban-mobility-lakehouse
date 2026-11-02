"""Compile Bicep offline. Never authenticate, validate against Azure, or deploy."""
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix='mobility-bicep-') as target:
    for stem in ('main', 'orchestration'):
        subprocess.run(['az', 'bicep', 'build', '--file', str(root / f'infra/{stem}.bicep'),
                        '--outfile', str(Path(target) / f'{stem}.json')], check=True)
    subprocess.run(['az', 'bicep', 'build-params', '--file', str(root / 'infra/dev.bicepparam'),
                    '--outfile', str(Path(target) / 'dev.json')], check=True)
    production = subprocess.run(['az', 'bicep', 'build-params', '--file', str(root / 'infra/prod.bicepparam'),
                                 '--outfile', str(Path(target) / 'prod.json')], capture_output=True, text=True)
    if production.returncode == 0 or 'BCP033' not in production.stderr or "'prod'" not in production.stderr:
        raise SystemExit('Expected explicit dev-only contract to reject production parameters')
print('PASS: two Bicep templates + dev parameters compiled; production rejected by dev-only contract')
