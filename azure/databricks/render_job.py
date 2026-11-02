"""Render a serial existing-cluster job definition; does not contact Databricks."""
import argparse
import json
import re


def render(cluster_id, notebook_path):
    if not re.fullmatch(r'[0-9]{4}-[0-9]{6}-[a-z0-9]+', cluster_id):
        raise ValueError('Provide an actual approved existing cluster ID')
    if not notebook_path.startswith('/') or '..' in notebook_path.split('/'):
        raise ValueError('Provide the absolute imported notebook workspace path')
    parameters = {'catalog': 'mobility', 'schema': 'yellow', 'source_month': '2024-01',
                  'source_uri': '', 'expected_sha256': '', 'expected_bytes': '', 'allow_revision': 'false'}
    return {
        'name': 'mobility-publish-month', 'max_concurrent_runs': 1,
        'timeout_seconds': 1800, 'queue': {'enabled': True},
        'parameters': [{'name': k, 'default': v} for k, v in parameters.items()],
        'tasks': [{
            'task_key': 'publish', 'existing_cluster_id': cluster_id,
            'timeout_seconds': 1800, 'max_retries': 1, 'min_retry_interval_millis': 60000,
            'notebook_task': {'notebook_path': notebook_path,
                              'base_parameters': {k: '{{job.parameters.' + k + '}}' for k in parameters}},
        }],
        'tags': {'project': 'urban-mobility-lakehouse', 'stage': 'unvalidated-lab'},
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster-id', required=True)
    parser.add_argument('--notebook-path', required=True)
    args = parser.parse_args()
    print(json.dumps(render(args.cluster_id, args.notebook_path), indent=2))
