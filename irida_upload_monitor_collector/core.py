import collections
import csv
import glob
import json
import logging
import os
import re
import shutil

from pathlib import Path
from typing import Iterator, Optional


def create_output_dirs(config):
    """
    Create output directories if they don't exist.

    :param config: Application config.
    :type config: dict[str, object]
    :return: None
    :rtype: None
    """
    base_outdir = config['output_dir']
    output_dirs = [
        base_outdir,
        os.path.join(base_outdir, 'uploaded_sequencing_runs'),
    ]
    for output_dir in output_dirs:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)    


def find_upload_dirs(config, check_complete=True):
    """
    """
    miseq_run_id_regex = r'\d{6}_M\d{5}_\d+_\d{9}-[A-Z0-9]{5}'
    nextseq_run_id_regex = r'\d{6}_VH\d{5}_\d+_[A-Z0-9]{9}'
    uploaded_sequencing_runs_dir = config['uploaded_sequencing_runs_dir']
    subdirs = os.scandir(uploaded_sequencing_runs_dir)
    
    for subdir in subdirs:
        run_id = subdir.name
        matches_miseq_regex = re.match(miseq_run_id_regex, run_id)
        matches_nextseq_regex = re.match(nextseq_run_id_regex, run_id)
        sequencer_type = None
        if matches_miseq_regex:
            sequencer_type = 'miseq'
        elif matches_nextseq_regex:
            sequencer_type = 'nextseq'
        not_excluded = run_id not in config['excluded_runs']
        ready_to_collect = False
        
        if True: # TODO: Replace with actual check
            ready_to_collect = True
            

        conditions_checked = {
            "is_directory": subdir.is_dir(),
            "matches_illumina_run_id_format": ((matches_miseq_regex is not None) or (matches_nextseq_regex is not None)),
            "not_excluded": not_excluded,
            "ready_to_collect": ready_to_collect,
        }
        conditions_met = list(conditions_checked.values())

        uploaded_sequencing_run_directory_path = os.path.abspath(subdir.path)
        uploaded_sequencing_run_dir = {
            "path": uploaded_sequencing_run_directory_path,
            "sequencer_type": sequencer_type,
        }
        if all(conditions_met):
            logging.info(json.dumps({
                "event_type": "uploaded_sequencing_run_directory_found",
                "sequencing_run_id": run_id,
                "uploaded_sequencing_run_directory_path": uploaded_sequencing_run_directory_path
            }))

            yield uploaded_sequencing_run_dir
        else:
            logging.debug(json.dumps({
                "event_type": "directory_skipped",
                "analysis_directory_path": os.path.abspath(subdir.path),
                "conditions_checked": conditions_checked
            }))
            yield None


def find_all_uploaded_sequencing_runs(config):
    """
    Finda all runs that have been uploaded.

    :param config: Application config.
    :type config: dict[str, object]
    :return: List of runs. Keys: ['run_id', 'sequencer_type']
    :rtype: list[dict[str, str]]
    """
    logging.info(json.dumps({"event_type": "find_uploaded_sequencing_runs_start"}))
    uploaded_sequencing_runs = []
    all_uploaded_sequencing_run_dirs = sorted(list(os.listdir(config['uploaded_sequencing_runs_dir'])))
    all_run_ids = filter(lambda x: re.match(r'\d{6}_[VM]', x) != None, all_uploaded_sequencing_run_dirs)
    for run_id in all_run_ids:
        if run_id in config['excluded_runs']:
            continue

        sequencer_type = None
        if re.match(r'\d{6}_M\d{5}_', run_id):
            sequencer_type = 'miseq'
        elif re.match(r'\d{6}_VH\d{5}_', run_id):
            sequencer_type = 'nextseq'
        
        uploaded_sequencing_run = {
            'sequencing_run_id': run_id,
            'sequencer_type': sequencer_type,
        }

        uploaded_sequencing_run_dir = os.path.join(config['uploaded_sequencing_runs_dir'], run_id)
    
        upload_started_path = os.path.join(uploaded_sequencing_run_dir, 'upload_started.json')  
        if os.path.exists(upload_started_path):
            with open(upload_started_path, 'r') as f:
                try:
                    upload_started = json.load(f)
                except json.decoder.JSONDecodeError as e:
                    upload_started = {}
                timestamp_upload_started = upload_started.get('timestamp_upload_started', None)
            if timestamp_upload_started is not None:
                uploaded_sequencing_run['timestamp_upload_started'] = timestamp_upload_started

        upload_completed_path = os.path.join(uploaded_sequencing_run_dir, 'upload_completed.json')
        if os.path.exists(upload_completed_path):
            with open(upload_completed_path, 'r') as f:
                try:
                    upload_completed = json.load(f)
                except json.decoder.JSONDecodeError as e:
                    upload_completed = {}
                timestamp_upload_completed = upload_completed.get('timestamp_upload_completed', None)
            if timestamp_upload_completed is not None:
                uploaded_sequencing_run['timestamp_upload_completed'] = timestamp_upload_completed

        uploaded_sequencing_runs.append(uploaded_sequencing_run)

    logging.info(json.dumps({
        "event_type": "find_uploaded_sequencing_runs_complete"
    }))

    return uploaded_sequencing_runs


def scan(config: dict[str, object]) -> Iterator[Optional[dict[str, str]]]:
    """
    Scanning involves looking for all existing runs and...

    :param config: Application config.
    :type config: dict[str, object]
    :return: A run directory to analyze, or None
    :rtype: Iterator[Optional[dict[str, object]]]
    """
    logging.info(json.dumps({"event_type": "scan_start"}))
    for upload_dir in find_upload_dirs(config):
        yield upload_dir
    
    
def collect_outputs(config: dict[str, object], uploaded_run_dir: Optional[dict[str, str]]):
    """
    Collect QC outputs for a specific analysis dir.

    :param config: Application config.
    :type config: dict[str, object]
    :param uploaded_run_dir: Analysis dir. Keys: ['path']
    :type uploaded_run_dir: dict[str, str]
    :return: 
    :rtype: 
    """
    uploaded_run_dir_path = uploaded_run_dir['path']
    run_id = os.path.basename(uploaded_run_dir_path)
    logging.info(json.dumps({"event_type": "collect_outputs_start", "sequencing_run_id": run_id, "uploaded_sequencing_run_dir_path": uploaded_run_dir_path}))
    upload_prepared_path = os.path.join(uploaded_run_dir_path, 'upload_prepared.json')
    if os.path.exists(upload_prepared_path):
        with open(upload_prepared_path, 'r') as f:
            try:
                upload_prepared = json.load(f)
            except json.decoder.JSONDecodeError as e:
                upload_prepared = None
        libraries = None
        if upload_prepared is not None:
            libraries = upload_prepared.get('libraries', None)
        if libraries is not None:
            uploaded_sequencing_run_dir_path = os.path.join(config['output_dir'], 'uploaded_sequencing_runs', run_id + ".json")
            with open(uploaded_sequencing_run_dir_path, 'w') as f:
                json.dump(libraries, f, indent=2)
                f.write('\n')
            logging.info(json.dumps({"event_type": "write_uploaded_sequencing_run_file_complete", "sequencing_run_id": run_id, "uploaded_sequencing_run_dir_path": uploaded_sequencing_run_dir_path}))

    logging.info(json.dumps({"event_type": "collect_outputs_complete", "sequencing_run_id": run_id, "uploaded_sequencing_run_dir_path": uploaded_run_dir_path}))
