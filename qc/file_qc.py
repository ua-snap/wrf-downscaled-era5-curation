import os
import glob
from collections import defaultdict
from typing import Set, Dict, Optional

import xarray as xr


def ensure_files_open(var_list):
    for v in var_list:
        files = era5plots.list_files_for_variable(v)
        
        for fp in tqdm(files, desc=f"Checking {v} NetCDF files"):
            try:
                with xr.open_dataset(fp) as ds:
                    pass  # File is OK
            except Exception as e:
                print(f"\n❌ BROKEN: {fp}")
                print(e)


def check_nc_variables_consistency_verbose(base_dir: str, print_progress: bool = True) -> Dict[str, any]:
    """Ensure that input netCDF files all have the same set of data variables."""
    
    stats = {
        'files_checked': 0,
        'files_failed': 0,
        'variable_sets': defaultdict(list),
        'inconsistent_files': []
    }
    
    first_vars = None
    
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.nc'):
                filepath = os.path.join(root, file)
                stats['files_checked'] += 1
                
                if print_progress and stats['files_checked'] % 100 == 0:
                    print(f"Checked {stats['files_checked']} files...")
                
                try:
                    with xr.open_dataset(filepath) as ds:
                        current_vars = frozenset(ds.data_vars.keys())
                        
                        if first_vars is None:
                            first_vars = current_vars
                            stats['baseline_variables'] = current_vars
                        
                        if current_vars != first_vars:
                            stats['inconsistent_files'].append(filepath)
                        
                        stats['variable_sets'][current_vars].append(filepath)
                except Exception as e:
                    stats['files_failed'] += 1
                    if print_progress:
                        print(f"Error reading {filepath}: {str(e)}")
                    continue
    
    stats['consistent'] = len(stats['variable_sets']) == 1
    stats['num_variable_sets'] = len(stats['variable_sets'])
    return stats


if __name__ == "__main__":
    results = check_nc_variables_consistency_verbose("/beegfs/CMIP6/wrf_era5/04km/", print_progress=True)

    if results['consistent']:
        print(f"All {results['files_checked']} files have the same variables")
        print("Variables:", sorted(results['baseline_variables']))
    else:
        print(f"Found {results['num_variable_sets']} different variable sets across {results['files_checked']} files")
        for varset, files in results['variable_sets'].items():
            print(f"\nVariables: {sorted(varset)}")
            print(f"Number of files: {len(files)}")
            print("Example file:", files[0])




