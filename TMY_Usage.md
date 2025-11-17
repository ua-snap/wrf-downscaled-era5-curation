# Parallel TMY Processing - Usage Guide

## Overview

The parallel TMY processing system splits the workload by calendar month, allowing 12 months to be processed simultaneously on different Chinook compute nodes.

## Architecture

```
┌─────────────────────────────────────────────────┐
│         submit_tmy_parallel.py                  │
│         (Submission Wrapper)                    │
└─────────────────────────────────────────────────┘
                      │
                      ├─────────────────┐
                      ↓                 ↓
┌──────────────────────────────┐  ┌───────────────────────────┐
│ Job Array (12 jobs)          │  │ Stitching Job (1 job)     │
│ process_tmy_month.py         │  │ stitch_tmy.py             │
│                              │  │                           │
│ Month 1: Jan → month_01.nc   │  │ Waits for array to finish │
│ Month 2: Feb → month_02.nc   │  │                           │
│ Month 3: Mar → month_03.nc   │  │ 1. Loads 12 monthly files │
│ ...                          │  │ 2. Concatenates           │
│ Month 12: Dec → month_12.nc  │  │ 3. Reprojects to 3338     │
│                              │  │ 4. Writes final TMY       │
└──────────────────────────────┘  └───────────────────────────┘
```

## Quick Start

```bash
python submit_tmy_parallel.py \
    --start_year 2010 \
    --end_year 2012 \
```

## File Structure

### Input Files
- Source data: `/beegfs/CMIP6/wrf_era5/04km/YYYY/era5_wrf_dscale_4km_YYYY-MM-DD.nc`

### Intermediate Files
- Location: `$TMY_OUTPUT_DIR/tmy/intermediate/`
- Format: `month_01_2010_2020.nc` through `month_12_2010_2020.nc`
- Size: ~400 MB each
- Notes: Good for debugging, but don't need to be retained after final TMY is created

### Final Output
- Location: `$TMY_OUTPUT_DIR/tmy/`
- Format: `t2_tmy_2010_2020_era5_4km_3338.nc`
- Size: ~5 GB (full domain)

### Logs
- Location: `logs/era5_tmy/`
- Monthly jobs: `month_JOBID_MONTH.out` (e.g., `month_12345_1.out` for January)
- Stitching job: `stitch_JOBID.out`

## Manual Execution for Development

### Python Worker Scripts

#### Process Single Month, Test Worker Script in Isolation
```bash
python process_tmy_month.py \
    --month 1 \
    --start_year 2010 \
    --end_year 2012 \
```

#### Stitch Months Together, Test Worker Sript in Isolation
```bash
python stitch_tmy.py \
    --start_year 2010 \
    --end_year 2012 \
```

### SLURM Submissions

#### Submit the Job Array (all months)
```bash
sbatch process_era5_tmy_month_array.sbatch 2010 2020
```

### Submit Job to Stitch Together the Individual Months

```bash
# Wait for array job to complete, then:
sbatch process_era5_tmy_stitch.sbatch 2010 2020
```

## Non-Prefect Monitoring

### Check Job Status
```bash
# Watch continuously
watch squeue -u $USER
```

### View Logs
```bash
# Monthly jobs (follow all), can get noisy
tail -f logs/era5_tmy/month_*
# Stitching job
tail -f logs/era5_tmy/stitch_*.out
```
