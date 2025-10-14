import argparse
import json
import logging
import os
import sys

from concurrent.futures import ProcessPoolExecutor
from functools import reduce
from pathlib import Path


logger = logging.getLogger(__name__)


def _define_args():
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument('--what', 
                             type=str,
                             required=True,
                             help='what to update [mips,matches]')

    args_parser.add_argument('-i', '--input', 
                             type=str,
                             required=True,
                             help='input path')

    args_parser.add_argument('-o', '--output',
                             type=str,
                             required=True,
                             help='output path')

    args_parser.add_argument('--nworkers',
                             type=int,
                             default=1,
                             help='number of concurrent workers')

    args_parser.add_argument('--verbose',
                             default=False,
                             action='store_true',
                             help='log verbose')

    return args_parser


def _scan_dir(d, suffix='.json'):
    for entry in os.scandir(d):
        if entry.is_file() and entry.name.endswith(suffix):
            # only go 1 level for now
            yield entry.path


def _update_match_files(input_dir, output_dir, nworkers=1):
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f'Update {input_path} -> {output_path}')

    updated_files = 0
    if nworkers > 1:
        with ProcessPoolExecutor(max_workers=nworkers) as tp:
            updated_files = reduce(lambda a, i: a + i,
                                   tp.map(_update_match_file, [(p, output_path) for p in _scan_dir(input_path)]))
    else:
        updated_files = reduce(lambda a, i: a + i,
                                map(_update_match_file, [(p, output_path) for p in _scan_dir(input_path)]))

    logger.info(f'Updated {updated_files} files from {input_path}')
    

def _update_match_file(args):
    input_file, output_dir = args
    input_filename = os.path.basename(input_file)

    try:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.debug(f'Read matches from {input_file}')
        input_image = data.get("inputImage")
        results = data.get("results")
        alignmentSpace = input_image.get('alignmentSpace')

        if  alignmentSpace == 'JRC2018_Unisex_20x_HR':
            anatomical_area = 'Brain'
        elif alignmentSpace == 'JRC2018_VNC_Unisex_40x_DS':
            anatomical_area = 'VNC'
        else:
            logger.error(f'Invalid alignment space ({alignmentSpace}) found for input image {input_image} in {input_file}')
            return 0
        
        input_image['anatomicalArea'] = anatomical_area
        for result in results:
            result_image = result.get('image')
            if result_image.get('alignmentSpace') != alignmentSpace:
                logger.error(f'Invalid alignment space found for result image {result_image} in {input_file}')
            else:
                result_image['anatomicalArea'] = anatomical_area

        # Compute relative output path
        target = Path(f'{output_dir}/{input_filename}')
        target.parent.mkdir(parents=True, exist_ok=True)

        logger.debug(f'Write updated match results {target}')
        with open(target, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return 1

    except (json.JSONDecodeError, OSError) as e:
        logger.error(f'Error processing {input_file}: {e}')
        return 0
    

def _update_mips_files(input_dir, output_dir, nworkers=1):
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f'Update {input_path} -> {output_path}')

    updated_files = 0
    if nworkers > 1:
        with ProcessPoolExecutor(max_workers=nworkers) as tp:
            updated_files = reduce(lambda a, i: a + i,
                                   tp.map(_update_mips_file, [(p, output_path) for p in _scan_dir(input_path)]))
    else:
        updated_files = reduce(lambda a, i: a + i,
                                map(_update_mips_file, [(p, output_path) for p in _scan_dir(input_path)]))
    
    logger.info(f'Updated {updated_files} files from {input_path}')


def _update_mips_file(args):
    input_file, output_dir = args
    try:
        input_filename = os.path.basename(input_file)
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.debug(f'Read MIPs from {input_file}')
        results = data.get("results")

        for result_image in results:
            if result_image.get("alignmentSpace") == 'JRC2018_Unisex_20x_HR':
                result_image['anatomicalArea'] = 'Brain'
            elif result_image.get("alignmentSpace") == 'JRC2018_VNC_Unisex_40x_DS':
                result_image['anatomicalArea'] = 'VNC'
            else:
                logger.error(f'Invalid alignment space found for result image {result_image} in {input_file}')

        # Compute relative output path
        target = Path(f'{output_dir}/{input_filename}')
        target.parent.mkdir(parents=True, exist_ok=True)

        logger.debug(f'Write updated MIPs {target}')
        with open(target, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return 1
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f'Error processing {input_file}: {e}')
        return 0
    

if __name__ == '__main__':
    args_parser = _define_args()
    args = args_parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=log_level,
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[
                            logging.StreamHandler(stream=sys.stdout)
                        ])

    input_path = Path(args.input)
    output_path = Path(args.output)
    if args.what == 'mips':
        if input_path.is_file():
            _update_mips_file((input_path, args.output))
        else:
            _update_mips_files(input_path, args.output, nworkers=args.nworkers)
        
    elif args.what == 'matches':
        if input_path.is_file():
            _update_match_file((input_path, args.output))
        else:
            _update_match_files(input_path, args.output, nworkers=args.nworkers)
    else:
        print(f'Invalid {args.what} - valid values are: mips, matches')
