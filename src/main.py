from repeat_donations import get_repeat_donations
import argparse

if __name__ == '__main__':

    ap = argparse.ArgumentParser(description='Repeat Donations CLI')
    h = 'Input file path'
    ap.add_argument('-i', '--input_path', required=True, help=h)
    h = 'Output file path'
    ap.add_argument('-o', '--output_path', required=True, help=h)
    h = 'Percentile file path'
    ap.add_argument('-p', '--percentile_path', required=True, help=h)
    h = 'Number of lines to buffer before writing to disk'
    ap.add_argument('-b', '--buffer_size', default=1000, help=h, type=int)
    args = vars(ap.parse_args())

    # TODO: use 'with' context for all three files.
    with open(args['input_path']) as input_fp, \
            open(args['output_path'], 'w') as output_fp, \
            open(args['percentile_path']) as percentile_fp:

        get_repeat_donations(input_fp, output_fp,
                             percentile_fp, args['buffer_size'])
