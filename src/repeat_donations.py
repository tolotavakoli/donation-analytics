from bisect import insort_left
from collections import defaultdict
from math import ceil
from time import time


# Columns for individual contribution:
_FEC_COLS = ['cmte_id', 'amndt_ind', 'rpt_tp', 'transaction_pgi', 'image_num',
             'transaction_tp', 'entity_tp', 'name', 'city', 'state', 'zip_code',
             'employer', 'occupation', 'transaction_dt', 'transaction_amt',
             'other_id', 'tran_id', 'file_num', 'memo_cd', 'memo_text', 'sub_id']


class CampaignHistory(object):
    """a container class for the campaign history"""

    def __init__(self):
        self.amounts = []


class DonorHistory(object):
    """a container class for the donors history"""

    def __init__(self):
        self.max_year = 0
        self.count = 0


def read_dict(inpt, cols, ucol, sep):
    """Reads lines from an iterable and yields a dict for each line containing the desired columns.

    # Arguments
        inpt: a file-pointer, list, or other iterable emitting one line at a time.
        cols: a list of all column names expected at each line.
        ucol: a list of the column names which should be yielded at each line.
        sep: the separator (delimiter) for values on each line.

    # Returns
        yields a dict for each line containing the columns in ucol.

    """
    c2i = {c: i for i, c in enumerate(cols)}
    uci = [(c, c2i[c]) for c in ucol]
    for line in inpt:
        values = line.split(sep)
        yield {c: values[i] for c, i in uci}


def nearest_rank_percentile(a, p):
    """Return the nearest rank percentile value from a sorted list.

    # Arguments
        a: A sorted list-like object.
        p: An integer in [1, 100] indicating the percentile to return.

    # Returns
        The list value corresponding to given percentile.
    """
    n = int(ceil(p / 100 * len(a)))
    return a[n - 1]


def skip_input_row(input_row):
    """Return a boolean indicating whether the input_row should be skipped.

    # Arguments
        input_row: An object supporting key-based value access (e.g., python dict,
        pandas' Series).

    # Returns
        Boolean indicating whether or not to skip.

    """

    return len(input_row['other_id']) > 1 \
        or len(input_row['zip_code']) < 5 \
        or len(input_row['transaction_dt']) != 8 \
        or len(input_row['name']) == 0 \
        or len(input_row['cmte_id']) == 0 \
        or len(input_row['transaction_amt']) == 0


def clean_input_row(input_row):
    """Clean the input_row's values for further processing.

    The input_row is modified in-place; returning it makes it possible to use
    with pandas' apply() function.

    # Arguments
        input_row: An object supporting key-based value access (e.g., python dict,
        pandas' Series).

    # Returns
        The modified input_row, which is also modified in-place.
    """

    # Use only the first five chars of the zip code.
    # Keep the zipcode as a str to avoid dropping leading 0's.
    input_row['zip_code'] = input_row['zip_code'][:5]

    # Extract year from the date property.
    input_row['year'] = int(str(input_row['transaction_dt'])[-4:])

    # Cast transaction amount to float.
    input_row['transaction_amt'] = float(input_row['transaction_amt'])

    return input_row


def input_row_to_campaign_key(input_row):
    """Return a unique identifier for the input_row's campaign.

    # Arguments
        input_row: An object supporting key-based value access (e.g., python dict,
        pandas' Series).

    # Returns
        Tuple identifier.
    """
    return (input_row['cmte_id'], input_row['zip_code'], input_row['year'])


def input_row_to_donor_key(input_row):
    """Return a unique identifier for the donor in this input_row.

    # Arguments
        input_row: An object supporting key-based value access (e.g., python dict,
        pandas' Series).

    # Returns
        Tuple identifier.
    """
    return (input_row['name'], input_row['zip_code'])


def update_donor_history(dnr_hst, input_row):
    """Modify a DonorHistory object to reflect the donation in input_row.

    # Arguments
        dnr_hist: DonorHistory object.

    # Returns
        None (object is modified in-place).

    """
    dnr_hst.max_year = max(dnr_hst.max_year, input_row['year'])
    dnr_hst.count += 1


def skip_donor_history(dnr_hst, input_row):
    """Given a DonorHistory object and a dict or pandas series input_row object,
    return a boolean indicating whether the donor should be skipped.

    Donor is skipped if:
    - not a repeat donor
    - the input_row's year is not greater/equal donor's previous max year (see the
    second FAQ for this implementation detail).

    # Arguments
        dnr_hist: DonorHistory object.
        input_row: An object supporting key-based value access (e.g., python dict,
        pandas' Series).

    # Returns
        Boolean indicating whether to skip.

    """

    return dnr_hst.count < 2 or dnr_hst.max_year != input_row['year']


def update_campaign_history(cmp_hst, input_row):
    """Modify a CampaignHistory object to reflect the donation in the given input_row.

    This involves adding the input_row's transaction amount in such a way that
    preserves the list in sorted order. insort_left makes this an O(n) operation
    instead of using python's sort() which is O(n log(n)).

    # Arguments
        cmp_hst: CampaignHistory object.
        input_row: An object supporting key-based value access (e.g., python dict,
        pandas' Series).

    # Returns
        None (object is modified in place).

    """
    insort_left(cmp_hst.amounts, input_row['transaction_amt'])


def get_output_str(cmte_id, zip_code, year, percentile, sum_, count):
    """Formats the given values according to the challenge output spec.

    # Arguments
        cmte_id: cmte_id for this campaign.
        zip_code: zip_code for this campaign.
        year: year for this campaign.
        percentile: percentile computed from this campaign's donations.
        sum_: sum of donations received by campaign.
        count: number of donations received.

    # Returns
        String

    """

    # If the sum of donations is a whole number, print it as an int.
    # Otherwise, print as a 2-decimal float.
    if int(sum_) != sum_:
        sum_ = '%.2lf' % sum_
    else:
        sum_ = '%d' % sum_

    return '%s|%s|%d|%d|%s|%d\n' % (
        cmte_id, zip_code, year, round(percentile), sum_, count)


def get_repeat_donations(input_fp, output_fp, percentile_fp, buffer_size=1000):
    """Parse an input file and write the repeat donations to an output file.

    This is the primary function exposed from this module. In combination with
    the helper functions defined in this module, it implements the logic
    required by the coding challenge.

    # Arguments
        input_fp: a file-pointer or buffer object from which the input
        rows are read. It's assumed that the input rows follow the FEC's pattern
        for individual contribution records described here:
        https://classic.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml

        output_fp: a file-pointer or buffer object to which the output rows
        are written. The rows' format follows the format/requirements in the
        coding challenge README.

        perfentile_fp: a file-pointer or iterable object from which a
        percentile value is read. It's assumed that the file contains a single
        integer in [1, 100]. e.g. a text file with "30" or iter([30]) will work.

        buffer_size: an integer indicating the size of the buffer to maintain
        before flushing to the output_fp. The internal buffer is maintained to
        make writes to disk more efficient.

    # Returns
        None

    """

    # Starting time.
    t0 = time()

    # read_dict generator for iterating through data file. Each row in the
    # data file is emitted as a Dict with column names as keys. Note that this
    # avoids loading all data into memory.
    usecols = ['name', 'zip_code', 'cmte_id', 'transaction_amt',
               'transaction_dt', 'other_id']
    input_iter = read_dict(input_fp, _FEC_COLS, usecols, '|')

    # Read the desired percentile.
    percentile = int(next(percentile_fp))

    # Map campaign key -> CampaignHistory object.
    cmp_hsts = defaultdict(CampaignHistory)

    # Map donor key -> DonorHistory object.
    dnr_hsts = defaultdict(DonorHistory)

    # Buffer to hold the lines before writing them to file.
    buffer_cnt = 0
    buffer_str = ''

    for i, input_row in enumerate(input_iter):

        # Check if this row needs to be skipped.
        if skip_input_row(input_row):
            continue

        # Clean up the row values.
        clean_input_row(input_row)

        # Compute unique keys for donor and campaign.
        dkey = input_row_to_donor_key(input_row)
        ckey = input_row_to_campaign_key(input_row)

        # Update the donor's history.
        update_donor_history(dnr_hsts[dkey], input_row)

        # Check if this donor needs to be skipped.
        if skip_donor_history(dnr_hsts[dkey], input_row):
            continue

        # This donor is a repeat donor. Update the campaign history.
        update_campaign_history(cmp_hsts[ckey], input_row)

        # Add the output row to the buffer.
        buffer_str += get_output_str(
            *ckey, nearest_rank_percentile(cmp_hsts[ckey].amounts, percentile),
            sum(cmp_hsts[ckey].amounts), len(cmp_hsts[ckey].amounts)
        )
        buffer_cnt += 1

        # Flush the buffer when it hits the specified size.
        if buffer_cnt % buffer_size == 0:
            print('Processed %d rows in %.1lf seconds' % (i + 1, time() - t0))
            output_fp.write(buffer_str)
            buffer_str = ''
            buffer_cnt = 0

    # Flush the buffer once more.
    print('Processed %d rows in %.1lf seconds' % (i + 1, time() - t0))
    output_fp.write(buffer_str)
