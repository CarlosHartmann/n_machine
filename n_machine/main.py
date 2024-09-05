'''
n_machine: Made to pull baseline data for the flair study. Requires the definitive(!) data that result after processing all annotations and include additional post-hoc extractions.

Usage:
Basic command:
poetry run python path/to/n_machine/n_machine/main.py

Required args:
--input or -I: path to directory containing the Pushshift data dumps
--output or -O: desired output path
--baseline_nr: 1 or 2, to set the extraction to be either "same community as declarers without pronoun declarations" or "random communities

Optional args mostly from the otacon project.


'''

import os
import re
import csv
import json
import pickle
import random
import logging
import calendar
import argparse
from datetime import datetime
from typing import TextIO
import pandas as pd
import langdetect
from langdetect import detect

from zstandard import ZstdDecompressor

# keep track of already-processed comments throughout function calls
hash_list = []

# return stats from which subreddits the relevant comments were and how many per subreddits
stats_dict = {}

pronouns_path = os.path.expanduser("~/Documents/GitHub/pronounlist/Pronouns")
pronouns = list()
for root, dirs, files in os.walk(pronouns_path):
	for file in files:
		if not file.startswith('.'):
			filepath = os.path.join(root, file)
			with open(filepath) as infile:
				prons = infile.read().split('\n')
				for elem in prons:
					pronouns.append(elem)

with open("/Users/chartman/Documents/GitHub/n_machine/output/baseline1b/userlist.pkl", "rb") as infile:
    userlist = set(pickle.load(infile))

# dedupe
pronouns = list(set(pronouns))
# sort by length to ensure correct matching
pronouns = sorted(pronouns, key=lambda x: len(x), reverse=True)


pronouns_bars = '|'.join(pronouns)
pronouns_regex = f'(?:{pronouns_bars})'
pronouns_regex = f'\\b{pronouns_regex}/{pronouns_regex}\\b'

free_pronoun_regex = '(.+)/\\2s(elf)?\\b'

noanyall_regex = "(no|any|all).pronouns?"

combined_regexes = '|'.join([pronouns_regex, free_pronoun_regex, noanyall_regex])
combined_negative_regex = f'(?:({combined_regexes}))'

# comparison data
path = "./assets/pronoun_declarers.pkl"
declarers = pd.read_pickle(path)


# Results data
subs = declarers['subreddit'].unique().tolist()

def reset_reservoir_and_results():
    'returns an empty reservoir and per-sub results for the subs we are interested in'
    reservoir = dict()
    monthly_results = dict()
    for sub in subs:
        monthly_results.setdefault(sub, [])
        reservoir.setdefault(sub, {'N':0, 'K':0})
    return monthly_results, reservoir
    

def generate_k(subreddit: str, year:int, month:int, data=declarers):
    count = ((data['year'] == year) & (data['month'] == month) & (data['subreddit'] == subreddit)).sum()
    return count


def find_all_matches(text, regex):
    """Iterate through all regex matches in a text, yielding the span of each as tuple."""
    r = re.compile(regex)
    for match in r.finditer(text):
        yield (match.start(), match.end())


def inside_quote(text: str, span: tuple) -> bool:
    """
    Test if a span-marked match is inside a quoted line.
    Such lines in Reddit data begin with "&gt;".
    """
    end = span[1]
    relevant_text = text[:end]
    return True if re.search('&gt;[^\n]+$', relevant_text) else False # tests if there is no linebreak between a quote symbol and the match


def extract(args, comment: dict, outfile: TextIO):
    """
    Extract a comment text and all relevant metadata.
    If no regex is supplied, extract the whole comment leaving the span field blank.
    If a regex is supplied, extract each match separately with its span info.
    Discard regex matches found inside of a quoted line.
    """
    
    if args.return_all:
        comment = json.dumps(comment)
        _=outfile.write(comment+'\n')
    
    else:
        text = comment['body']
        user = comment['author']
        flairtext = comment['author_flair_text']
        subreddit = comment['subreddit']
        score = comment['score']
        date = comment['created_utc']
        
        # assemble a standard Reddit URL for older data
        url_base = "https://www.reddit.com/r/"+subreddit+"/comments/"
        oldschool_link = url_base + comment['link_id'].split("_")[1] + "//" + comment['id']

        # choose the newer "permalink" metadata instead if available
        permalink = "https://www.reddit.com" + comment['permalink'] if 'permalink' in comment.keys() else oldschool_link

        csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        span = None
        row = [text, span, subreddit, score, user, flairtext, date, permalink]
        csvwriter.writerow(row)


def filter(comment: dict, popularity_threshold: int) -> tuple:
    """
    Test if a Reddit comment breaks any of the filtering rules.
    This is for nuanced criteria so positives are kept for manual review.
    """
    if popularity_threshold is not None:
        if comment['score'] < popularity_threshold:
            return True, "score below defined threshold"
    
    text = comment['body']
    #if nlp(text)._.is_profane:
    #    return True, "offensive language"

    if "i'm a bot" in text.lower():
        return True, "non-human generated"
    
    return False, None


def relevant(comment: dict, args: argparse.Namespace, subs, baseline_nr) -> bool:
    """
    Test if a Reddit comment is at all relevant to the search.
    This is for broad criteria so negatives are discarded.
    The filters are ordered by how unlikely they are to pass for efficiency.
    """

    if comment['subreddit'] not in subs and baseline_nr == 1: 
        return False
    elif comment['subreddit'] in subs and baseline_nr == 2:
        return False
    
    filtered, _ = filter(comment, args.popularity) if not args.dont_filter else False, None
    filtered = filtered[0]
    if filtered:
        return False

    if comment['author_flair_text'] is None:
        pass
    else:
        search = re.search(combined_negative_regex, comment['author_flair_text']) if args.case_sensitive else re.search(combined_negative_regex, comment['author_flair_text'], re.IGNORECASE)
        if search:
            return False
    
    return True if comment['author'] not in userlist else False
         


def write_csv_headers(outfile_path: str):
    """Write the headers for both the results file and the file for filtered-out hits."""
    with open(outfile_path, 'a', encoding='utf-8') as outf:
        headers = ['text', 'span', 'subreddit', 'score', 'user', 'flairtext', 'date', 'permalink']
        csvwriter = csv.writer(outf, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(headers)


def read_redditfile(file: str):
    """
    Iterate over the pushshift JSON lines, yielding them as Python dicts.
    Decompress iteratively if necessary.
    """
    # older files in the dataset are uncompressed while newer ones use zstd compression and have .xz, .bz2, or .zst endings
    if not file.endswith('.bz2') and not file.endswith('.xz') and not file.endswith('.zst'):
        with open(file, 'r', encoding='utf-8') as infile:
            for line in infile:
                l = json.loads(line)
                yield(l)
    else:
        for comment, some_int in read_lines_zst(file):
            yield json.loads(comment)

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		logging.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close() 


def within_timeframe(month: str, time_from: tuple, time_to: tuple) -> bool:
    """Test if a given month from the Pushshift Corpus is within the user's provided timeframe."""
    # a month's directory name has the format "RC YYYY-MM"
    month = re.sub('\.\w+$', '', month) # remove file ending
    y = int(month.split("_")[1].split("-")[0])
    m = int(month.split("-")[1])

    if time_from is not None:
        from_year, from_month = time_from[0], time_from[1]

        if y < from_year:
            return False
        if y == from_year and m < from_month:
            return False
    
    if time_to is not None:
        to_year, to_month= time_to[0], time_to[1]

        if y > to_year:
            return False
        if y == to_year and m > to_month:
            return False

    return True


def fetch_data_timeframe(input_dir: str) -> tuple:
    """
    Establish a timeframe based on all directories found in the input directory.
    Used when no timeframe was given by user.
    """
    months = [elem.replace("RC_", "") for elem in os.listdir(input_dir) if not elem.endswith(".txt")]
    months = [elem.replace("RS_", "") for elem in months]
    months = [elem.replace(".zst", "") for elem in months if elem.endswith('.zst')]

    months = sorted(months)
    months = [(int(elem.split("-")[0]), int(elem.split("-")[1])) for elem in months]
    return months[0], months[-1]


def establish_timeframe(time_from: tuple, time_to: tuple, input_dir: str) -> list:
    """Return all months of the data within a timeframe as list of directories."""
    months = [elem for elem in os.listdir(input_dir) if elem.startswith("RC") or elem.startswith("RS")] # all available months in the input directory

    return sorted([month for month in months if within_timeframe(month, time_from, time_to)], reverse=False)


def valid_date(string) -> tuple:
    """
    Check if a given date follows the required formatting and is valid.
    Returns a (year, month) tuple.
    Used as ArgParser type.
    """
    if re.search('^20[012]\d\-0?\d[012]?$', string):
        year, month = int(string.split("-")[0]), int(string.split("-")[1])
        if month > 12 or month < 1:
            msg = f"not a valid month: {month}"
            raise argparse.ArgumentTypeError(msg)
        else:
            return (year, month)
    else:
        msg = f"not a valid date: {string}"
        raise argparse.ArgumentTypeError(msg)


def dir_path(string) -> str:
    """
    Test if a given path exists on the machine.
    Used as ArgParser type.
    """
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)


def sample_float(num) -> float:
    try:
        num = float(num)
    except:
        raise TypeError(f"{num} is not a recognized number format.")
    
    if num > 1.0 or num < 0:
        raise TypeError("Sample size must be given as number between 0.0 and 1.0")
    
    return num


def comment_regex(string) -> str:
    """
    Some modifications for supplied regexes.
    Currently just to allow for quoted blocks to come at the beginning if the supplied regex asks for regex matches at the beginning of comments via ^
    """
    
    if os.path.isfile(string):
        regex = open(string, "r", encoding="utf-8").read()
    else:
        regex = string

    initial_regex_tester = "^((?:\(\?<[=!].*?\)))?(\^)" # to check if expression has ^ at beginning, while also allow for look-behind statements that can contain ^

    if re.search(initial_regex_tester, regex):
        flag = re.search(f'{initial_regex_tester}(.+$)', regex).group(1) # in case there is a flag of the type (?i) at the start
        flag = '' if flag is None else flag

        expr = re.search(f'{initial_regex_tester}(.+$)', regex).group(3)
        
        regex = flag+ '^' + r'(>.+\n\n)*' + expr
        logging.info(f"Regex changed to {regex}")

    return regex


def assemble_outfile_name(args: argparse.Namespace, month) -> str:
    """
    Assemble the outfile name out of the search parameters in human-readable and sanitized form.
    Full path is returned.
    """
    outfile_name = f"baseline-{args.baseline_nr}_reservoir-sampled_based-on_pronoun-declarers_from-month_"

    # add timeframe info
    # this allows for the name to make sense with any or both of the timeframe bounds absent or present
    if month is not None:
        outfile_name += "_from_" + month
    else:
        if args.time_from is not None:
            outfile_name += "_from_" + str(args.time_from[0]) + '-' + str(args.time_from[1])
        if args.time_to is not None:
            outfile_name += "_up_to_" + str(args.time_to[0]) + '-' + str(args.time_to[1])
    # add time of search
    outfile_name += "_executed-at_" + datetime.now().strftime('%Y-%m-%d_at_%Hh-%Mm-%Ss')
    # specify the month of the reddit data
    outfile_name = outfile_name + "_" + month if month is not None else outfile_name
    # add file ending
    outfile_name += ".csv" if not args.return_all else ".jsonl"

    return outfile_name


def define_parser() -> argparse.ArgumentParser:
    """Define console argument parser."""
    parser = argparse.ArgumentParser(description="Keyword search comments from the Pushshift data dumps")

    # directories
    parser.add_argument('--input', '-I', type=dir_path, required=True,
                        help="The directory containing the input data, ie. the Pushshift data dumps.")
    parser.add_argument('--output', '-O', type=dir_path, required=False,
                        help="The directory where search results will be saved to.")
    
    # timeframe
    parser.add_argument('--time_from', '-F', type=valid_date, required=False,
                        help="The beginning of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no lower bound.")
    parser.add_argument('--time_to', '-T', type=valid_date, required=False,
                        help="The end of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no upper bound.")
    
    # search parameters
    parser.add_argument('--commentregex', '-CR', type=comment_regex, required=False,
                        help="The regex to search the comments with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--flairregex', '-FR', type=comment_regex, required=False,
                        help="The regex to search the comment flairs with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--case-sensitive', '-CS', action='store_true',
                        help="Makes search case-sensitive if any regex (comment or flair) was supplied.")
    parser.add_argument('--popularity', '-P', type=int, required=False,
                        help="Popularity threshold: Filters out comments with a score lower than the given value.")
    parser.add_argument('--toplevel', '-TL', action='store_true', required=False,
                        help="Only consider top-level comments, ie. comments not posted as a reply to another comment, but directly to a post.")
    parser.add_argument('--language', '-L', required=False,
                        help="Language to be used for spacy search.")
    
    # special
    parser.add_argument('--count', '-C', action='store_true',
                        help="Only counts the relevant comments per month and prints the statistic to console.")
    parser.add_argument('--include_quoted', action='store_true',
                        help="Include regex matches that are inside Reddit quotes (lines starting with >, often but not exclusively used to quote other Reddit users)")
    parser.add_argument('--sample', '-SMP', type=sample_float, required=False,
                        help="Retrieve a sample of results fitting the other parameters. Sample size is given as float between 0.0 and 1.0 where 1.0 returns 100% of results")
    parser.add_argument('--return_all', action='store_true', required=False,
                        help="Will return every search hit in its original and complete JSON form.")
    parser.add_argument('--dont_filter', action='store_true', required=False,
                        help="Skip any filtering.")
    parser.add_argument('--baseline_nr', type=int, required=True,
                        help="What kind of baseline is wanted, 1 or 2.")

    return parser


def handle_args() -> argparse.Namespace:
    """Handle argument-related edge cases by throwing meaningful errors."""
    parser = define_parser()
    args = parser.parse_args()

    if args.output is None and not args.count:
        parser.error("Since you're not just counting, you need to supply an output directory.")

    # ensure that the timeframe makes sense (either the from-year is later than to-year, or the from-month is later than to-month in the same year)
    # only necessary if both endpoints are given
    if args.time_from is not None and args.time_to is not None:
        if args.time_from[0] > args.time_to[0] or (args.time_from[0] == args.time_to[0] and args.time_from[1] > args.time_to[1]):
            parser.error("argument --time_from is later than --time_to")
    # if no timeframe is given, all available months are searched
    elif args.time_from is None and args.time_to is None:
        logging.info("No timeframe supplied. Searching all months found in the input directory.")
        args.time_from, args.time_to = fetch_data_timeframe(args.input)
    
    if args.baseline_nr != 1 and args.baseline_nr != 2:
         parser.error("Baseline Nr is required and must be either 1 or 2.")

    return args


def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
    m_num = int(month.split("-")[1]) # get month integer
    m_name = calendar.month_name[m_num]

    logging.info("Processing " + m_name + " " + year)

def parse_month(month: str):
    "get year and month as integers from filename"
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = int(month.split("-")[0]) # get year string from the format 'RC_YYYY-MM.zst'
    month = int(month.split("-")[1]) # get month integer
    return month, year    

def get_data_file(path: str) -> str:
    """
    Find the correct file type of each month directory.
    Files can be plain, zst, xz, or bz2.
    Throw error if no usable file is present in directory.
    """
    for ending in ['', '.zst', '.xz', '.bz2']:
        if os.path.isfile(path+ending):
            return path+ending
    logging.warning("Month directory " + dir + " does not contain a valid data dump file.")
    exit()


def process_month(month, args, outfile):
    log_month(month)

    infile = args.input + "/" + month

    month, year = parse_month(month)
    

    if not args.count:
        if args.baseline_nr == 1:
            monthly_results, reservoir = reset_reservoir_and_results()
            for sub in subs:
                reservoir[sub]['K'] = generate_k(sub, year, month, declarers)
            month_subs = [sub for sub in subs if reservoir[sub]['K'] > 0]
        
        elif args.baseline_nr == 2:
            monthly_results = list()
            k = ((declarers['year'] == year) & (declarers['month'] == month)).sum()
            k = 2*k # to have more than enough
            n = 0
            month_subs = subs

        for comment in read_redditfile(infile):

            if args.baseline_nr == 1:
                if month_subs == []:
                    break
            elif args.baseline_nr == 2:
                if k == 0:
                    break     

            if relevant(comment, args, month_subs, args.baseline_nr):
                if args.baseline_nr == 1:
                    sub = comment['subreddit']
                    k = reservoir[sub]['K']
                    reservoir[sub]['N'] += 1
                    n = reservoir[sub]['N']

                    if len(monthly_results[sub]) < k:
                        monthly_results[sub].append(comment)
                    else:
                        s = int(random.random() * n)
                        if s < k:
                            monthly_results[sub][s] = comment
                
                elif args.baseline_nr == 2:
                    if len(monthly_results) < k:
                        monthly_results.append(comment)
                    else:
                        n += 1
                        s = int(random.random() * n)
                        if s < k:
                            monthly_results[s] = comment
    
    elif args.count:
        monthly_counts = {sub: 0 for sub in subs}

        for comment in read_redditfile(infile):
            if relevant(comment, args, subs):
                 sub = comment['subreddit']
                 monthly_counts[sub] += 1

    if args.count:
         outfile = re.sub('\.csv', '', outfile)
         outfile = f'{outfile}.jsonl'

    with open(outfile, "a", encoding="utf-8") as outf:
        if not args.count:
            if args.baseline_nr == 1:
                for sub in list(monthly_results.keys()):
                    for comment in monthly_results[sub]:
                        extract(args, comment, outf)
            elif args.baseline_nr == 2:
                for comment in monthly_results:
                    extract(args, comment, outf) 
        
        elif args.count:
             data = json.dumps(monthly_counts)
             _=outf.write(data)


def fetch_model(lang):
    if lang.lower() == "german" or lang.lower() == "deutsch":
        return 'de_dep_news_trf'
    else:
        logging.info("Only German spacy models are currently installed.")
        exit()


def main():
    logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')
    args = handle_args()
    timeframe = establish_timeframe(args.time_from, args.time_to, args.input)
    logging.info(f"Establishing baseline for each month from {timeframe[0]} to {timeframe[-1]}")

    # Writing the CSV headers
    for month in timeframe:
        outfile = assemble_outfile_name(args, month)
        outfile = os.path.join(args.output, outfile)
        if not args.return_all:
            write_csv_headers(outfile)
        process_month(month, args, outfile)
    


if __name__ == "__main__":
    main()