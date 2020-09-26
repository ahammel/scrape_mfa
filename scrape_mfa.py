#!/usr/bin/env python3
import argparse
import csv
import itertools
import json.decoder
import os.path
import sys
import time
import traceback

import requests

BASE_URL = "https://api.pushshift.io"

START_TIME = 1494028860
# ^ May 06, 2017 (the day after the Simple Questions rule was implemented)

OUTPUT_FILE = "./scrape_mfa_results.tsv"
DEDUPLICATED_FILE = "./scrape_mfa_results.deduplicted.tsv"
COLUMNS_FILE = "./columns.tsv"

PARSER = argparse.ArgumentParser()
PARSER.add_argument(
    "-o",
    "--output-file",
    default=OUTPUT_FILE,
    help="File where comment data is stored while fetching is in progress",
)
PARSER.add_argument(
    "-d",
    "--deduplicated-file",
    default=DEDUPLICATED_FILE,
    help="File where final, deduplicated comment data is stored",
)
PARSER.add_argument(
    "-c",
    "--columns-file",
    default=COLUMNS_FILE,
    help="File where the column definitions are stored",
)

COMMENT_FIELDS = [
    "thread_id",
    "thread_created_utc",
    "id",
    "created_utc",
    "author",
    "parent_id",
    "body",
    "all_awardings",
    "approved_at_utc",
    "associated_award",
    "author_flair_background_color",
    "author_flair_css_class",
    "author_flair_richtext",
    "author_flair_template_id",
    "author_flair_text",
    "author_flair_text_color",
    "author_flair_type",
    "author_fullname",
    "author_patreon_flair",
    "author_premium",
    "awarders",
    "banned_at_utc",
    "can_mod_post",
    "collapsed",
    "collapsed_because_crowd_control",
    "collapsed_reason",
    "comment_type",
    "distinguished",
    "edited",
    "gildings",
    "is_submitter",
    "link_id",
    "locked",
    "no_follow",
    "permalink",
    "retrieved_on",
    "score",
    "send_replies",
    "stickied",
    "subreddit",
    "subreddit_id",
    "top_awarded_type",
    "total_awards_received",
    "treatment_tags",
]


def initialize_output_file():
    if not os.path.exists(OUTPUT_FILE):
        open(OUTPUT_FILE, "w+")


def initial_page_boundary():
    try:
        with open(OUTPUT_FILE, "r") as handle:
            reader = csv.DictReader(
                handle, fieldnames=COMMENT_FIELDS, dialect="excel-tab"
            )
            latest_page = max(
                itertools.chain(
                    (START_TIME,),
                    (int(row["thread_created_utc"]) for row in reader),
                )
            )
            return latest_page - 1
            # ^ fetch the latest page again as it might not have been
            # completely scraped if the previous run crashed halfyway through a
            # thread
    except FileNotFoundError as err:
        return START_TIME


def fetch_dq_thread_ids(
    search_term,
    after,
    page_size=100,
):
    # Replacing all this gnarly imperative code with a paginator object is left
    # as an exercise to the reader
    pages_fetched = 0

    while True:
        page = fetch_dq_thread_page(
            search_term=search_term, page_size=page_size, after=after
        )

        # If there are no threads on the page, we're done
        if len(page) == 0:
            print(f"No {search_term} threads found after time {after}")
            break

        print(
            f"Fetched {len(page)} {search_term} thread IDs starting at time"
            f" {after}"
        )

        for thread in page:
            yield thread
            after = thread["thread_created_utc"]


def fetch_dq_thread_page(search_term, after, page_size=100):
    params = {
        "subreddit": "malefashionadvice",
        "author": "AutoModerator",
        "title": search_term,
        "fields": ["id", "title", "created_utc"],
        "num_comments": ">0",
        "sort": "asc",
        "sort_type": "created_utc",
        "size": page_size,
        "after": after,
    }
    json = fetch_json(
        f"{BASE_URL}/reddit/search/submission",
        params=params,
    )
    return [
        {
            "thread_id": row["id"],
            "thread_created_utc": row["created_utc"],
            "thread_title": row["title"],
        }
        for row in json["data"]
    ]


def fetch_comment_ids(threads):
    for thread in threads:
        thread_id = thread["thread_id"]
        json = fetch_json(
            f"{BASE_URL}/reddit/submission/comment_ids/{thread_id}"
        )
        data = json["data"]
        print(
            f"Fetched {len(data)} comment IDs for thread"
            f" '{thread['thread_title']}' ({thread_id})"
            f" submitted at time {thread['thread_created_utc']}"
        )
        for comment_id in data:
            yield {
                "id": comment_id,
                **thread,
            }


def fetch_comments(comments):
    # Restructure comments list as a dict in order to loo up the thread info
    # later
    comment_dict = {comment["id"]: comment for comment in comments}
    json = fetch_json(
        f"{BASE_URL}/reddit/search/comment",
        params={
            "ids": ",".join(comment_dict.keys()),
            "fields": COMMENT_FIELDS,
        },
    )
    data = json["data"]
    print(f"Fetched {len(data)} comments")
    for comment in data:
        thread_info = comment_dict[comment["id"]]
        yield {
            **correct_parent_id(comment),
            **thread_info,
        }


def correct_parent_id(comment):
    # The 'parent_id' field of a comment is prefixed with 't1_' if the parent
    # entity is another comment, or 't3_' if the parent is a link or a
    # submission[1].
    #
    # For my purposes (e.g., determining whether a comment is top level or a
    # reply) this is inconvenient. If the prefix is 't1_', I strip the prefix
    # so that the 'parent_id' properly foreign keys to the parent comment. If
    # the prefix is 't3_' I replace the parent id with None.
    #
    # [1] I'm pretty sure. This behaviour isn't documented, but that's my
    # interpretation of this stanza:
    # https://github.com/pushshift/api/blob/ded75fadbc4bf4a3ea4b5cf4518b5bd4e2d7ca1e/api/Comment.py#L39-L45
    if comment["parent_id"].startswith("t1_"):
        comment["parent_id"] = comment["parent_id"][3:]
    elif comment["parent_id"].startswith("t3_"):
        comment["parent_id"] = None
    return comment


def csvify_comments(comments, handle):
    writer = csv.DictWriter(
        handle,
        fieldnames=COMMENT_FIELDS,
        dialect="excel-tab",
        extrasaction="ignore",
    )
    for comment in comments:
        writer.writerow(comment)


def deduplicate(in_handle, out_handle):
    reader = csv.DictReader(
        in_handle,
        fieldnames=COMMENT_FIELDS,
        dialect="excel-tab",
    )
    writer = csv.DictWriter(
        out_handle,
        fieldnames=COMMENT_FIELDS,
        dialect="excel-tab",
        extrasaction="ignore",
    )
    comment_ids = set()
    for row in reader:
        if row["id"] not in comment_ids:
            writer.writerow(row)
            comment_ids.add(row["id"])
    return comment_ids


def fetch_json(url, params = None):
    r = requests.get(url, params)
    if r.status_code == 200:
        try:
            return r.json()
        except json.decoder.JSONDecodeError:
            pass
    raise Exception(
        f"Failed to fetch from {url} with params {params}"
        f"\n\nStatus code: {r.status_code}"
        f"\nBody:\n{r.text}"
    )


def chunk(size, iterable):
    """Divide an iterable into lists of a given size.

    >>> list(chunk(3, [1, "two", 3, "cat", None]))
    [[1, "two", 3], ["cat", None]]

    """

    class Sentinel(object):
        pass

    sentinel_value = Sentinel()
    # Use a private type as the sentinel value so that the iterable can contain
    # items of any type wihout bugs
    zipped = itertools.zip_longest(
        *[iter(iterable)] * size, fillvalue=sentinel_value
    )
    # wtf. This is stackoverflow copypasta (as if that isn't obvious)
    return ([x for x in chunk if x is not sentinel_value] for chunk in zipped)


def scrape_mfa(columns_file, deduplicated_file, output_file):
    with open(columns_file, "w+") as handle:
        csv.DictWriter(
            handle, fieldnames=COMMENT_FIELDS, dialect="excel-tab"
        ).writeheader()

    initialize_output_file()

    after = initial_page_boundary()
    print(f"Fetching data starting at time {after}")

    dq_thread_ids = itertools.chain(
        fetch_dq_thread_ids(search_term="Simple Questions", after=after),
        fetch_dq_thread_ids(search_term="Daily Questions", after=after),
    )

    comment_ids = fetch_comment_ids(dq_thread_ids)

    comments = (
        comment
        for comment_page in chunk(500, comment_ids)
        for comment in fetch_comments(comment_page)
    )

    with open(output_file, "a+") as handle:
        csvify_comments(comments, handle)

    print("Deduplicating results")
    with open(output_file, "r") as infile, open(
        deduplicated_file, "w+"
    ) as outfile:
        comment_ids = deduplicate(infile, outfile)
    print(f"Wrote {len(comment_ids)} comments to {DEDUPLICATED_FILE}")


def main(columns_file, output_file, deduplicated_file):
    done = False
    sleeps =  [80, 60, 30, 20, 10, 10, 0]
    while True:
        try:
            scrape_mfa(
                columns_file=columns_file,
                deduplicated_file=deduplicated_file,
                output_file=output_file,
            )
        except Exception:
            traceback.print_exc()
            if len(sleeps) == 0:
                break
            sleep = sleeps.pop()
            print(f"Restarting after {sleep} seconds")
            time.sleep(sleep)
        else:
            done = True
            break
    if not done:
        print("Too may restarts. Aborting")
        sys.exit(1)
    print("Process complete")


if __name__ == "__main__":
    args = PARSER.parse_args()
    main(**vars(args))
