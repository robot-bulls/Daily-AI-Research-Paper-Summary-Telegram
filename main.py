import re
import math
import json

import backoff
import requests
import feedparser
import concurrent.futures

from io import BytesIO
from datetime import datetime, timedelta
from tqdm import tqdm
from pdfminer.high_level import extract_text

import openai
from llama_index.llms import OpenAI
from langchain.text_splitter import RecursiveCharacterTextSplitter


def get_papers_from_arxiv():
    """
    Retrieve papers from arXiv's Computer Science category submitted the previous day.
    """

    base_url = 'https://export.arxiv.org/api/query?'
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    query_encoded = f'cat:cs.* AND submittedDate:[{yesterday}0000 TO {yesterday}2359]'
    url = f'{base_url}search_query={requests.utils.quote(query_encoded)}&start=0&max_results=81'

    print("Adding papers.")
    response = requests.get(url)
    response.raise_for_status()

    feed = feedparser.parse(response.text)
    papers_list = []

    for idx, entry in enumerate(feed.entries, start=1):
        pdf_url = next((link.href for link in entry.links if link.type == 'application/pdf'), None)

        if pdf_url:
            try:
                papers_list.append((entry.link, entry.authors, f"{idx}. {entry.title}", entry.summary, pdf_url))
            except Exception as e:
                print(f"Error processing PDF for {idx}. {entry.title}. Error: {e}")

    print("Finished adding papers.")
    return papers_list


def extract_text_from_pdf(pdf_url):
    """
    Extracts and returns text content from a PDF located at the provided URL.
    """
    response = requests.get(pdf_url)
    response.raise_for_status()
    pdf_data = BytesIO(response.content)
    
    return extract_text(pdf_data)


@backoff.on_exception(backoff.expo,
                      (openai.error.RateLimitError, concurrent.futures.TimeoutError),
                      max_tries=4)
def complete_with_retry(llm, text, timeout=120):
    """
    Send a request to llm.complete with retry, delay, and timeout.
    """
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(llm.complete, text)
            return future.result(timeout=timeout)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise  # re-raise the exception


def concatenate_titles_and_abstracts(titles_and_abstracts, minimum_groups=4):
    """
    Concatenate titles and abstracts into groups.
    """
    grouped_strings = []
    
    total_titles = len(titles_and_abstracts)
    
    # Calculate how many complete groups of size 20 can be made
    groups_of_20 = total_titles // 20
    
    # Calculate the number of titles left after making those groups
    remaining_titles = total_titles % 20

    # Calculate additional groups required for the remaining titles
    additional_groups = (remaining_titles + 19) // 20  # This ensures we don't exceed 20 in a group
    
    # The total minimum groups is the sum of groups of size 20 and additional groups
    minimum_groups = max(minimum_groups, groups_of_20 + additional_groups)
    
    # Determine the size of each group and the number of titles left over
    group_size = total_titles // minimum_groups
    leftover = total_titles % minimum_groups
    
    current_group = 1
    concatenated_string = ""

    for i, (entry_link, authors, title, abstract, pdf_url) in enumerate(titles_and_abstracts, 1):
        concatenated_string += f"{title}: {abstract}; "
        
        # Update the group_size for the first `leftover` groups
        current_group_size = group_size + (1 if current_group <= leftover else 0)
        
        # If the current index equals the current group size,
        # append the concatenated string to the list and reset the string.
        # Also update the current_group
        if i % current_group_size == 0:
            grouped_strings.append(concatenated_string)
            concatenated_string = ""
            current_group += 1
            
    return grouped_strings


def generate_summary(text):
    """Generates a summary for the given text by iteratively summarizing and refining it."""
    llm = OpenAI(temperature=0.7, model="gpt-4")

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=2500,
        chunk_overlap=0,
        length_function=len,
        is_separator_regex=False,
    )

    docs = text_splitter.create_documents([text])
    docs = [doc.page_content for doc in docs]
    num_splits = int(math.log2(len(docs)))

    for _ in tqdm(range(num_splits)):
        if len(docs) % 2 == 0:
            docs = [docs[i] + docs[i + 1] for i in range(0, len(docs) - 1, 2)]
        else:
            last_doc = docs[-1]
            docs = [docs[i] + docs[i + 1] for i in range(0, len(docs) - 1, 2)]
            docs[-1] = docs[-1] + last_doc

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_index = {executor.submit(
            lambda i, doc: (i, complete_with_retry(llm, "Concisely and simply explain what this text is about: " + doc).text),
            i, doc): i for i, doc in enumerate(docs)}
    
            for future in concurrent.futures.as_completed(future_to_index):
                i, result = future.result()
                docs[i] = result

    return docs[0]


def reduce_selection(llm, titles_and_abstracts):
    """Refines the selection of titles and abstracts until there are 3 or fewer papers."""

    prompt = "Give me the top 3, in your opinion, most interesting papers. Rank your choices. Do not change the given indexes."
    previous_titles_and_abstracts = []  # Store the previous iteration's titles and abstracts

    print("Selecting the top 3 papers.")
    while len(titles_and_abstracts) > 3:
        print(f"Current number of titles: {len(titles_and_abstracts)}")
        previous_titles_and_abstracts = titles_and_abstracts.copy()  # Update the previous iteration's titles and abstracts
        contexts = concatenate_titles_and_abstracts(titles_and_abstracts)
        all_chosen_indices = set()

        def process_context(context):
            response = complete_with_retry(llm, context + prompt).text
            max_index = len(titles_and_abstracts)
            new_indices = [int(match) for match in re.findall(r'\b\d+\b', response) if int(match) <= max_index][1::2]
            return new_indices

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_context, context) for context in contexts]
            for future in concurrent.futures.as_completed(futures):
                new_indices = future.result()
                all_chosen_indices.update(new_indices)

        titles_and_abstracts = [titles_and_abstracts[idx-1] for idx in all_chosen_indices]

    # If the final set of papers is less than 3, fill in the remaining spots
    # with papers from the last iteration's titles and abstracts.
    if len(titles_and_abstracts) < 3:
        remaining_papers_needed = 3 - len(titles_and_abstracts)
        # Get the remaining papers from the previous iteration's titles and abstracts
        # assuming that the previous iteration had more than 3 papers.
        if len(previous_titles_and_abstracts) > 3:
            additional_papers = previous_titles_and_abstracts[:remaining_papers_needed]
            titles_and_abstracts.extend(additional_papers)
        else:
            print("Not enough papers to fulfill the 3-paper requirement.")

    print("Finished selecting the top 3 papers.")
    return titles_and_abstracts


def create_paper_strings(papers):
    """Generate formatted strings for a list of papers."""
    paper_strings = []

    for paper in papers:
        entry_link, authors, title, abstract, pdf_url = paper
        print(f"Now summarizing: {title}")

        author_names = [a['name'] for a in authors]
        authors_str = ', '.join(author_names)

        full_text = extract_text_from_pdf(pdf_url)

        summary = generate_summary(full_text)
        print(f"Summary complete: {summary}")

        paper_string = f"Link: {entry_link}\n\nAuthors: {authors_str}\n\nTitle: {title}\n\nSummary: {summary}"
        paper_strings.append(paper_string)

    return paper_strings

with open('config.json', 'r') as file:
    params = json.load(file)

openai.api_key = params['api_key']
llm = OpenAI(temperature=0, model="gpt-4")

titles_and_abstracts = get_papers_from_arxiv()

chosen_titles_and_abstracts = reduce_selection(llm, titles_and_abstracts)
chosen_titles_and_abstracts = [(entry_link, authors, re.sub(r'\n', '', re.sub(r'\d+\.', '', title)), abstract, pdf_url) 
                               for entry_link, authors, title, abstract, pdf_url in chosen_titles_and_abstracts]

paper_strings = create_paper_strings(chosen_titles_and_abstracts)

token = params['token']
channel_id = params['channel_id']

print("Sending summaries to Telegram.")
for paper_string in paper_strings:

    url = f'https://api.telegram.org/bot{token}/sendMessage'
    params = {
        'chat_id': channel_id,
        'text': paper_string,
        'disable_web_page_preview': True,
    }

    requests.post(url, params=params)
