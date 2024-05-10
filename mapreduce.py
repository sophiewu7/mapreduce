import os
import sys
import argparse
from collections import defaultdict
import re
import string

def clean_word(word):
    """Remove punctuation from the start and end of the word and convert to lower case."""
    # Find the position of the first and last Latin letter
    first_latin = re.search(r'[a-zA-Z]', word)
    last_latin = re.search(r'[a-zA-Z](?=[^a-zA-Z]*$)', word)

    if first_latin and last_latin:
        # Extract the substring from the first to last Latin letter inclusive
        start_index = first_latin.start()
        end_index = last_latin.start()
        return word[start_index:end_index + 1].lower()
    else:
        # Return the original word if no Latin letters are found
        return word.lower()

def is_valid_latin_word(word):
    """Check if the word contains only Latin letters."""
    return re.fullmatch(r'[a-zA-Z]+', word) is not None

def process_file(file_path):
    """Process each file to extract words, cleaning each and checking validity."""
    words = defaultdict(int)
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line_words = line.split(" ")
            for word in line_words:
                cleaned_word = clean_word(word)
                if is_valid_latin_word(cleaned_word):
                    words[cleaned_word.lower()] += 1
                    print(cleaned_word)
    return words

def reduce_words(all_words):
    """Reduce the word counts from all files into a single dictionary."""
    final_counts = defaultdict(int)
    for word_dict in all_words:
        for word, count in word_dict.items():
            final_counts[word] += count
    return final_counts

def main(input_dir, output_dir):
    all_words = []
    for file_name in os.listdir(input_dir):
        if file_name.endswith(".txt"):
            file_path = os.path.join(input_dir, file_name)
            words = process_file(file_path)
            all_words.append(words)

    final_counts = reduce_words(all_words)
    sorted_words = sorted(final_counts.items(), key=lambda x: (-x[1], x[0]))

    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, 'output.txt')

    with open(output_file_path, 'w', encoding='utf-8') as file:
        for word, count in sorted_words:
            file.write(f"{word},{count}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Word Count Script")
    parser.add_argument('--input', help="Input directory containing text files", required=True)
    parser.add_argument('--output', help="Output directory to save results", required=True)

    args = parser.parse_args()
    main(args.input, args.output)
