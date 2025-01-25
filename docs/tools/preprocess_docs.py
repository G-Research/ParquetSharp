import os
import re

def process_markdown_file(filepath):
    '''
    Preprocesses a markdown file by replacing inline code blocks with a special token.

    This allows DocFX to link to the correct API reference page when the code block is a type name.

    Args:
        filepath (str): The path to the markdown file.
    '''
    with open(filepath, 'r', encoding='utf-8') as file:
        content = file.read()

    processed_content = re.sub(r'(?<!`)`([^`\n]+)`(?!`)', r'@\1', content)

    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(processed_content)
    print(f"Processed {filepath}")

def process_docs_folder(folder_path):
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.md'):
                process_markdown_file(os.path.join(root, file))

if __name__ == '__main__':
    docs_folder = 'docs'
    print("Preprocessing markdown files...")
    process_docs_folder(docs_folder)
    print("Preprocessing completed successfully.")
