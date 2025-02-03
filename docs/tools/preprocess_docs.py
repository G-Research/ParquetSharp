import os
import re

def process_markdown_file(filepath):
    '''
    Preprocesses a markdown file by replacing inline code blocks with a special token,
    for namespaces starting with "ParquetSharp" or "System".

    Args:
        filepath (str): The path to the markdown file.
    '''
    with open(filepath, 'r', encoding='utf-8') as file:
        content = file.read()

    def replace_namespace(match):
        code = match.group(1)
        if code.startswith("ParquetSharp") or code.startswith("System"):
            return f"@{code}"
        return f"`{code}`"

    processed_content = re.sub(r'(?<!`)`([^`\n]+)`(?!`)', replace_namespace, content)

    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(processed_content)
    print(f"Processed {filepath}")

def process_docs_folder(folder_path):
    '''
    Iterates through all markdown files in the specified folder and applies preprocessing.

    Args:
        folder_path (str): Path to the folder containing markdown files.
    '''
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.md'):
                process_markdown_file(os.path.join(root, file))

if __name__ == '__main__':
    docs_folder = 'docs'
    print("Preprocessing markdown files...")
    process_docs_folder(docs_folder)
    print("Preprocessing completed successfully.")
