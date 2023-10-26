from typing import List, Set
import argparse
import os
import re


def get_local_imports(filepath: str) -> List[str]:
    local_imports: List[str] = []
    with open(filepath, "r") as f:
        lines = f.readlines()
        for line in lines:
            if re.match(r"^from \.|^import \.", line):
                local_imports.append(line.strip())
    return local_imports


def get_file_content(filepath: str) -> str:
    with open(filepath, "r") as f:
        return f.read()


def merge_recursive(filepath: str, base_dir: str, seen: Set[str]) -> str:
    if filepath in seen:
        return ""
    seen.add(filepath)

    merged_content: str = f"## \n## {filepath}\n##\n" + get_file_content(filepath) + "\n\n"
    local_imports: List[str] = get_local_imports(filepath)

    for local_import in local_imports:
        module_name_result = re.search(r"from \.(.+) import.*$", local_import)
        if module_name_result is None:
            raise ValueError(f"Could not parse module name from {local_import}")
        module_name: str = module_name_result.group(1)
        module_path: str = os.path.join(base_dir, f"{module_name}.py")

        module_content: str = merge_recursive(module_path, base_dir, seen)
        merged_content = module_content + "\n\n" + merged_content

    return merged_content


if __name__ == "__main__":
    """
    The output of this script is a giant Python file that contains all the code from the relative import graph
    of the --entry file. The relative imports do have to be removed from the giant file though, which is what
    the remove_relative_imports.py script does.
    """
    parser = argparse.ArgumentParser(description="Merge Python files into one")
    parser.add_argument("--entry", required=True, help="Entry Python file to scan")
    parser.add_argument("--output", required=True, help="Output Python file")

    args = parser.parse_args()
    entry_file: str = args.entry
    output_file: str = args.output

    base_dir: str = os.path.dirname(os.path.abspath(entry_file))
    seen: Set[str] = set()

    merged_content: str = merge_recursive(entry_file, base_dir, seen)

    with open(output_file, "w") as f:
        f.write(merged_content)

    print(f"Merged Python files into {output_file}.")
