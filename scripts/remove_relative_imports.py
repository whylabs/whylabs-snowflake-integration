import ast
import astunparse
import sys

def remove_relative_imports(filename) -> None:
    # Read the source file.
    with open(filename, "r") as source:
        tree = ast.parse(source.read(), filename)

    # Function to check if the current node is a relative import.
    def is_relative_import(node) -> bool:
        return isinstance(node, ast.ImportFrom) and node.level != 0

    def is_comment(node) -> bool:
        return isinstance(node, ast.Expr) and isinstance(node.value, ast.Str)

    # Remove the relative import statement nodes.
    new_tree_body = [node for node in tree.body if not is_relative_import(node) and not is_comment(node)]
    tree.body = new_tree_body

    # Write the modified tree back to the source file.
    with open(filename, "w") as source:
        source.write(astunparse.unparse(tree))

def main() -> None:
    # Check if the script received the right number of arguments.
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} filename")
        sys.exit(1)

    # The list of command line arguments passed to a Python script. argv[0] is the script name.
    file_path = sys.argv[1]

    try:
        remove_relative_imports(file_path)
        print(f"Removed all relative imports from '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
