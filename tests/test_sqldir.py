import pytest
from io import UnsupportedOperation

from sqldir import install_patch, remove_patch, cursor

@pytest.fixture
def sqldir_patched():
    """
    Fixture to install the SqlDir patch with an in-memory SQLite database.
    Ensures the patch is removed after the test.
    """
    install_patch(path=":memory:") 
    yield
    remove_patch()

def test_write_and_read(sqldir_patched):
    """
    Test writing to a file and reading the content back in text mode.
    """
    # Write to the file
    with open("test.txt", "w") as f:
        bytes_written = f.write("Hello, world!\n")
    assert bytes_written == len("Hello, world!\n")

    # Read back the content
    with open("test.txt", "r") as f:
        content = f.read()
    assert content == "Hello, world!\n"

def test_append(sqldir_patched):
    """
    Test appending data to an existing file in text mode.
    """
    # Write initial content
    with open("append.txt", "w") as f:
        f.write("Line 1\n")

    # Append new content
    with open("append.txt", "a") as f:
        bytes_written = f.write("Line 2\n")
    assert bytes_written == len("Line 2\n")

    # Read back the content
    with open("append.txt", "r") as f:
        content = f.read()
    assert content == "Line 1\nLine 2\n"

def test_read_non_existing_file(sqldir_patched):
    """
    Test reading from a non-existing file should return empty content.
    """
    with open("nonexistent.txt", "r") as f:
        content = f.read()
    assert content == ""

def test_iteration(sqldir_patched):
    """
    Test the iteration protocol (__iter__ and __next__) in text mode.
    """
    lines = ["First line\n", "Second line\n", "Third line\n"]
    with open("iterate.txt", "w") as f:
        for line in lines:
            f.write(line)

    with open("iterate.txt", "r") as f:
        for idx, line in enumerate(f):
            assert line == lines[idx]

def test_seek_and_tell(sqldir_patched):
    """
    Test seeking to different positions and telling the current position in text mode.
    """
    content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    with open("seek.txt", "w") as f:
        f.write(content)

    with open("seek.txt", "r") as f:
        f.seek(5)
        pos = f.tell()
        assert pos == 5
        data = f.read(5)
        assert data == "FGHIJ"
        pos = f.tell()
        assert pos == 10

def test_write_unsupported_mode(sqldir_patched):
    """
    Test that writing to a file not opened for writing raises an error.
    """
    with open("readonly.txt", "w") as f:
        f.write("Read-only content")

    with open("readonly.txt", "r") as f:
        with pytest.raises(UnsupportedOperation) as exc_info:
            f.write("Attempt to write")
    assert "sqldir:: file not open for writing" in str(exc_info.value)

def test_readline_and_readlines(sqldir_patched):
    """
    Test reading lines using readline and readlines in text mode.
    """
    lines = ["First line\n", "Second line\n", "Third line\n"]
    with open("readlines.txt", "w") as f:
        for line in lines:
            f.write(line)

    with open("readlines.txt", "r") as f:
        # Test readline
        first_line = f.readline()
        assert first_line == "First line\n"
        second_line = f.readline()
        assert second_line == "Second line\n"
        third_line = f.readline()
        assert third_line == "Third line\n"
        end = f.readline()
        assert end == ""

    with open("readlines.txt", "r") as f:
        # Test readlines
        all_lines = f.readlines()
        assert all_lines == lines

def test_context_manager(sqldir_patched):
    """
    Test that the context manager properly closes the file and commits changes.
    """
    # Write using context manager
    with open("context.txt", "w") as f:
        f.write("Context manager content\n")

    # Read back to verify
    with open("context.txt", "r") as f:
        content = f.read()
    assert content == "Context manager content\n"

def test_multiple_files(sqldir_patched):
    """
    Test handling multiple files independently in text mode.
    """
    files_content = {
        "file1.txt": "Content of file 1\n",
        "file2.txt": "Content of file 2\n",
        "file3.txt": "Content of file 3\n",
    }

    # Write to multiple files
    for filename, content in files_content.items():
        with open(filename, "w") as f:
            f.write(content)

    # Read back the content
    for filename, content in files_content.items():
        with open(filename, "r") as f:
            read_content = f.read()
        assert read_content == content

def test_overwrite_file(sqldir_patched):
    """
    Test overwriting an existing file in text mode.
    """
    with open("overwrite.txt", "w") as f:
        f.write("Original content\n")

    with open("overwrite.txt", "w") as f:
        f.write("Overwritten content\n")

    with open("overwrite.txt", "r") as f:
        content = f.read()
    assert content == "Overwritten content\n"

def test_binary_write_and_read(sqldir_patched):
    """
    Test writing and reading binary data.
    """
    binary_data = b"\x00\x01\x02\x03\x04\x05"
    with open("binary.bin", "wb") as f:
        bytes_written = f.write(binary_data)
    assert bytes_written == len(binary_data)

    with open("binary.bin", "rb") as f:
        read_data = f.read()
    assert read_data == binary_data

def test_read_after_close(sqldir_patched):
    """
    Test that operations after closing the file behave correctly.
    """
    f = open("close_test.txt", "w")
    f.write("Some data\n")
    f.close()

    with pytest.raises(ValueError):
        f.write("More data")

    with pytest.raises(ValueError):
        f.read()

def test_flush_method(sqldir_patched):
    """
    Test that the flush method does not raise errors.
    """
    with open("flush_test.txt", "w") as f:
        f.write("Data before flush\n")
        try:
            f.flush()
        except Exception as e:
            pytest.fail(f"flush() raised an exception: {e}")

def test_non_standard_mode(sqldir_patched):
    """
    Test opening a file with a mode that doesn't match SqlDir criteria.
    It should fallback to the unmodified open.
    """
    test_content = "Standard open content\n"
    with open("standard.txt", "w") as f:
        f.write(test_content)

    # Re-open with a non-standard mode (e.g., binary mode)
    with open("standard.txt", "rb") as f:
        content = f.read()
    assert content == test_content.encode("utf-8")

def test_unicode_handling(sqldir_patched):
    """
    Test that Unicode characters are handled correctly.
    """
    unicode_text = "こんにちは世界\n"  # "Hello World" in Japanese
    with open("unicode.txt", "w", encoding="utf-8") as f:
        f.write(unicode_text)

    with open("unicode.txt", "r", encoding="utf-8") as f:
        content = f.read()
    assert content == unicode_text

def test_read_partial_content(sqldir_patched):
    """
    Test reading partial content using the size parameter in text mode.
    """
    content = "Partial read test content\n"
    with open("partial.txt", "w") as f:
        f.write(content)

    with open("partial.txt", "r") as f:
        partial = f.read(7)
    assert partial == "Partial"

def test_readline_with_size(sqldir_patched):
    """
    Test readline with the size parameter in text mode.
    """
    content = "Line one\nLine two\nLine three\n"
    with open("readline_size.txt", "w") as f:
        f.write(content)

    with open("readline_size.txt", "r") as f:
        line = f.readline(5)
    assert line == "Line "

    with open("readline_size.txt", "r") as f:
        line = f.readline(100)
    assert line == "Line one\n"

def test_readlines_with_hint(sqldir_patched):
    """
    Test readlines with the hint parameter in text mode.
    """
    lines = ["Line 1\n", "Line 2\n", "Line 3\n", "Line 4\n"]
    with open("readlines_hint.txt", "w") as f:
        for line in lines:
            f.write(line)

    with open("readlines_hint.txt", "r") as f:
        read_lines = f.readlines(hint=12) #type:ignore
        assert read_lines == ["Line 1\n", "Line 2\n"]

def test_file_close_commits_changes(sqldir_patched):
    """
    Test that closing a file commits changes to the database.
    """
    with open("commit.txt", "w") as f:
        f.write("Committed content\n")

    # Re-open and read to verify
    with open("commit.txt", "r") as f:
        content = f.read()
    assert content == "Committed content\n"

def test_insert_or_replace_behavior(sqldir_patched):
    """
    Test that opening a file in write mode replaces existing content.
    """
    with open("replace.txt", "w") as f:
        f.write("First content\n")

    with open("replace.txt", "w") as f:
        f.write("Second content\n")

    with open("replace.txt", "r") as f:
        content = f.read()
    assert content == "Second content\n"

def test_multiple_modes(sqldir_patched):
    """
    Test opening a file with multiple modes like 'r+' in text mode.
    """
    with open("multi_mode.txt", "w") as f:
        f.write("Initial content\n")

    with open("multi_mode.txt", "r+") as f:
        content = f.read()
        assert content == "Initial content\n"
        f.seek(0)
        f.write("Updated")

    with open("multi_mode.txt", "r") as f:
        content = f.read()
    assert content == "Updated content\n"  # Corrected expectation

def test_connection(sqldir_patched):
    cur = cursor() 
    res = cur.execute("select content from files where name = ?", ("test.txt",))
    assert len(res.fetchone()) == 1 
