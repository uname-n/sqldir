# sqldir

**sqldir** patches standard file operations, allowing you to read write and manipulated files from a sqlite database.

## Installation
To install **sqldir**, run the following command:
```bash
pip install sqldir
```

## Quick Start
```python
from sqldir import install_patch, cursor
install_patch()

with open("file.txt", "w") as f:
    f.write("hello")

with open("file.txt", "a") as f:
    f.write("world")

cur = cursor()
res = cur.execute("select filename, content from files").fetchone()
print(res)
```
