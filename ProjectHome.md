SqliteFS allows a sqlite database to be presented as a file system node. By using regular file system commands (and using existing browsers, shells, and libraries), one can alter the structure and items of a sqlite database.

This file system adapter makes use of the fuse Python binding and an abstracted file system class (to simplify the development of platform-independent file systems).

If a fuse-equivalent is available in Microsoft systems, could be easily ported, as it was designed to have a pluggable system-implementation backend.

As for the license ... it's simple:

If you derive financial benefit from these artifacts, consider donating an appropriate amount. That's just good karma.

If you're just playing around with the code, good for you -- enjoy, learn, and improve it.