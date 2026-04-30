---
description: Always load these instructions when generating or modifying code for this project.
---

Format code up to 100 columns width. Prefer generating fewer lines of code instead of being verbose.

Only create comments for non-obvious code, generally as function docstrings. Avoid generating many comments.

Try to avoid handling parsing errors for required data completely. If the data is required, just let the exception happen, and let the script crash. This provides the call stack, and helps diagnostics, and does not swallow errors silently. Only handle parsing errors for optional data, and in that case, just ignore the field if it cannot be parsed, but do not let the script crash.