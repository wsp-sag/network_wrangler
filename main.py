"""Macros for MkDocs."""

import os
import re


def define_env(env):
    """This is the hook for defining variables, macros and filters.

    - variables: the dictionary that contains the environment variables
    - macro: a decorator function, to declare a macro.
    """

    @env.macro
    def include_file(filename: str, downshift_h1=True, start_line: int = 0, end_line: int = None):
        """Include a file, optionally indicating start_line and end_line.

        Args:
            filename: file to include, relative to the top directory of the documentation project.
            downshift_h1: If true, will downshift headings by 1 if h1 heading found.
                Defaults to True.
            start_line (Optional): if included, will start including the file from this line
                (indexed to 0)
            end_line (Optional): if included, will stop including at this line (indexed to 0)
        """
        full_filename = os.path.join(env.project_dir, filename)
        with open(full_filename, "r") as f:
            lines = f.readlines()
        line_range = lines[start_line:end_line]
        content = "".join(line_range)

        # Downshift headings if h1 found
        md_heading_re = {
            1: re.compile(r"(#{1}\s)(.*)"),
            2: re.compile(r"(#{2}\s)(.*)"),
            3: re.compile(r"(#{3}\s)(.*)"),
            4: re.compile(r"(#{4}\s)(.*)"),
            5: re.compile(r"(#{5}\s)(.*)"),
        }
        print(f"???before downshifting! {full_filename}")
        if md_heading_re[1].search(content) and downshift_h1:
            print("!!!downshifting!")
            content = re.sub(md_heading_re[5], r"#\1\2", content)
            content = re.sub(md_heading_re[4], r"#\1\2", content)
            content = re.sub(md_heading_re[3], r"#\1\2", content)
            content = re.sub(md_heading_re[2], r"#\1\2", content)
            content = re.sub(md_heading_re[1], r"#\1\2", content)

        return content
