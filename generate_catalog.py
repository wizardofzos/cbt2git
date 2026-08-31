#!/usr/bin/env python
"""Generate searchable Markdown and HTML catalogs from CBT pickle data."""
import argparse
import html
import json
import os
import sys
import pandas as pd


def guess_category(comment):
    """Derives a useful functional category from the file description."""
    text = str(comment).lower()
    if any(k in text for k in ["rexx", "clist", "exec"]):
        return "REXX / CLIST"
    if any(k in text for k in ["asm", "assembler", "dsect", "macro", "hlasm"]):
        return "Assembler / Macro"
    if any(k in text for k in ["ispf", "panel", "dialog", "edit macro"]):
        return "ISPF Utility"
    if any(k in text for k in ["jcl", "proc", "submit"]):
        return "JCL / Batch"
    if any(k in text for k in ["smf", "accounting", "logrec", "racc"]):
        return "SMF / Reporting"
    if any(k in text for k in ["vtoc", "catalog", "dasd", "disk", "storage", "vvds", "volser"]):
        return "Storage / DASD"
    if any(k in text for k in ["cbt doc", "manual", "documentation", "guide"]):
        return "Documentation"
    if any(k in text for k in ["security", "racf", "acf2", "top secret"]):
        return "Security / RACF"
    if any(k in text for k in ["cics", "ims", "db2"]):
        return "Subsystems (CICS/DB2)"
    return "General Utility"


def generate(pickle_path=".cbt.pkl", out_md="CATALOG.md", out_html="index.html"):
    if not os.path.exists(pickle_path):
        print(f"Error: {pickle_path} not found. Run cbt-get first.")
        sys.exit(1)

    df = pd.read_pickle(pickle_path)
    print(f"Loaded {len(df)} entries from {pickle_path}")

    #Generate Markdown Catalog 
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("# CBT Tape Repository Directory\n\n")
        f.write("A searchable index of all mirrored CBT Tape repositories.\n\n")
        f.write("| File # | Repository | Description | Category | Updated? |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- |\n")

        for _, row in df.iterrows():
            cbtnum_str = str(row["cbtnum"]).strip()
            num = cbtnum_str.zfill(3) if cbtnum_str.isdigit() else cbtnum_str
            url = f"https://github.com/CBTTape/CBT{num}"
            raw_comment = str(row.get("comment", "")).strip()
            comment = html.escape(raw_comment.replace("|", "-"))
            category = guess_category(raw_comment)
            updated_str = "Yes" if bool(row.get("updated", False)) else "No"
            f.write(f"| {num} | [CBT{num}]({url}) | {comment} | {category} | {updated_str} |\n")

    print(f"Saved markdown directory to {out_md}")

    # Generate Interactive Search HTML (index.html for GitHub Pages)
    records = []
    for _, row in df.iterrows():
        cbtnum_str = str(row["cbtnum"]).strip()
        num = cbtnum_str.zfill(3) if cbtnum_str.isdigit() else cbtnum_str
        raw_comment = str(row.get("comment", "")).strip()
        records.append({
            "cbtnum": f"<a href='https://github.com/CBTTape/CBT{num}' target='_blank'>CBT{num}</a>",
            "comment": html.escape(raw_comment),
            "category": guess_category(raw_comment),
            "updated": "Yes" if bool(row.get("updated", False)) else "No",
        })

    json_records = json.dumps(records)

    html_code = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CBT Tape Explorer</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background-color: #f4f6f8;
            margin: 0;
            padding: 30px 15px;
            color: #24292f;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: #ffffff;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        }}
        h1 {{
            margin-top: 0;
            color: #0969da;
        }}
        p.subtitle {{
            color: #57606a;
            margin-bottom: 24px;
        }}
        table.dataTable {{
            border-collapse: collapse !important;
            width: 100% !important;
        }}
        th {{
            background-color: #f6f8fa;
            text-align: left;
            padding: 12px !important;
        }}
        td {{
            padding: 10px 12px !important;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>CBT Tape Master Directory</h1>
        <p class="subtitle">Search, filter, and jump directly to any of the 1,000+ CBT Tape repositories on GitHub.</p>
        <table id="cbtTable" class="display">
            <thead>
                <tr>
                    <th>Repository</th>
                    <th>Description</th>
                    <th>Category</th>
                    <th>Latest Update</th>
                </tr>
            </thead>
        </table>
    </div>

    <script>
        const tableData = {json_records};
        $(document).ready(function () {{
            $('#cbtTable').DataTable({{
                data: tableData,
                columns: [
                    {{ data: 'cbtnum' }},
                    {{ data: 'comment' }},
                    {{ data: 'category' }},
                    {{ data: 'updated' }}
                ],
                pageLength: 50,
                lengthMenu: [25, 50, 100, 250, 500],
                order: [[0, 'asc']]
            }});
        }});
    </script>
</body>
</html>"""

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html_code)
    print(f"Saved interactive HTML to {out_html}")


def main():
    parser = argparse.ArgumentParser(description="Generate Markdown and HTML catalogs from CBT Tape pickle data.")
    parser.add_argument("--pickle", default=".cbt.pkl", help="Path to .cbt.pkl file (default: .cbt.pkl)")
    parser.add_argument("--out-md", default="CATALOG.md", help="Markdown output path (default: CATALOG.md)")
    parser.add_argument("--out-html", default="index.html", help="HTML output path (default: index.html)")
    args = parser.parse_args()

    generate(pickle_path=args.pickle, out_md=args.out_md, out_html=args.out_html)


if __name__ == "__main__":
    main()