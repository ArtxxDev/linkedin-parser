# linkedin-parser

Install:
pip3 install playwright playwright-stealth
python3 -m playwright install chromium

Run:
python parser.py --emails emails.txt --cookies cookies.json --url https://www.linkedin.com --workers 2
