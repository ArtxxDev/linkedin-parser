import argparse
import asyncio
import csv
import json
import logging
import random
from playwright.async_api import async_playwright, Page, Browser
from playwright_stealth import Stealth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def load_emails(path: str) -> list[str]:
    emails = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            email = line.strip()
            if email and "@" in email:
                emails.append(email)
    log.info(f"Загружено {len(emails)} email-адресов")
    return emails


def load_cookies(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict):
        raw = [{"name": k, "value": v} for k, v in raw.items()]
    allowed_fields    = {"name", "value", "domain", "path", "secure", "httpOnly", "sameSite", "expires"}
    allowed_same_site = {"Strict", "Lax", "None"}
    cleaned = []
    for cookie in raw:
        c = {k: v for k, v in cookie.items() if k in allowed_fields}
        if "sameSite" in c and c["sameSite"] not in allowed_same_site:
            del c["sameSite"]
        cleaned.append(c)
    return cleaned


class Parser:
    browser: Browser = None
    context = None
    timeout: int = 60000

    def __init__(self, cookies, base_url, search_path="/search/results/people/", search_param="keywords", proxy=None, headless=True):
        self.cookies      = cookies
        self.base_url     = base_url.rstrip("/")
        self.search_path  = search_path
        self.search_param = search_param
        self.proxy        = proxy
        self.headless     = headless

    async def init(self):
        playwright = await async_playwright().start()
        self._playwright = playwright

        launch_args = {
            "headless": self.headless,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        }

        if self.proxy:
            launch_args["proxy"] = {"server": self.proxy}

        self.browser = await playwright.chromium.launch(**launch_args)
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        await self.context.add_cookies(self.cookies)
        log.info("Браузер запущен, куки установлены")
        await self.check_auth()

    async def check_auth(self):
        page = await self.new_page()
        try:
            search_url = f"{self.base_url}{self.search_path}?{self.search_param}=test"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=self.timeout)
            url = page.url
            if "/login" in url or "/signin" in url or "/auth" in url:
                raise RuntimeError("Сессия истекла или куки неверные. Обнови cookies.json.")
            log.info(f"Авторизация успешна: {url}")
        finally:
            await page.close()

    async def new_page(self) -> Page:
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        await page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())
        return page

    async def auto_scroll(self, page: Page):
        try:
            await page.evaluate("""
                async () => {
                    await new Promise((resolve) => {
                        let totalHeight = 0;
                        const distance = 200;
                        const timer = setInterval(() => {
                            window.scrollBy(0, distance);
                            totalHeight += distance;
                            if (totalHeight >= document.body.scrollHeight) {
                                clearInterval(timer);
                                resolve();
                            }
                        }, 100);
                    });
                }
            """)
        except Exception:
            pass

    async def enrich_from_profile(self, page: Page, result: dict, profile_url: str):
        try:
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=self.timeout)
            await asyncio.sleep(random.uniform(1.5, 2.5))
            await self.auto_scroll(page)
            await asyncio.sleep(1.0)

            name_el = await page.query_selector("h1")
            if name_el:
                result["name"] = (await name_el.inner_text()).strip()

            pos_el = await page.query_selector("div[data-generated-suggestion-target]")
            if not pos_el:
                pos_el = await page.query_selector("h1 + div")
            if pos_el:
                result["position"] = (await pos_el.inner_text()).strip()

            loc_el = await page.query_selector("button[id*='location'] span[aria-hidden='true']")
            if not loc_el:
                loc_el = await page.query_selector("span[id*='location']")
            if loc_el:
                result["location"] = (await loc_el.inner_text()).strip()

            first_job = await page.query_selector("[componentkey^='entity-collection-item--']")
            if first_job:
                all_p = await first_job.query_selector_all("p")
                texts = []
                for p in all_p:
                    t = (await p.inner_text()).strip()
                    if t:
                        texts.append(t)

                if len(texts) >= 1:
                    result["current_job_title"] = texts[0]
                if len(texts) >= 2:
                    parts = texts[1].split("·")
                    result["company"]         = parts[0].strip()
                    result["employment_type"] = parts[1].strip() if len(parts) > 1 else ""
                if len(texts) >= 3:
                    result["job_duration"] = texts[2]
                if len(texts) >= 4:
                    result["job_location"] = texts[3].split("·")[0].strip()

            log.info(f"    → {result.get('name')} | {result.get('current_job_title')} | {result.get('company')} | {result.get('job_location')}")
        except Exception as e:
            log.warning(f"    → Профиль недоступен: {e}")

    async def lookup_email(self, page: Page, email: str) -> dict:
        result = {
            "email":            email,
            "found":            False,
            "name":             "",
            "position":         "",
            "location":         "",
            "current_job_title": "",
            "company":          "",
            "employment_type":  "",
            "job_duration":     "",
            "job_location":     "",
            "profile_url":      "",
        }

        username = email.split("@")[0]
        url = f"{self.base_url}{self.search_path}?{self.search_param}={username}"

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)
            await asyncio.sleep(random.uniform(0.3, 0.8))

            page_count = 1

            while True:
                try:
                    await page.wait_for_selector("[role='listitem']", timeout=5000)
                except Exception:
                    pass

                await self.auto_scroll(page)
                await asyncio.sleep(2.0)

                cards = await page.query_selector_all("[role='listitem']")

                for card in cards:
                    name_el = await card.query_selector("p[componentkey]")
                    all_p   = await card.query_selector_all("p")
                    url_el  = await card.query_selector("a[href*='/in/']")

                    if not name_el:
                        continue

                    name_text = (await name_el.inner_text()).strip()
                    if name_text == "LinkedIn Member":
                        continue

                    paragraphs = []
                    for p in all_p:
                        t = (await p.inner_text()).strip()
                        if t:
                            paragraphs.append(t)

                    result["found"]    = True
                    result["name"]     = (paragraphs[0] if paragraphs else "").split("•")[0].strip()
                    result["position"] = paragraphs[1] if len(paragraphs) > 1 else ""
                    result["location"] = paragraphs[2] if len(paragraphs) > 2 else ""

                    href = (await url_el.get_attribute("href")) if url_el else ""
                    profile_url = href.split("?")[0] if href else ""
                    result["profile_url"] = profile_url

                    if profile_url:
                        await self.enrich_from_profile(page, result, profile_url)
                        await page.go_back(wait_until="domcontentloaded", timeout=self.timeout)

                    log.info(f"  ✓  {email}  →  {result['name']} [{result['position']}]")
                    return result

                next_btn = await page.query_selector("button.artdeco-pagination__button--next")
                if not next_btn:
                    break
                if await next_btn.get_attribute("disabled") is not None:
                    break

                await next_btn.click()
                await page.wait_for_load_state("domcontentloaded", timeout=self.timeout)
                page_count += 1

            if not result["found"]:
                log.info(f"  ✗  {email}  →  не найден")

        except asyncio.TimeoutError:
            log.warning(f"  !  {email}  →  timeout")
        except Exception as e:
            log.error(f"  !  {email}  →  ошибка: {e}")

        return result

    async def run(self, emails: list[str], delay: float = 1.0, workers: int = 3) -> list[dict]:
        total = len(emails)
        results = [None] * total
        semaphore = asyncio.Semaphore(workers)

        async def process(i, email):
            async with semaphore:
                log.info(f"[{i+1}/{total}] {email}")
                page = await self.new_page()
                try:
                    result = await self.lookup_email(page, email)
                    results[i] = result
                    await asyncio.sleep(delay + random.uniform(0, 0.5))
                except Exception as e:
                    log.error(f"  !  {email}  →  {e}")
                    results[i] = {"email": email, "found": False}
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass

        await asyncio.gather(*[process(i, email) for i, email in enumerate(emails)])
        return [r for r in results if r is not None]

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
        log.info("Браузер закрыт")


FIELDNAMES = ["email", "found", "name", "position", "location", "current_job_title", "company", "employment_type", "job_duration", "job_location", "profile_url"]


def save_csv(results: list[dict], path: str):
    cleaned = [{k: r.get(k, "") for k in FIELDNAMES} for r in results if r]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(cleaned)
    log.info(f"Сохранено → {path}  ({len(cleaned)} строк)")


def parse_args():
    p = argparse.ArgumentParser(description="Playwright Email Parser")
    p.add_argument("--emails",       required=True)
    p.add_argument("--cookies",      required=True)
    p.add_argument("--url",          required=True)
    p.add_argument("--search-path",  default="/search/results/people/")
    p.add_argument("--search-param", default="keywords")
    p.add_argument("--output",       default="results.csv")
    p.add_argument("--delay",        type=float, default=1.0)
    p.add_argument("--proxy",        default=None)
    p.add_argument("--workers",      type=int, default=3)
    p.add_argument("--tor",          action="store_true")
    p.add_argument("--visible",      action="store_true", default=True)
    return p.parse_args()


async def main():
    args = parse_args()
    emails  = load_emails(args.emails)
    cookies = load_cookies(args.cookies)

    proxy = None
    if args.tor:
        proxy = "socks5://127.0.0.1:9050"
        log.info("Используем Tor")
    elif args.proxy:
        proxy = args.proxy
        log.info(f"Используем прокси: {proxy}")

    parser = Parser(
        cookies=cookies,
        base_url=args.url,
        search_path=args.search_path,
        search_param=args.search_param,
        proxy=proxy,
        headless=not args.visible,
    )

    results = []
    try:
        await parser.init()
        results = await parser.run(emails, delay=args.delay, workers=args.workers)
        found = sum(1 for r in results if r["found"])
        log.info(f"Итого: найдено {found}/{len(results)}")
        save_csv(results, args.output)
    except KeyboardInterrupt:
        log.info("Остановлено пользователем")
        if results:
            save_csv(results, args.output)
    finally:
        await parser.close()


if __name__ == "__main__":
    asyncio.run(main())
    