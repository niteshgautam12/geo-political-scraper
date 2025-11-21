import os
import io
import csv
import traceback
from datetime import datetime
from urllib.parse import urljoin

from flask import (
    Flask, render_template, request, jsonify, redirect, url_for,
    Response, abort
)
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, UniqueConstraint, Boolean
)
from sqlalchemy.orm import declarative_base, sessionmaker
import feedparser
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

load_dotenv()

DB_FILE = os.environ.get("GEOPOLITICS_DB", "geopolitics.sqlite")
USER_AGENT = os.environ.get("USER_AGENT", "GeopoliticsScraper/1.0 (+https://example.com)")
SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "10"))
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")  # set to enable admin actions
KEYWORDS = [k.strip().lower() for k in os.environ.get(
    "KEYWORDS",
    "russia,ukraine,china,taiwan,nato,israel,palestine,iran,afghanistan,india,modi,us,united states,syrian,korea,vladimir,zelensky"
).split(",")]

DEFAULT_SOURCES = [
    {"name": "Al Jazeera", "type": "rss", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"name": "BBC World", "type": "rss", "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "Reuters World", "type": "rss", "url": "http://feeds.reuters.com/Reuters/worldNews"},
    {"name": "NYTimes World", "type": "rss", "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"},
    {"name": "The Guardian World (html)", "type": "html", "url": "https://www.theguardian.com/world"},
]

HEADERS = {"User-Agent": USER_AGENT}

Base = declarative_base()
engine = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False}, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)


class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True)
    source = Column(String(255), index=True)
    title = Column(String(1024))
    url = Column(String(2048), unique=True, index=True)
    summary = Column(Text)
    published = Column(DateTime, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint('url', name='uq_url'),)


class Source(Base):
    __tablename__ = "sources"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(50))  # 'rss' or 'html'
    url = Column(String(2048), unique=True)
    enabled = Column(Boolean, default=True)


Base.metadata.create_all(bind=engine)


def ensure_default_sources():
    session = SessionLocal()
    try:
        existing = session.query(Source).count()
        if existing == 0:
            for s in DEFAULT_SOURCES:
                src = Source(name=s["name"], type=s["type"], url=s["url"], enabled=True)
                session.add(src)
            session.commit()
    finally:
        session.close()


ensure_default_sources()



def parse_rss(url):
    out = []
    feed = feedparser.parse(url)
    for e in (feed.entries or [])[:60]:
        title = e.get("title")
        link = e.get("link")
        summary = e.get("summary") or e.get("description") or ""
        published = None
        if getattr(e, "published_parsed", None):
            try:
                published = datetime(*e.published_parsed[:6])
            except Exception:
                published = None
        out.append({"title": title, "url": link, "summary": summary, "published": published})
    return out


def fetch_html(url, timeout=12):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def extract_links_with_keywords(html, base_url, keywords):
    soup = BeautifulSoup(html, "html.parser")
    found = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip()
        href = urljoin(base_url, a['href'])
        combined = (text + " " + href).lower()
        if any(k in combined for k in keywords):
            found.append({"title": text or href, "url": href, "summary": ""})

    seen = set()
    out = []
    for it in found:
        if it["url"] not in seen:
            out.append(it)
            seen.add(it["url"])
    return out[:80]


def scrape_source(source):
    try:
        if source.get("type") == "rss":
            return parse_rss(source.get("url"))
        else:
            html = fetch_html(source.get("url"))
            return extract_links_with_keywords(html, source.get("url"), KEYWORDS)
    except Exception as e:
        print(f"Error scraping {source.get('name') or source.get('url')}: {e}")
        traceback.print_exc()
        return []


def scrape_source_obj(src_obj):
    src = {"name": src_obj.name, "type": src_obj.type, "url": src_obj.url}
    return scrape_source(src)


def store_items(items, source_name):
    session = SessionLocal()
    added = 0
    try:
        for it in items:
            try:
                title = (it.get("title") or "").strip()
                url = it.get("url")
                if not url or not title:
                    continue
                low = (title + " " + (url or "")).lower()
                if not any(k in low for k in KEYWORDS) and "al jazeera" not in source_name.lower():
                    continue
                exists = session.query(Article).filter(Article.url == url).first()
                if exists:
                    continue
                art = Article(
                    source=source_name[:255],
                    title=title[:1024],
                    url=url[:2048],
                    summary=(it.get("summary") or "")[:4000],
                    published=it.get("published")
                )
                session.add(art)
                session.commit()
                added += 1
            except Exception:
                session.rollback()
    finally:
        session.close()
    return added


def scrape_all():
    session = SessionLocal()
    try:
        sources = session.query(Source).filter(Source.enabled == True).all()
    finally:
        session.close()
    total = 0
    for src in sources:
        items = scrape_source_obj(src)
        n = store_items(items, src.name)
        total += n
    print(f"[{datetime.utcnow().isoformat()}] Added {total} new articles")
    return total


# ----- FULL-TEXT FETCH & SUMMARY HELPER -----


def fetch_full_text_and_summary(url, max_paragraphs=3):
    """
    Fetch page and return a plain-text summary made of first few <p> paragraphs.
    """
    try:
        html = fetch_html(url, timeout=15)
        soup = BeautifulSoup(html, "html.parser")
        article_el = soup.find("article")
        paras = []
        if article_el:
            for p in article_el.find_all("p"):
                text = p.get_text(strip=True)
                if text:
                    paras.append(text)
                if len(paras) >= max_paragraphs:
                    break
        if not paras:
            main = soup.find("main") or soup
            for p in main.find_all("p"):
                text = p.get_text(strip=True)
                if text:
                    paras.append(text)
                if len(paras) >= max_paragraphs:
                    break
        summary = "\n\n".join(paras).strip()
        if not summary:
            desc = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"})
            if desc and desc.get("content"):
                summary = desc.get("content").strip()
        return summary or None
    except Exception as e:
        print("Fulltext fetch failed for", url, e)
        traceback.print_exc()
        return None


app = Flask(__name__, static_folder="static", template_folder="templates")


@app.context_processor
def inject_now():
    return {'now': datetime.utcnow}


@app.route("/")
def index():
    q = request.args.get("q", "").strip().lower()
    session = SessionLocal()
    try:
        rows = session.query(Article).order_by(Article.published.desc().nullslast(), Article.fetched_at.desc()).limit(300).all()
    finally:
        session.close()
    if q:
        rows = [r for r in rows if q in (r.title or "").lower() or q in (r.summary or "").lower()]
    session = SessionLocal()
    try:
        srcs = session.query(Source).filter(Source.enabled == True).all()
        source_names = ", ".join([s.name for s in srcs])
    finally:
        session.close()
    return render_template("index.html", items=rows, q=q, sources=source_names)


@app.route("/article/<int:article_id>")
def article_view(article_id):
    session = SessionLocal()
    try:
        art = session.query(Article).get(article_id)
    finally:
        session.close()
    if not art:
        abort(404)
    return render_template("article.html", article=art)


@app.route("/article/<int:article_id>/fetch_full", methods=["POST"])
def article_fetch_full(article_id):
    session = SessionLocal()
    try:
        art = session.query(Article).get(article_id)
        if not art:
            abort(404)
        summary = fetch_full_text_and_summary(art.url, max_paragraphs=4)
        if summary:
            art.summary = summary
            art.fetched_at = datetime.utcnow()
            session.add(art)
            session.commit()
    except Exception as e:
        session.rollback()
        print("Error fetching full text:", e)
        traceback.print_exc()
    finally:
        session.close()
    return redirect(url_for("article_view", article_id=article_id))


@app.route("/export.csv")
def export_csv():
    session = SessionLocal()
    try:
        rows = session.query(Article).order_by(Article.published.desc().nullslast(), Article.fetched_at.desc()).all()
    finally:
        session.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "title", "url", "source", "published", "fetched_at", "summary"])
    for r in rows:
        writer.writerow([r.id, r.title, r.url, r.source, r.published.isoformat() if r.published else "", r.fetched_at.isoformat(), (r.summary or "").replace("\n", " ").strip()])
    contents = output.getvalue()
    output.close()
    return Response(contents, mimetype="text/csv", headers={"Content-Disposition": "attachment;filename=geopolitics_articles.csv"})

@app.route("/scrape_now")
def scrape_now():
    """Manually trigger scraping and return to the index."""
    try:
        n = scrape_all()
        print(f"Manual scrape added {n} articles")
    except Exception as e:
        print("Manual scrape failed:", e)
    return redirect(url_for("index"))




def check_admin_token(req_token):
    if not ADMIN_TOKEN:
        return False
    return req_token and req_token == ADMIN_TOKEN


@app.route("/admin")
def admin_ui():
    session = SessionLocal()
    try:
        srcs = session.query(Source).all()
    finally:
        session.close()
    return render_template("admin.html", sources=srcs)


@app.route("/admin/add", methods=["POST"])
def admin_add():
    token = request.form.get("token")
    if not check_admin_token(token):
        return "Invalid admin token", 403
    name = request.form.get("name")
    stype = request.form.get("type", "rss")
    url = request.form.get("url")
    if not name or not url:
        return "name+url required", 400
    session = SessionLocal()
    try:
        s = Source(name=name, type=stype, url=url, enabled=True)
        session.add(s)
        session.commit()
    except Exception as e:
        session.rollback()
        return f"Error adding source: {e}", 400
    finally:
        session.close()
    return redirect(url_for("admin_ui"))


@app.route("/admin/toggle", methods=["POST"])
def admin_toggle():
    token = request.form.get("token")
    if not check_admin_token(token):
        return "Invalid admin token", 403
    sid = request.form.get("id")
    session = SessionLocal()
    try:
        s = session.query(Source).get(int(sid))
        if s:
            s.enabled = not s.enabled
            session.add(s)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return redirect(url_for("admin_ui"))


@app.route("/admin/delete", methods=["POST"])
def admin_delete():
    token = request.form.get("token")
    if not check_admin_token(token):
        return "Invalid admin token", 403
    sid = request.form.get("id")
    session = SessionLocal()
    try:
        s = session.query(Source).get(int(sid))
        if s:
            session.delete(s)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return redirect(url_for("admin_ui"))


# ----- API -----


@app.route("/api/articles")
def api_articles():
    session = SessionLocal()
    try:
        rows = session.query(Article).order_by(Article.published.desc().nullslast(), Article.fetched_at.desc()).limit(500).all()
    finally:
        session.close()
    out = []
    for r in rows:
        out.append({
            "id": r.id,
            "title": r.title,
            "url": r.url,
            "summary": r.summary,
            "source": r.source,
            "published": r.published.isoformat() if r.published else None,
            "fetched_at": r.fetched_at.isoformat()
        })
    return jsonify(out)

scheduler = BackgroundScheduler()
scheduler.add_job(func=scrape_all, trigger="interval", minutes=SCRAPE_INTERVAL_MINUTES, next_run_time=datetime.now())
scheduler.start()



if __name__ == "__main__":
    print("Starting enhanced geopolitics scraper webapp...")
    if not ADMIN_TOKEN:
        print("Warning: ADMIN_TOKEN not set — admin actions disabled. Set ADMIN_TOKEN env var to enable.")
    try:
        scrape_all()
    except Exception as e:
        print("Initial scrape failed:", e)
        traceback.print_exc()
   
    app.run(debug=True, port=5000)
