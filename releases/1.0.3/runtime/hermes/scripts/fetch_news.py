#!/usr/bin/env python3
"""
Step 1: 多源抓取 → 保存到 /tmp/news_fetched.json
用法: python3 fetch_news.py
"""
import urllib.request, xml.etree.ElementTree as ET, ssl, time, json, re, sys
from datetime import datetime, timezone, timedelta

ctx = ssl.create_default_context()
BJT = timezone(timedelta(hours=8))

SOURCES = [
    ("IT之家",          "https://www.ithome.com/rss/",                 "中文", 30),
    ("36kr",            "https://36kr.com/feed",                       "中文", 30),
    ("钛媒体",          "https://www.tmtpost.com/rss",                 "中文", 18),
    ("爱范儿",          "https://www.ifanr.com/feed",                  "中文", 20),
    ("VentureBeat AI",  "https://venturebeat.com/category/ai/feed",     "英文", 10),
    ("TechCrunch AI",   "https://techcrunch.com/category/artificial-intelligence/feed/","英文",20),
    ("MIT TR",          "https://www.technologyreview.com/feed/",       "英文", 10),
    ("MarkTechPost",    "https://www.marktechpost.com/feed/",           "英文", 10),
    ("Synced Review",   "https://syncedreview.com/feed/",              "英文", 10),
    ("KDnuggets",       "https://www.kdnuggets.com/feed",             "英文", 10),
    ("AI Trends",       "https://www.aitrends.com/feed/",              "英文", 10),
    ("AI News",         "https://artificialintelligence-news.com/feed/","英文",12),
    ("Wired",           "https://www.wired.com/feed/rss",             "英文", 30),
    ("Ars Technica",    "https://feeds.arstechnica.com/arstechnica/index","英文",20),
    ("Hacker News",     "https://hnrss.org/frontpage",                 "英文", 20),
    ("Quanta Magazine",  "https://www.quantamagazine.org/feed/",         "英文", 10),
    ("Google AI",       "https://blog.google/technology/ai/rss/",        "英文", 20),
    ("NVIDIA Blog",     "https://blogs.nvidia.com/feed/",             "英文", 20),
    ("OpenAI Blog",     "https://openai.com/blog/rss.xml",             "英文", 30),
    ("arXiv cs.AI",     "https://arxiv.org/rss/cs.AI",                 "英文", 20),
    ("arXiv cs.CL",     "https://arxiv.org/rss/cs.CL",                 "英文", 20),
    ("arXiv cs.LG",     "https://arxiv.org/rss/cs.LG",                 "英文", 20),
    ("arXiv cs.CV",     "https://arxiv.org/rss/cs.CV",                 "英文", 20),
    ("GitHub",          "github",                                       "英文", 15),
]

TAG_MAP = {
    "IT之家":"IT/数码/AI","36kr":"科技创投","钛媒体":"TMT深度","爱范儿":"产品/科技",
    "VentureBeat AI":"AI行业","TechCrunch AI":"科技AI","MIT TR":"AI深度",
    "MarkTechPost":"AI技术","Synced Review":"AI研究","KDnuggets":"数据科学/AI",
    "AI Trends":"AI趋势","AI News":"AI新闻","Wired":"科技文化","Ars Technica":"科技/AI",
    "Hacker News":"程序员社区","Quanta Magazine":"科学/AI","Google AI":"Google AI",
    "NVIDIA Blog":"NVIDIA官方","OpenAI Blog":"OpenAI官方",
    "arXiv cs.AI":"学术论文","arXiv cs.CL":"学术论文","arXiv cs.LG":"学术论文","arXiv cs.CV":"学术论文",
    "GitHub":"开源项目",
}

def ai_classify(title, summary):
    text = (title + ' ' + summary).lower()
    if any(k in text for k in ['llm','gpt','gemini','claude','model','训练','fine-tun','rlhf','moe','scaling','transformer','neural','网络结构','参数','benchmark']):
        return "🚀技术突破"
    if any(k in text for k in ['发布','reveal','launch','announce','unveil','首发','曝光','release','debut']):
        return "🔥今日热点"
    if any(k in text for k in ['invest','funding','series','raise','ceo','cooperat','partner','融资','合作','收购','acqui','ipo','战略']):
        return "🏢企业动态"
    if any(k in text for k in ['tutorial','tool','open-source','github','dataset','教程','开源','工具','install','setup','framework','library']):
        return "🛠️工具/教程"
    return "💼商业模式"

def fetch_rss(name, url, lang, limit=30):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=8, context=ctx)
        content = resp.read().decode('utf-8', errors='ignore')
        root = ET.fromstring(content)
        items = root.findall('.//item') or root.findall('.//entry')
        records = []
        for item in items[:limit]:
            title = (item.findtext('title') or '').strip()
            link = (item.findtext('link') or '').strip()
            if not link:
                link = item.findtext('guid') or ''
            pub_str = (item.findtext('pubDate') or item.findtext('published') or item.findtext('updated') or '')
            desc = re.sub(r'<[^>]+>', '', (item.findtext('description') or item.findtext('summary') or ''))[:300]
            if pub_str:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(pub_str.strip())
                except:
                    pub_dt = datetime.now(BJT)
            else:
                pub_dt = datetime.now(BJT)
            records.append({
                "title": title[:200],
                "link": link,
                "platform": name,
                "pub_time": int(pub_dt.timestamp() * 1000),
                "summary": desc,
                "lang": lang,
                "category": TAG_MAP.get(name, "其他"),
            })
        return records
    except Exception as e:
        print(f"  抓取失败 {name}: {e}", file=sys.stderr)
        return []

def fetch_github(lang, limit=15):
    try:
        url = "https://api.github.com/search/repositories?q=AI+created:>2026-04-10&sort=stars&order=desc&per_page=15"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0","Accept":"application/vnd.github.v3+json"})
        resp = urllib.request.urlopen(req, timeout=8, context=ctx)
        data = json.loads(resp.read().decode())
        records = []
        for item in data.get('items', [])[:limit]:
            created = datetime.fromisoformat(item['created_at'].replace('Z','+00:00')).timestamp()
            records.append({
                "title": f"{item['full_name']} ⭐{item.get('stargazers_count',0)}",
                "link": item.get('html_url',''),
                "platform": "GitHub",
                "pub_time": int(created * 1000),
                "summary": (item.get('description') or '')[:300],
                "lang": lang,
                "category": "开源项目",
            })
        return records
    except Exception as e:
        print(f"  抓取失败 GitHub: {e}", file=sys.stderr)
        return []

# 主流程
print(f"[{datetime.now(BJT).strftime('%Y-%m-%d %H:%M')}] 抓取开始...")
all_new = []
src_stats = {}
for name, url, lang, limit in SOURCES:
    if url == 'github':
        recs = fetch_github(lang, limit)
    else:
        recs = fetch_rss(name, url, lang, limit)
    for r in recs:
        r['category'] = ai_classify(r['title'], r['summary'])
    src_stats[name] = len(recs)
    all_new.extend(recs)
    time.sleep(0.2)

# 统计
from collections import Counter
cats = Counter(r['category'] for r in all_new)
print(f"抓取完成: {len(all_new)} 条\n")
for c, n in cats.most_common():
    print(f"  {c}: {n}条")

# 保存
with open('/tmp/news_fetched.json', 'w', encoding='utf-8') as f:
    json.dump({"records": all_new, "stats": src_stats, "fetched_at": datetime.now(BJT).isoformat()}, f, ensure_ascii=False)
print(f"\n已保存到 /tmp/news_fetched.json")
