#!/usr/bin/env python3
"""
纯 Python TextRank 实现——无外部依赖。
用于从 summary_raw 生成提取式摘要，供 OpenClaw 辅助判断。
"""
import re


# ── 停用词表（静态，内嵌）────────────────────────────────
_HN_BOILERPLATE = {
    'article url:', 'comments url:', 'points:', '# comments:',
    'hacker news', 'show hn', 'url:', 'http', 'https://',
}

# ── 预过滤 ───────────────────────────────────────────────
def _prefilter(text: str) -> str:
    """对 Hacker News 等来源进行行级预过滤，去除 URL 行和 boilerplate"""
    lines = text.split('\n')
    filtered = []
    for line in lines:
        stripped = line.strip().lower()
        # 跳过纯 URL 行
        if stripped.startswith('article url:') or stripped.startswith('comments url:'):
            continue
        # 跳过太短的行（通常是元数据行）
        if len(stripped) < 20:
            continue
        # 跳过纯数字/标点行
        if re.match(r'^[\d\s\W]+$', stripped):
            continue
        filtered.append(line)
    return '\n'.join(filtered)


# ── 分词 ─────────────────────────────────────────────────
def _tokenize(text: str) -> set:
    text_lower = text.lower()
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text_lower))
    total_chars = len(text_lower.replace(' ', ''))
    is_chinese = total_chars > 0 and chinese_chars > total_chars * 0.3

    tokens = set()
    if is_chinese:
        # 中文：2~4 字滑动窗口
        for n in range(2, 5):
            for i in range(len(text_lower) - n + 1):
                word = text_lower[i:i+n]
                if re.match(r'^[\u4e00-\u9fff]+$', word):
                    tokens.add(word)
    else:
        # 英文：单词 + 3~5 char ngram
        words = re.findall(r'[a-z0-9]{2,}', text_lower)
        for w in words:
            tokens.add(w)
        for n in range(3, 6):
            for i in range(len(text_lower) - n + 1):
                word = text_lower[i:i+n]
                if re.match(r'^[a-z0-9]+$', word):
                    tokens.add(word)
    return tokens


# ── Jaccard ───────────────────────────────────────────────
def _jaccard(s1: str, s2: str) -> float:
    t1 = _tokenize(s1)
    t2 = _tokenize(s2)
    if not t1 or not t2:
        return 0.0
    inter = len(t1 & t2)
    union = len(t1 | t2)
    return inter / union if union > 0 else 0.0


# ── TextRank 核心 ─────────────────────────────────────────
def textrank_sentences(text: str, top_n: int = 3, min_sent_len: int = 10,
                       damping: float = 0.85, max_iter: int = 10) -> list:
    """
    纯 Python TextRank 提取 top_n 关键句。

    参数:
        text: 输入文本
        top_n: 提取句数
        min_sent_len: 句子最短长度
        damping: PageRank 阻尼系数
        max_iter: 迭代轮数
    返回:
        list[str]: 提取的关键句（按原文顺序）
    """
    # 1. 预过滤（Hacker News 等）
    text_filtered = _prefilter(text)
    if len(text_filtered.strip()) < 30:
        text_filtered = text  # 兜底用原文

    # 2. 句子切分
    raw_sents = re.split(r'[。！？.!?\n]+', text_filtered)
    sentences = [s.strip() for s in raw_sents if len(s.strip()) >= min_sent_len]

    if not sentences:
        return []
    if len(sentences) <= top_n:
        return sentences

    # 3. 建相似度图
    n = len(sentences)
    out_weights = [0.0] * n  # 出度权重和

    # 第一轮：计算所有相似度同时记录出度
    sim_matrix = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            sim = _jaccard(sentences[i], sentences[j])
            if sim > 0.01:  # 阈值过滤噪音
                sim_matrix[i][j] = sim
                sim_matrix[j][i] = sim
                out_weights[i] += sim
                out_weights[j] += sim

    # 4. PageRank 迭代
    scores = [1.0 / n] * n
    for _ in range(max_iter):
        new_scores = [(1 - damping) / n] * n
        for i in range(n):
            for j in range(n):
                w = sim_matrix[j][i]
                if w > 0 and out_weights[j] > 0:
                    new_scores[i] += damping * scores[j] * (w / out_weights[j])
        scores = new_scores

    # 5. 取 top_n（按原文顺序输出）
    ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
    top_indices = [idx for idx, _ in ranked[:top_n]]
    top_indices.sort()
    return [sentences[i] for i in top_indices]


# ── 主入口 ───────────────────────────────────────────────
def extract_summary(text: str, top_n: int = 3, fallback_len: int = 200) -> str:
    """
    生成提取式摘要。

    策略:
    1. 尝试 TextRank 提取 top_n 关键句
    2. 失败则返回前 fallback_len 字原文摘录

    参数:
        text: 原始摘要文本（summary_raw）
        top_n: 提取句数
        fallback_len: 兜底原文截取长度
    返回:
        str: 提取式摘要
    """
    if not text or len(text.strip()) < 30:
        return (text or '').strip()[:fallback_len]

    try:
        sentences = textrank_sentences(text, top_n=top_n, min_sent_len=10)
        if sentences:
            result = '。'.join(sentences)
            if len(result.strip()) < 30:
                return text.strip()[:fallback_len]
            return result
    except Exception:
        pass

    return text.strip()[:fallback_len]
