#!/usr/bin/env python3
"""
pipeline_full.py – Stand-alone replication of the provided n8n workflow.
Chạy thẳng hàm run_pipeline() để nhận kết quả sau node Postgres2.
"""
# ==============================================================
# 0) IMPORT & CẤU HÌNH – BẢN GHI CỨNG KHÓA
# ==============================================================
# https://chatgpt.com/c/683e9e4e-9798-8000-9cd1-f20d06753cee?model=gpt-4o

import json, datetime
from typing import Dict, Any, List

import psycopg2, openai, google.generativeai as genai
from pinecone import Pinecone   
import pymysql

from dotenv import load_dotenv
import os

DEBUG = True
# ---------- 1. ĐIỀN KHÓA/BÍ MẬT TẠI ĐÂY ----------
load_dotenv()

# Lấy API keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# ==============================================================
# ---------- Khởi tạo SDK ----------
openai.api_key = OPENAI_API_KEY
genai.configure(api_key=GEMINI_API_KEY)

pc    = Pinecone(api_key=PINECONE_API_KEY)          # classic ⇒ thêm environment="xxx-gcp"
index = pc.Index("database-rv-indicator")

memory_buffers: Dict[str, List[dict]] = {}

###############################################################
# 1) HÀM TIỆN ÍCH (SQL, markdown, số)
###############################################################
def embed_text(text: str) -> List[float]:
    """Tạo embedding thực từ OpenAI cho semantic search."""
    response = openai.embeddings.create(
        model="text-embedding-3-small",
        input=[text]  # Lưu ý phải là list
    )
    return response.data[0].embedding

# Replace with your actual MySQL credentials
MYSQL_DSN = {
    "host": "192.168.9.72",
    "user": "HUNG.NTG",
    "password": "GiaHung@56785678",
    "database": "TTPT",
    "port": 3306  # default port
}

def postgres_execute(sql: str) -> List[Dict[str, Any]]:
    print('////////////////')
    if DEBUG:
        print("\n🟡 EXEC SQL (preview):", sql.splitlines()[0], "...")
    connection = pymysql.connect(**MYSQL_DSN)
    try:
        with connection.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        connection.close()
    if DEBUG:
        print("🟢 Rows fetched:", len(rows))
    print('////////////////')
    return rows

def build_schema_map(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    tables: Dict[str, Any] = {}
    for r in rows:
        t, c, dt, desc = r["table_name"], r["column_name"], r.get("data_type","unknown"), r.get("table_description","")
        tables.setdefault(t, {"description": desc, "columns": []})
        tables[t]["columns"].append(f"{c}:{dt}")
    return {"summary": "Schema Overview (with Table Descriptions)", "tables": tables}

def fmt_vi(x):
    try: n=float(x)
    except: return x
    if n>=1e9: return f"{n/1e9:,.2f}".replace(",",".").rstrip("0").rstrip(".")+" B"
    if n>=1e6: return f"{n/1e6:,.2f}".replace(",",".").rstrip("0").rstrip(".")+" m"
    return f"{n:,.0f}".replace(",", ".")

def rows_to_md(rows: List[Dict[str, Any]]) -> str:
    if not rows: return "_Không có dữ liệu_"
    hd=list(rows[0].keys())
    head="| "+" | ".join(hd)+" |"
    div ="| "+" | ".join(["---"]*len(hd))+" |"
    lines=[]
    for r in rows:
        cells=[fmt_vi(r[k]) for k in hd]
        lines.append("| "+" | ".join(map(str,cells))+" |")
    return "\n".join([head,div,*lines])

###############################################################
# 2) PROMPT & SCHEMA
###############################################################
PROMPT_AGENT2_SYSTEM = """# Context
Người dùng đang muốn trích xuất dữ liệu từ database
# Role
Bạn là người tách yêu cầu người dùng thành các thành phần:
1. Chỉ tiêu
2. Thời gian
3. Loại thời gian (Y/Q/M/D, mặc định Y)
4. Mã chứng khoán (nếu có)
"""
PROMPT_AGENT5_SYSTEM = """# Vai trò:
Bạn là AI chuẩn hoá thời gian cho truy vấn dữ liệu tài chính.
Từ:
- time_type ("D","Q","Y")
- thoigian (chuỗi mô tả, vd "2015-2020", "quý 1 2023")
→ Trả 3 trường:
- time_type giữ nguyên
- time_start, time_end chuẩn hoá:
    Q ⇒ "QYYYY_0x" ; Y ⇒ "YYYYY" ; D ⇒ "DYYYY_MM_DD"
Hôm nay: {today}
Chỉ trả JSON, không markdown.
"""
SCHEMA_AGENT2 = {"chitieu":"","thoigian":"","loaithoigian":"Y","mack":""}
SCHEMA_AGENT5 = {"time_type":"Y","time_start":"Y2020","time_end":"Y2020"}

###############################################################
# 3) GEMINI HELPER
###############################################################
def _wrap(text: str):
    return [{"role": "user", "parts": [{"text": text}]}]

def call_gemini(system_prompt: str, user_text: str, schema: dict,
                max_iter: int = 5,
                model_name: str = "gemini-2.0-flash-001") -> dict:
    model = genai.GenerativeModel(model_name)
    template = json.dumps(schema, ensure_ascii=False, indent=2)
    prompt = (
        f"{system_prompt}\n\n"
        f"# Yêu cầu user:\n{user_text}\n\n"
        f"#  Trả về JSON duy nhất, **không được bọc trong ``` hoặc markdown**:\n{template}"
)
    for step in range(1, max_iter + 1):
        txt = model.generate_content(_wrap(prompt)).text.strip()

        # --- Gọt bỏ nếu bị bọc trong code block như ```json ... ``` ---
        if txt.startswith("```"):
            import re
            txt = re.sub(r'^```[a-zA-Z]*\n', '', txt)
            txt = re.sub(r'\n```$', '', txt)
            txt = txt.strip()

        if DEBUG:
            print(f"\n⚡ Gemini STEP {step} raw:\n{txt[:500]} ...\n")

        try:
            data = json.loads(txt)
            if all(k in data for k in schema):
                return data
        except:
            pass

        prompt = f"{txt}\n\n❌ Sai format, hãy trả đúng JSON."
    raise ValueError("Gemini không trả JSON hợp lệ.")

###############################################################
# 4) PINECONE
###############################################################
def query_pinecone(prompt: str, top_k: int = 3, ns: str = "all") -> Dict[str, Any]:
    print(f"\n📘 Pinecone query với prompt: {prompt}")

    # 🚀 Dùng semantic vector thật
    vector = embed_text(prompt)

    res = index.query(
        vector=vector,
        top_k=top_k,
        namespace=ns,
        include_metadata=True
    )

    if not res.matches:
        raise ValueError("❌ ERROR: Không tìm thấy bất kỳ kết quả nào trong Pinecone.")

    print("📗 Pinecone raw matches:")
    for i, m in enumerate(res.matches):
        print(f"🔹 [{i+1}] {m.metadata.get('text', 'N/A')}")

    return res.matches[0].metadata
###############################################################
# 5) PIPELINE
###############################################################
def run_pipeline(user_text: str, conv_id: str = "debug") -> dict:
    memory_buffers.setdefault(conv_id, []).append({"role":"user","content":user_text})

    # ---------- Agent 2 ----------
    agent2 = call_gemini(PROMPT_AGENT2_SYSTEM, user_text, SCHEMA_AGENT2)
    if DEBUG:
        print("\n✅ Agent2 JSON:", json.dumps(agent2, ensure_ascii=False, indent=2))

    # ---------- Pinecone ----------
    meta = query_pinecone(agent2["chitieu"])
    if DEBUG:
        print("📘 Pinecone query với prompt:", agent2["chitieu"])
        print("\n✅ Pinecone meta:", meta)
    table_name = meta["table_name"]
    column     = meta["column_name"].replace("data.data_dict.", "")

    # ---------- Agent 5 ----------
    prompt5 = f"Timetype là: {agent2['loaithoigian']}\nKhoảng thời gian: {agent2['thoigian']}"
    agent5 = call_gemini(
        PROMPT_AGENT5_SYSTEM.format(today=datetime.date.today()),
        prompt5, SCHEMA_AGENT5
    )
    if DEBUG:
        print("\n✅ Agent5 JSON:", agent5)

    # ---------- Build dynamic SQL ----------
    mack     = agent2.get("mack","").strip()
    ttype    = agent5["time_type"]
    t_start  = agent5["time_start"]
    t_end    = agent5["time_end"]
    col_safe = column.replace("'", "''")
    
    where = f"`time_code` BETWEEN '{t_start}' AND '{t_end}' AND `time_code` LIKE '{ttype}%'"
    if mack:
        where = f"`data_name` LIKE '{mack}' AND " + where

    data_sql = f"""SELECT 
        time_code,
        JSON_UNQUOTE(JSON_EXTRACT(data_dict, '$."{col_safe}".data')) AS `{col_safe}`
    FROM TTPT.{table_name.upper()}
    WHERE {where}
    ORDER BY time_code;
    """

    if DEBUG:
        print("\n✅ Dynamic SQL:\n", data_sql)

    # ---------- Postgres2 ----------
    data_rows = postgres_execute(data_sql)
    markdown  = rows_to_md(data_rows)

    return {
        "time_filter": agent5,
        "query": data_sql.strip(),
        "data_markdown": markdown,
        "raw_rows": data_rows
    }

###############################################################
# 6) CHẠY THỬ
###############################################################
if __name__ == "__main__":
    user_query = "lấy vốn chủ sở hữu của HPG từ 2015 tới 2025"
    print("Index có:", pc.list_indexes().names())
    try:
        result = run_pipeline(user_query)
        print("\n🎉 Hoàn tất! Markdown kết quả:\n")
        print(result["data_markdown"])
    except Exception as e:
        print("\n❌ ERROR:", e)
