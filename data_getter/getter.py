import requests
import json
import time
import os
import math

# 获取路径
current_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(current_dir, '..', 'raw_data', 'raw_movies_data_tst2.json')
OUTPUT_FILE = os.path.normpath(OUTPUT_FILE)

# --- 配置区 ---
API_KEY = "9dbb23330ed549c60ffbc45b60cd74d8"  # 记得替换
START_YEAR = 2025               # 起始年份
END_YEAR = 2025                 # 结束年份
MOVIES_PER_YEAR = 20           # 每年目标抓取数量 (Top 250)

def get_movies_by_year_paginated(year, target_count):
    movie_ids = []
    max_pages = math.ceil(target_count / 20)
    
    print(f"  - 正在获取 {year} 年的 ID 列表，需扫描 {max_pages} 页...")
    
    base_url = "https://api.themoviedb.org/3/discover/movie"
    
    for page in range(1, max_pages + 1):
        params = {
            "api_key": API_KEY,
            "primary_release_year": year,
            "sort_by": "popularity.desc",
            "page": page
        }
        try:
            res = requests.get(base_url, params=params, timeout=10)
            if res.status_code == 200:
                new_ids = [m['id'] for m in res.json().get('results', [])]
                movie_ids.extend(new_ids)
                # 如果已经凑够了，就提前退出
                if len(movie_ids) >= target_count:
                    break
            else:
                print(f"    ! 第 {page} 页获取失败 (Code: {res.status_code})")
            
            # 翻页稍微停顿一下
            time.sleep(0.2)
            
        except Exception as e:
            print(f"    ! 连接错误: {e}")
            
    # 截取前 target_count 个
    return movie_ids[:target_count]

def get_full_details(movie_id):
    """获取详情 + 演职员表"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": API_KEY, "append_to_response": "credits"}
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 429: # 触发限流
            print("    ! 触发限流 (429)，暂停 3 秒...")
            time.sleep(3)
            return get_full_details(movie_id) # 重试
    except Exception:
        pass
    return None

def clean_data(raw):
    if not raw: return None
    
    credits = raw.get("credits", {})
    
    # --- 辅助函数：给人员信息“抽脂” ---
    def minify_person(p):
        return {
            "id": p.get("id"),
            "name": p.get("name"),
            "gender": p.get("gender") # 1=女, 2=男
        }

    # --- 1. 处理 Cast (前10) ---
    raw_cast = credits.get("cast", [])
    # 只取前10，并且只保留关键字段
    clean_cast = [minify_person(p) for p in raw_cast[:10]]
    
    # --- 2. 处理 Crew (只找导演) ---
    raw_crew = credits.get("crew", [])
    # 找到所有导演
    directors = [p for p in raw_crew if p.get('job') == 'Director']
    # 清洗字段
    clean_directors = [minify_person(p) for p in directors]
    
    # --- 3. 处理国家 ---
    countries = raw.get("origin_country", [])
    if not countries and raw.get("production_countries"):
        countries = [c["iso_3166_1"] for c in raw["production_countries"]]

    # --- 4. 组装最终结果 ---
    return {
        "id": raw.get("id"),
        "title": raw.get("title"),
        "original_title": raw.get("original_title"),
        "release_date": raw.get("release_date"),
        "runtime": raw.get("runtime"),
        "origin_country": countries, 
        "credits": {
            "cast": clean_cast,       # 已经是瘦身版
            "directors": clean_directors # 这里直接叫 directors 更清晰
        }
    }

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(current_dir, OUTPUT_FILE)
    
    total_years = END_YEAR - START_YEAR + 1
    total_movies_saved = 0
    
    print(f"🚀 开始抓取 {START_YEAR}-{END_YEAR} 年间每年的 Top {MOVIES_PER_YEAR} 电影")
    print(f"📁 结果将保存至: {output_path}")
    print("-" * 50)
    
    # 'w' 模式打开文件，准备流式写入
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("[\n")
        is_first_entry = True
        
        for year in range(START_YEAR, END_YEAR + 1):
            print(f"\n📅 正在处理年份: {year}")
            
            # 1. 先把这一年的 ID 全拿到
            ids = get_movies_by_year_paginated(year, MOVIES_PER_YEAR)
            print(f"  > 找到 {len(ids)} 部电影，开始下载详情...")
            
            # 2. 逐个下载详情
            for idx, m_id in enumerate(ids):
                # 打印进度条
                print(f"\r    [{idx+1}/{len(ids)}] Fetching ID: {m_id} ...   ", end="")
                
                full_data = get_full_details(m_id)
                clean = clean_data(full_data)
                
                if clean:
                    if not is_first_entry:
                        f.write(",\n")
                    json.dump(clean, f, ensure_ascii=False)
                    is_first_entry = False
                    total_movies_saved += 1
                
                # 稍微休息，防止过于频繁
                time.sleep(0.1)
                
            print("") # 这一年结束后换行
            
        f.write("\n]")
        
    print("-" * 50)
    print(f"\n✅ 任务完成！共保存 {total_movies_saved} 部电影。")

if __name__ == "__main__":
    main()