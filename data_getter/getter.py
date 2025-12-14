import requests
import json
import time
import os

# --- 配置区 ---
API_KEY = "9dbb23330ed549c60ffbc45b60cd74d8"  # 记得替换
TARGET_YEAR = 2019             # 抓取该年份之后的电影
MAX_PAGES = 1                  # 限制抓取的页数（每页20部），测试时建议设小一点，比如 5
OUTPUT_FILE = "..\\raw_data\\raw_movies_data_tst.json"

def get_movie_ids_from_discover(page):
    """从发现接口获取电影 ID 列表"""
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": API_KEY,
        "primary_release_date.gte": f"{TARGET_YEAR}-01-01",
        "sort_by": "popularity.desc",
        "page": page
    }
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            return [m['id'] for m in res.json().get('results', [])]
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
    return []

def get_full_movie_details(movie_id):
    """
    获取单部电影的【全量】信息
    使用 append_to_response 同时抓取：
    - credits: 演员和导演
    - release_dates: 发行日期（包含详细的国家分级信息）
    - keywords: 关键词
    """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {
        "api_key": API_KEY,
        # 这里是一次性抓全所有关联数据的关键
        "append_to_response": "credits,release_dates,keywords" 
    }
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 429:
            print("触发速率限制，暂停 5 秒...")
            time.sleep(5)
            return get_full_movie_details(movie_id) # 重试
    except Exception as e:
        print(f"Error fetching movie {movie_id}: {e}")
    return None

def main():
    all_movies_data = []
    
    print(f"Step 1: 正在扫描 {TARGET_YEAR} 年后的热门电影 ID...")
    
    # 1. 收集 ID
    movie_ids = []
    for page in range(1, MAX_PAGES + 1):
        print(f"  - Scanning page {page}/{MAX_PAGES}...")
        ids = get_movie_ids_from_discover(page)
        movie_ids.extend(ids)
        time.sleep(0.5) # 稍微休息一下
    
    print(f"共发现 {len(movie_ids)} 部电影，准备获取详细数据...")
    
    # 2. 逐个获取详细信息
    for i, m_id in enumerate(movie_ids):
        print(f"[{i+1}/{len(movie_ids)}] Fetching details for ID: {m_id}", end="\r")
        
        details = get_full_movie_details(m_id)
        if details:
            all_movies_data.append(details)
        
        # 礼貌性延时，避免被封 IP
        time.sleep(0.25)

    # 3. 保存到本地文件
    print(f"\n\nStep 2: 正在将 {len(all_movies_data)} 条原始数据写入文件...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_movies_data, f, ensure_ascii=False, indent=2)
    
    print(f"成功！原始数据已保存至: {os.path.abspath(OUTPUT_FILE)}")
    print("现在我们可以随时分析这个文件，而不需要联网了。")

if __name__ == "__main__":
    main()