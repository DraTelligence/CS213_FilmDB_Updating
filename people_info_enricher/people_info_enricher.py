import json
import requests
import time
import os
import concurrent.futures
from dotenv import load_dotenv

# --- 配置区 ---
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

# 的 7 年数据文件名
FILE_PATTERN = 'raw_movies_data_{}.json' 
START_YEAR = 2019
END_YEAR = 2019
OUTPUT_FILE = '../raw_data/people_details_map_tst.json'

# 只留前 N 个主演
MAX_CAST_ORDER = 4

# 并发数量 (TMDB 建议不要超过 20)
MAX_WORKERS = 12

def get_person_details_safe(person_id):
    """
    单个查询函数，增加了简单的重试逻辑
    """
    url = f"https://api.themoviedb.org/3/person/{person_id}"
    params = {"api_key": API_KEY}
    try:
        res = requests.get(url, params=params, timeout=5)
        if res.status_code == 200:
            data = res.json()
            b_day = data.get('birthday')
            d_day = data.get('deathday')
            born = int(b_day[:4]) if b_day else None
            died = int(d_day[:4]) if d_day else None
            return (person_id, {"born": born, "died": died}) # 返回元组
            
        elif res.status_code == 429:
            # 如果被限流，稍微睡一下并返回 None (让主程序决定是否重试，这里简化为放弃)
            time.sleep(1)
            return (person_id, None) 
            
    except Exception:
        pass
    return (person_id, {"born": None, "died": None}) # 出错当做空处理

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(current_dir, OUTPUT_FILE)
    
    # --- 阶段 1: 扫描文件，筛选核心 ID ---
    print("扫描文件，筛选 [导演] 和 [前4位主演]...")
    target_person_ids = set()
    
    for year in range(START_YEAR, END_YEAR + 1):
        file_name = FILE_PATTERN.format(year)
        file_path = os.path.join(current_dir, '../raw_data', file_name)
        
        if not os.path.exists(file_path): continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            movies = json.load(f)
            
        for m in movies:
            credits = m.get('credits', {})
            
            # 1. 筛选演员：只取列表里的前 n 个
            # 注意：我们的 raw_data 已经是 cast[:10] 了，这里再切片一次 cast[:4]
            current_cast = credits.get('cast', [])
            for p in current_cast[:MAX_CAST_ORDER]:
                if p.get('id'): target_person_ids.add(p['id'])
            
            # 2. 筛选导演：全部保留
            # 兼容不同版本的 key ('directors' 或 'crew')
            directors = credits.get('directors', []) or [x for x in credits.get('crew', []) if x.get('job') == 'Director']
            for p in directors:
                if p.get('id'): target_person_ids.add(p['id'])

    total_people = len(target_person_ids)
    print(f"筛选完毕！目标人数 {total_people} 人。")
    print(f"启动 {MAX_WORKERS} 线程并发查询")

    # --- 阶段 2: 多线程并发查询 ---
    people_db = {}
    
    # 如果有旧文件，先加载（断点续传）
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                people_db = json.load(f)
            print(f"  📂 已加载 {len(people_db)} 条历史数据。")
        except: pass

    # 找出还没查的 ID
    ids_to_fetch = [pid for pid in target_person_ids if str(pid) not in people_db]
    
    if not ids_to_fetch:
        print("🎉 所有数据已存在，无需查询！")
        return

    count = 0
    total = len(ids_to_fetch)

    # 使用 ThreadPoolExecutor 进行并发
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_id = {executor.submit(get_person_details_safe, pid): pid for pid in ids_to_fetch}
        
        for future in concurrent.futures.as_completed(future_to_id):
            pid, result = future.result()
            count += 1
            
            if result:
                people_db[str(pid)] = result
            
            # 打印进度 (每10个打印一次，防止刷屏太快)
            if count % 10 == 0 or count == total:
                print(f"[{count}/{total}] 已处理... (当前库大小: {len(people_db)})", end="\r")

            # 定期保存 (每200个)
            if count % 200 == 0:
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(people_db, f, indent=0)

    # 最后保存
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(people_db, f)
        
    print(f"\n\n完成！核心人员的 born/died 数据已保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()