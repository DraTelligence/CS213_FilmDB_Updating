import json
import os
import collections

# --- 📁 配置区 ---
RAW_DATA_DIR = '../raw_data'
FILE_PATTERN = 'raw_movies_data_{}.json'
YEARS = range(2019, 2026)

# 你的数据库代码文件 (CSV格式)
# 假设内容格式为: Code,Name (例如: GB,United Kingdom)
DB_CODES_FILE = '../original_data/country_code.csv'

def load_db_codes():
    """读取 CSV 文件"""
    codes = set()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, DB_CODES_FILE)
    
    if not os.path.exists(file_path):
        print(f"❌ 错误：找不到文件 {file_path}")
        return codes

    print(f"📂 正在读取数据库代码: {file_path} ...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',')
                if parts:
                    # 去除引号和空格，保留原样（即保留小写）
                    raw_code = parts[0].strip().strip('"').strip("'").strip()
                    if len(raw_code) == 2:
                        codes.add(raw_code)
    except Exception as e:
        print(f"❌ 出错: {e}")
        return codes

    print(f"✅ 加载成功！数据库有 {len(codes)} 个代码 (示例: {list(codes)[:3]})")
    return codes

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_codes = load_db_codes()
    
    tmdb_stats = collections.Counter()
    tmdb_example_map = {} 

    print("\n🚀 正在扫描 TMDB 数据...")
    for year in YEARS:
        file_name = FILE_PATTERN.format(year)
        path = os.path.join(current_dir, RAW_DATA_DIR, file_name)
        if not os.path.exists(path): continue
        with open(path, 'r', encoding='utf-8') as f:
            for m in json.load(f):
                countries = m.get('origin_country', [])
                if countries:
                    code = countries[0] # TMDB 这里的 code 通常是大写
                    tmdb_stats[code] += 1
                    if code not in tmdb_example_map:
                        tmdb_example_map[code] = m.get('title')

    # --- 比对逻辑 (核心修改) ---
    missing_codes = []
    
    print("\n" + "="*60)
    print(f"{'TMDB':<6} | {'DB现状':<8} | {'分析与建议'}")
    print("-" * 60)
    
    for code, count in tmdb_stats.most_common():
        # code 是 TMDB 的大写代码 (如 'US')
        
        # 1. 完美匹配 (数据库里也是大写)
        if code in db_codes:
            continue
            
        # 2. 大小写匹配 (数据库里是小写 'us')
        elif code.lower() in db_codes:
            note = f"⚠️ 建议映射: {code} -> {code.lower()}"
            missing_codes.append((code, note, tmdb_example_map.get(code)))
            print(f"{code:<6} | {code.lower():<8} | {note}")
            
        # 3. 特殊别名匹配 (检测常见不一致)
        # 即使你的是小写，我们也尝试匹配一下常见映射
        elif code == 'GB' and 'uk' in db_codes:
            note = "⚠️ 建议映射: GB -> uk"
            missing_codes.append((code, note, tmdb_example_map.get(code)))
            print(f"{code:<6} | {'uk':<8} | {note}")
            
        elif code == 'US' and 'usa' in db_codes:
            note = "⚠️ 建议映射: US -> usa"
            missing_codes.append((code, note, tmdb_example_map.get(code)))
            print(f"{code:<6} | {'usa':<8} | {note}")

        # 4. 彻底缺失
        else:
            note = f"❓ 确实缺失 (建议映射为 {code.lower()})"
            missing_codes.append((code, note, tmdb_example_map.get(code)))
            print(f"{code:<6} | {'NONE':<8} | {note}")

    print("="*60)
    
    # --- 生成字典 ---
    print("\n💡 请将以下字典更新到 generate_sql_final.py 的 COUNTRY_MAP 中：")
    print("-" * 20)
    print("COUNTRY_MAP = {")
    
    for code, note, example in missing_codes:
        if "->" in note:
            # 提取建议的目标值 (例如 'us' 或 'uk')
            target = note.split("->")[1].strip()
            print(f'    "{code}": "{target}",  # {example}')
        else:
            # 确实缺失的，建议映射成小写，假设你会插入新国家
            print(f'    "{code}": "{code.lower()}",  # 缺失国家: {example}')
            
    print("}")
    print("-" * 20)

if __name__ == "__main__":
    main()