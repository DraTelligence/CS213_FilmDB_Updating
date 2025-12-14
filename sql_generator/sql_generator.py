import json
import os
import csv

# --- 📁 配置区 ---
# 1. 输入数据
MOVIE_FILE_PATTERN = '../raw_data/raw_movies_data_{}.json' 
PEOPLE_FILE = '../raw_data/people_details_map.json'
START_YEAR = 2019
END_YEAR = 2025

# 2. "旧账"文件 (导出自数据库)
EXISTING_PEOPLE_CSV = '../original_data/existing_people.csv'
EXISTING_MOVIES_CSV = '../original_data/existing_movies.csv'

# 3. 输出 SQL
OUTPUT_SQL = '../clean_sql/update_filmdb_final.sql'

# 4. ID 计数器起点 (用于新人/新电影)
NEXT_MOVIE_ID_START = 9210
NEXT_PEOPLE_ID_START = 16510

# 5. 策略
MAX_CAST_COUNT = 4
COUNTRY_MAP = {
    "US": "us",  # It Chapter Two
    "FR": "fr",  # La troupe à Palmade s'amuse avec Isabelle Nanty
    "DE": "de",  # Zerschunden - Ein Fall für Dr. Abel
    "GB": "gb",  # A Shaun the Sheep Movie: Farmageddon
    "IT": "it",  # Se c’è un aldilà sono fottuto - Vita e cinema di Claudio Caligari
    "CA": "ca",  # Dragged Across Concrete
    "ES": "sp",  # 缺失国家: Father There Is Only One
    "JP": "jp",  # Fate/stay night: Heaven's Feel II. Lost Butterfly
    "ID": "id",  # Horas Amang: Tiga Bulan untuk Selamanya
    "BR": "br",  # M8 - When Death Rescues Life
    "IN": "in",  # Article 15
    "PH": "ph",  # S.O.N.S. (Sons Of Nanay Sabel)
    "KR": "kr",  # The Legendary Lighter
    "BE": "be",  # A Good Woman Is Hard to Find
    "MX": "mx",  # The House of Flowers Presents: The Funeral
    "CN": "cn",  # Ne Zha
    "AU": "au",  # Dora and the Lost City of Gold
    "PL": "pl",  # How I Became a Gangster
    "AT": "at",  # How I Taught Myself to Be a Child
    "SE": "se",  # A Piece of My Heart
    "AR": "ar",  # Me, Myself and My Dead Wife
    "NO": "no",  # Forgotten Christmas
    "NL": "nl",  # Penoza: The Final Chapter
    "PT": "pt",  # An Indian in War - Life and Work of António-Pedro Vasconcelos
    "FI": "fi",  # Iron Sky: The Coming Race
    "DK": "dk",  # Queen of Hearts
    "TH": "th",  # Lost and Found: Billkin & PP Krit First Worldwide Digital Performance
    "PE": "pe",  # Django: En el nombre del hijo
    "CL": "cl",  # Nobody Knows I'm Here
    "VN": "vn",  # The Third Wife
    "CZ": "cz",  # Deadtown aneb Cesta tam a zase zpátky
    "IR": "ir",  # Learning to Skateboard in a Warzone (If You're a Girl)
    "RU": "ru",  # Horse Julius on the Throne and Three Heroes
    "CH": "ch",  # Continental Drift (South)
    "TR": "tr",  # Ela and Hilmi with Ali
    "CO": "co",  # My Cousin the Sexologist 2
    "DM": "dm",  # In the Arms of an Assassin
    "EC": "ec",  # Dedicated to my ex
    "IE": "ie",  # The Hole in the Ground
    "IL": "il",  # 'Til Kingdom Come
    "KZ": "kz",  # Bullets of Justice
    "NZ": "nz",  # Shadow in the Cloud
    "HN": "hn",  # Kelas Bintang - Mangga Muda
    "BG": "bg",  # Working Class Goes to Hell
    "EE": "ee",  # A Greyhound of a Girl
    "HU": "hu",  # Rendszerhiba - A magyar film el nem mondott története
    "RO": "ro",  # Do Not Expect Too Much from the End of the World
    "HK": "hk",  # The Prosecutor
    "LV": "lv",  # Flow
    "DO": "do",  # Captain Avispa
    "GR": "gr",  # The Return
    "SA": "sa",  # The Fakenapping
}

# --- 🛠️ 辅助函数 ---
def safe_str(text):
    if text is None: return "NULL"
    clean = str(text).replace("'", "''")
    return f"'{clean}'"

def resolve_country(iso_code):
    if not iso_code: return "'us'" 
    code = COUNTRY_MAP.get(iso_code, iso_code.lower())
    return f"'{code}'"

def split_name(fullname):
    """拆分姓名，用于比对"""
    if not fullname: return None, None
    parts = fullname.strip().split()
    if len(parts) == 1:
        return parts[0], "" # Surname 为空字符串而不是 NULL，方便字典比对
    surname = parts[-1]
    firstname = " ".join(parts[:-1])
    return firstname, surname

def get_gender_char(tmdb_gender):
    if tmdb_gender == 1: return 'F'
    if tmdb_gender == 2: return 'M'
    return '?'

# --- 📚 查重字典构建 ---
def load_existing_data():
    """读取 CSV 构建内存查找表"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    p_path = os.path.join(current_dir, EXISTING_PEOPLE_CSV)
    m_path = os.path.join(current_dir, EXISTING_MOVIES_CSV)
    
    # key: (first_name, surname) -> value: peopleid
    existing_people = {}
    # key: (title, year) -> value: movieid
    existing_movies = {}

    print("📚 正在加载旧数据库索引...")

    # 1. 加载人
    if os.path.exists(p_path):
        with open(p_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 3: continue
                # 假设 CSV 无表头，或是跳过表头逻辑 (这里简单判定如果ID不是数字就跳过)
                if not row[0].isdigit(): continue
                
                pid = int(row[0])
                first = row[1].strip()
                surname = row[2].strip()
                # 存入字典 (注意：为了匹配宽容度，可以转小写对比，这里先保持原样)
                existing_people[(first, surname)] = pid
    else:
        print("⚠️ 未找到 existing_people.csv，将无法去重人员！")

    # 2. 加载电影
    if os.path.exists(m_path):
        with open(m_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 3: continue
                if not row[0].isdigit(): continue
                
                mid = int(row[0])
                title = row[1].strip()
                year = int(row[2]) if row[2].isdigit() else 0
                existing_movies[(title, year)] = mid
    else:
        print("⚠️ 未找到 existing_movies.csv，将无法去重电影！")
        
    print(f"✅ 索引加载完毕: 现有人员 {len(existing_people)}, 现有电影 {len(existing_movies)}")
    return existing_people, existing_movies

# --- 🚀 主程序 ---
def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(current_dir, OUTPUT_SQL)
    people_path = os.path.join(current_dir, PEOPLE_FILE)

    # 1. 加载辅助数据
    people_details = {}
    if os.path.exists(people_path):
        with open(people_path, 'r', encoding='utf-8') as f:
            people_details = json.load(f)
            
    # 2. 加载查重字典
    db_people_map, db_movie_map = load_existing_data()
    
    # 3. 初始化 ID 计数器 (全局)
    curr_movie_id = NEXT_MOVIE_ID_START
    curr_people_id = NEXT_PEOPLE_ID_START
    
    # TMDB ID -> DB ID 的本次运行映射 (防止本次生成的数据内部重复)
    tmdb_to_db_people_cache = {} 
    
    print(f"✍️ 正在生成去重后的 SQL -> {OUTPUT_SQL}")
    
    with open(output_path, 'w', encoding='utf-8') as sql:
        sql.write("BEGIN;\n\n")
        
        stats = {"skipped_movies": 0, "new_movies": 0, "old_people_used": 0, "new_people_added": 0}

        for year in range(START_YEAR, END_YEAR + 1):
            file_name = MOVIE_FILE_PATTERN.format(year)
            file_path = os.path.join(current_dir, file_name)
            if not os.path.exists(file_path): continue
            
            with open(file_path, 'r', encoding='utf-8') as f:
                movies_data = json.load(f)
                
            print(f"  📂 处理 {year} ...")
            
            for m in movies_data:
                title = m.get('title')
                r_date = m.get('release_date', '')
                r_year = int(r_date.split('-')[0]) if r_date else year
                
                # --- 🛑 电影去重检查 ---
                # 如果 (标题, 年份) 已经在现有数据库里，直接跳过整部电影
                # (或者你可以选择只更新credits，但通常直接跳过更安全)
                if (title, r_year) in db_movie_map:
                    stats["skipped_movies"] += 1
                    # print(f"    跳过已存在电影: {title}")
                    continue

                # 是新电影，分配新 ID
                new_movie_id = curr_movie_id
                curr_movie_id += 1
                stats["new_movies"] += 1
                
                runtime = m.get('runtime', 0)
                countries = m.get('origin_country', [])
                c_code = countries[0] if countries else 'US'
                
                # 写入 Movies 表
                sql.write(f"-- Movie: {title} (ID: {new_movie_id})\n")
                sql.write(
                    f"INSERT INTO movies (movieid, title, country, year_released, runtime) "
                    f"VALUES ({new_movie_id}, {safe_str(title)}, {resolve_country(c_code)}, {r_year}, {runtime});\n"
                )
                
                # --- 处理人员 ---
                credits = m.get('credits', {})
                directors = credits.get('directors', []) 
                if not directors: directors = [x for x in credits.get('crew', []) if x.get('job') == 'Director']
                cast = credits.get('cast', [])[:MAX_CAST_COUNT]
                
                person_list = []
                for p in directors: person_list.append((p, 'D'))
                for p in cast: person_list.append((p, 'A'))
                
                # 本片内部去重
                movie_people_processed = set()
                
                for person, role_code in person_list:
                    tmdb_p_id = person.get('id')
                    if not tmdb_p_id: continue
                    tmdb_p_id_str = str(tmdb_p_id)
                    
                    p_name = person.get('name')
                    first, surname = split_name(p_name)
                    if first is None: continue # 名字有问题
                    
                    # --- 🛑 人员去重核心逻辑 ---
                    final_people_id = None
                    is_new_person_to_insert = False
                    
                    # 1. 检查本次运行缓存 (是否刚才在这批 JSON 里遇到过他)
                    if tmdb_p_id in tmdb_to_db_people_cache:
                        final_people_id = tmdb_to_db_people_cache[tmdb_p_id]
                    
                    # 2. 检查旧数据库 (是否是老演员)
                    elif (first, surname) in db_people_map:
                        final_people_id = db_people_map[(first, surname)]
                        # 记录到缓存，下次遇到直接用
                        tmdb_to_db_people_cache[tmdb_p_id] = final_people_id
                        stats["old_people_used"] += 1
                        
                    # 3. 确实是新人
                    else:
                        final_people_id = curr_people_id
                        tmdb_to_db_people_cache[tmdb_p_id] = final_people_id
                        # 更新一下 db_map，防止这批数据里有两个不同 TMDB_ID 但名字一样的人(少见但防万一)
                        db_people_map[(first, surname)] = final_people_id
                        
                        curr_people_id += 1
                        is_new_person_to_insert = True
                        stats["new_people_added"] += 1

                    # --- 只有新人，才生成 INSERT INTO people ---
                    if is_new_person_to_insert:
                        # 补全生日
                        detail = people_details.get(tmdb_p_id_str, {})
                        born = detail.get('born')
                        died = detail.get('died')
                        
                        # NULL 处理 (born强制填0, died允许NULL)
                        born_val = 0 if born is None else int(born)
                        died_val = 'NULL' if died is None else int(died)
                        
                        gender = get_gender_char(person.get('gender'))
                        
                        # 注意 surname 为空时，SQL里要写 ''
                        surname_sql = "''" if surname == "" else safe_str(surname)

                        sql.write(
                            f"INSERT INTO people (peopleid, first_name, surname, born, died, gender) "
                            f"VALUES ({final_people_id}, {safe_str(first)}, {surname_sql}, {born_val}, {died_val}, '{gender}');\n"
                        )
                    
                    # --- 写入 Credits (不管新人旧人，只要参演了这部新电影就要写) ---
                    # 联合主键防重
                    unique_key = (final_people_id, role_code)
                    if unique_key not in movie_people_processed:
                        sql.write(
                            f"INSERT INTO credits (movieid, peopleid, credited_as) "
                            f"VALUES ({new_movie_id}, {final_people_id}, '{role_code}');\n"
                        )
                        movie_people_processed.add(unique_key)

                sql.write("\n")
        
        # 结尾：还是保留 ROLLBACK 供测试，或者改 COMMIT
        sql.write("\n-- COMMIT; \nROLLBACK;\n") 
        
        print("-" * 30)
        print("📊 统计结果:")
        print(f"  跳过已存电影: {stats['skipped_movies']}")
        print(f"  新增电影:     {stats['new_movies']}")
        print(f"  复用原有演员: {stats['old_people_used']} 次")
        print(f"  新增演员:     {stats['new_people_added']} 人")
        print(f"✅ SQL 生成完毕: {OUTPUT_SQL}")

if __name__ == "__main__":
    main()