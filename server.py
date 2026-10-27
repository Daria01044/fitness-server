from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Header, Depends
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from supabase import create_client, Client
import json
import os
import psycopg2
import shutil
import subprocess
import hmac
import time
import threading
users_lock = threading.RLock()
try:
    import imageio_ffmpeg
    HAS_FFMPEG = True
except ImportError as e:
    HAS_FFMPEG = False
    print(f"[WARNING] imageio-ffmpeg не установлен: {e}")
from datetime import datetime, timedelta
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi import Body

ADMIN_KEY = os.environ.get("ADMIN_KEY")
ADMIN_PIN = os.environ.get("ADMIN_PIN")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_BUCKET = "fitness"

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("[STORAGE] Supabase Storage подключен")
else:
    print("[STORAGE] ВНИМАНИЕ: SUPABASE_URL/KEY не заданы, файлы будут на диске")

def verify_admin(x_admin_key: str = Header(None)):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden: admin key required")

PROTECTED_FIELDS = {"id", "device_id"}

# Хранилище активных подключений: {user_id: websocket}
active_connections: Dict[int, WebSocket] = {}

# Хранилище сообщений: {user_id: [{"from": "user"/"admin", "text": "...", "time": "..."}]}
chat_messages: Dict[int, list] = {}

app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi.staticfiles import StaticFiles

# Папка для хранения данных на сервере (Amvera сохраняет её между перезапусками)
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(DATA_DIR, "data.json")
USERS_FILE = os.path.join(DATA_DIR, "users.json")

UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

DATABASE_URL = os.environ.get("DATABASE_URL")
print(f"[DB] DATABASE_URL={'ЕСТЬ' if DATABASE_URL else 'НЕТ (пусто)'}")
print(f"[DB] USE_DB={bool(DATABASE_URL)}")

if not DATABASE_URL:
    print("[DB ERROR] Переменная DATABASE_URL не задана! Данные будут в файлах.")

USE_DB = bool(DATABASE_URL)


def _db_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def kv_get(name, default=None):
    conn = _db_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("select value from kv where name = %s", (name,))
            row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else default


def kv_set(name, value):
    conn = _db_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "insert into kv (name, value) values (%s, %s::jsonb) "
                "on conflict (name) do update set value = excluded.value",
                (name, json.dumps(value, ensure_ascii=False)),
            )
    finally:
        conn.close()


def _file_path(name):
    return os.path.join(DATA_DIR, f"{name}.json")


def store_load(name, default):
    if USE_DB:
        return kv_get(name, default)
    path = _file_path(name)
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def store_save(name, value):
    if USE_DB:
        kv_set(name, value)
        return
    tmp = _file_path(name) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _file_path(name))


def init_store():
    """Создаёт таблицу и переносит файлы из репозитория, если в базе их ещё нет."""
    if not USE_DB:
        return
    conn = _db_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("create table if not exists kv (name text primary key, value jsonb not null)")
    finally:
        conn.close()
    for name in ("data", "users", "exercises", "inventory"):
        path = _file_path(name)
        if os.path.exists(path) and kv_get(name, None) is None:
            with open(path, "r", encoding="utf-8") as f:
                kv_set(name, json.load(f))
            print(f"[INIT] {name} загружен из файла в базу")


def load_data():
    return store_load("data", {"intro_workout": {}, "blocks": []})

def save_data(data):
    store_save("data", data)

def load_users():
    return store_load("users", {"users": [], "current_user": None})

def save_users(users_data):
    store_save("users", users_data)

def load_exercises():
    return store_load("exercises", {"exercises": {}})

def save_exercises(data):
    store_save("exercises", data)

def load_inventory():
    return store_load("inventory", {"items": [], "note": ""})

def save_inventory(data):
    store_save("inventory", data)


try:
    init_store()
except Exception as e:
    print(f"[INIT ERROR] {e}")

def today_str():
    return datetime.now().strftime("%Y-%m-%d")


# --- Модели данных ---

class WorkoutData(BaseModel):
    intro_workout: Dict[str, Any]
    blocks: List[Dict[str, Any]]


class UserData(BaseModel):
    id: Optional[int] = None
    first_name: str
    last_name: str
    device_id: Optional[str] = None
    level: Optional[str] = None
    intro_done: bool = False
    current_block: Optional[int] = None
    current_index: int = 0
    done: Dict[str, bool] = {}
    comments: Dict[str, str] = {}
    done_dates: Dict[str, str] = {}
    start_date: Optional[str] = None
    last_payment_date: Optional[str] = None
    payment_due_date: Optional[str] = None
    is_active: bool = True
    payment_request: Optional[Dict[str, Any]] = None


# --- API ---

@app.get("/")
def read_root():
    return {"status": "ok", "service": "fitness-server"}

@app.get("/health")
def health():
    load_users()  # трогаем базу, чтобы Supabase не ушёл на паузу
    return {"status": "ok"}    

@app.get("/data")
def get_data():
    """Отдаёт тренировки (data.json)."""
    return load_data()


@app.get("/users")
def get_users():
    """Отдаёт всех пользователей."""
    return load_users()


@app.post("/users")
def update_users(users_data: Dict[str, Any], x_admin_key: Optional[str] = Header(None)):
    """Обновляет список пользователей (полная перезапись). Требует ключ."""
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    save_users(users_data)
    return {"status": "ok"}

@app.post("/user/register")
def register_user(user: UserData):
    device_id = (user.device_id or "").strip() or None

    with users_lock:
        users_data = load_users()
        users = users_data["users"]

        for u in users:
            if u["first_name"].lower() == user.first_name.lower() and u["last_name"].lower() == user.last_name.lower():
                existing_device = u.get("device_id")

                if not existing_device:
                    if device_id:
                        u["device_id"] = device_id
                        save_users(users_data)
                    return {"status": "existing", "user": u}

                if device_id and existing_device == device_id:
                    return {"status": "existing", "user": u}

                return {"status": "device_mismatch"}

        now = datetime.now()
        new_id = max([u["id"] for u in users], default=0) + 1
        new_user = {
            "id": new_id,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "device_id": device_id,
            "level": None,
            "intro_done": False,
            "current_block": None,
            "current_index": 0,
            "done": {},
            "comments": {},
            "done_dates": {},
            "start_date": now.strftime("%Y-%m-%d"),
            "last_payment_date": now.strftime("%Y-%m-%d"),
            "payment_due_date": (now + timedelta(days=7)).strftime("%Y-%m-%d"),
            "is_active": True,
            "payment_request": None,
        }
        users.append(new_user)
        save_users(users_data)
        return {"status": "created", "user": new_user}


ADMIN_ONLY_FIELDS = {"is_active", "payment_due_date", "last_payment_date", "start_date"}

def is_admin_request(key):
    return bool(ADMIN_KEY) and key == ADMIN_KEY

ADMIN_ONLY_FIELDS = {"is_active", "payment_due_date", "last_payment_date", "start_date"}

@app.post("/user/update")
def update_user(
    payload: Dict[str, Any] = Body(...),
    x_admin_key: Optional[str] = Header(None)
):
    uid = payload.get("id")
    if uid is None:
        raise HTTPException(status_code=400, detail="id required")

    is_admin = (x_admin_key == ADMIN_KEY)

    with users_lock:
        users_data = load_users()
        for u in users_data["users"]:
            if u["id"] == uid:
                for k, v in payload.items():
                    # Защищенные поля: id, device_id — никогда
                    if k in PROTECTED_FIELDS:
                        continue
                    # Админские поля: только с ключом
                    if k in ADMIN_ONLY_FIELDS and not is_admin:
                        continue
                    u[k] = v
                save_users(users_data)
                return {"status": "ok", "user": u}

    raise HTTPException(status_code=404, detail="User not found")

_login_fails = {"count": 0, "until": 0.0}

@app.post("/admin/login")
def admin_login(data: Dict[str, Any] = Body(...)):
    """Проверяет PIN-код администратора и отдаёт ключ."""
    now = time.time()
    if now < _login_fails["until"]:
        raise HTTPException(status_code=429, detail="Слишком много попыток, подождите")
    pin = str(data.get("pin", ""))
    if ADMIN_PIN and ADMIN_KEY and hmac.compare_digest(pin, ADMIN_PIN):
        _login_fails["count"] = 0
        return {"status": "ok", "key": ADMIN_KEY}
    _login_fails["count"] += 1
    if _login_fails["count"] >= 5:
        _login_fails["until"] = now + 300
        _login_fails["count"] = 0
    raise HTTPException(status_code=403, detail="Неверный PIN-код")
    
@app.websocket("/ws/chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: int):
    await websocket.accept()
    active_connections[user_id] = websocket

    # Отправляем историю сообщений
    history = chat_messages.get(user_id, [])
    for msg in history:
        await websocket.send_json(msg)

    try:
        while True:
            data = await websocket.receive_json()
            text = data.get("text", "").strip()
            if not text:
                continue

            msg = {
                "from": "user",
                "text": text,
                "time": today_str(),
            }
            chat_messages.setdefault(user_id, []).append(msg)

            # Отправляем подтверждение пользователю
            await websocket.send_json(msg)

            # Отправляем сообщение админу (если он подключён)
            if 0 in active_connections:
                try:
                    await active_connections[0].send_json({
                        "from": "user",
                        "user_id": user_id,
                        "text": text,
                        "time": today_str(),
                    })
                except Exception:
                    pass

    except WebSocketDisconnect:
        if user_id in active_connections:
            del active_connections[user_id]

@app.websocket("/ws/admin")
async def websocket_admin(websocket: WebSocket):
    await websocket.accept()
    active_connections[0] = websocket

    try:
        while True:
            data = await websocket.receive_json()
            user_id = data.get("user_id")
            text = data.get("text", "").strip()

            if not user_id or not text:
                continue

            msg = {
                "from": "admin",
                "text": text,
                "time": today_str(),
            }
            chat_messages.setdefault(user_id, []).append(msg)

            # Отправляем сообщение пользователю (если он онлайн)
            if user_id in active_connections:
                try:
                    await active_connections[user_id].send_json(msg)
                except Exception:
                    pass

    except WebSocketDisconnect:
        if 0 in active_connections:
            del active_connections[0]    
            
EXERCISES_FILE = os.path.join(DATA_DIR, "exercises.json")

@app.get("/exercises")
def get_exercises():
    return load_exercises()


@app.post("/exercises", dependencies=[Depends(verify_admin)])
def update_exercises(data: Dict[str, Any]):
    save_exercises(data)
    return {"status": "ok"}
    
@app.post("/workout/save", dependencies=[Depends(verify_admin)])
def save_workout(data: Dict[str, Any]):
    """Добавляет или заменяет тренировку в блоке по номеру."""
    block_id = data.get("block_id")
    number = data.get("number")
    text = data.get("text", "").strip()

    if not block_id or not number or not text:
        raise HTTPException(status_code=400, detail="Нужны block_id, number и text")

    workouts_data = load_data()
    blocks = workouts_data.get("blocks", [])

    for block in blocks:
        if block["id"] == block_id:
            workouts = block.get("workouts", [])

            # Ищем тренировку с таким номером
            existing = None
            for w in workouts:
                if w["number"] == number:
                    existing = w
                    break

            if existing:
                existing["text"] = text
                existing["video_url"] = data.get("video_url", "")
                existing["audio_url"] = data.get("audio_url", "")
                save_data(workouts_data)
                return {"status": "replaced", "workout": existing}
            else:
                # Добавляем новую
                new_id = block_id * 100 + number
                days = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница"]
                day = days[(number - 1) % 5]

                new_workout = {
                    "id": new_id,
                    "week": 1,
                    "day": day,
                    "number": number,
                    "text": text,
                    "video_url": data.get("video_url", ""),
                    "audio_url": data.get("audio_url", ""),
                }
                workouts.append(new_workout)
                # Сортируем по номеру
                workouts.sort(key=lambda x: x["number"])
                block["workouts"] = workouts
                save_data(workouts_data)
                return {"status": "created", "workout": new_workout}

    raise HTTPException(status_code=404, detail="Block not found")


@app.post("/workout/delete", dependencies=[Depends(verify_admin)])
def delete_workout(data: Dict[str, Any]):
    """Удаляет тренировку из блока по номеру."""
    block_id = data.get("block_id")
    number = data.get("number")

    if not block_id or not number:
        raise HTTPException(status_code=400, detail="Нужны block_id и number")

    workouts_data = load_data()
    blocks = workouts_data.get("blocks", [])

    for block in blocks:
        if block["id"] == block_id:
            workouts = block.get("workouts", [])
            block["workouts"] = [w for w in workouts if w["number"] != number]
            save_data(workouts_data)
            return {"status": "ok"}

    raise HTTPException(status_code=404, detail="Block not found")

@app.post("/block/add", dependencies=[Depends(verify_admin)])
def add_block():
    """Создаёт новый блок."""
    workouts_data = load_data()
    blocks = workouts_data.get("blocks", [])

    # Определяем новый id
    new_id = max([b["id"] for b in blocks], default=0) + 1

    # Блоки 1–8 — beginner, 9+ — advanced
    level = "beginner" if new_id <= 8 else "advanced"

    new_block = {
        "id": new_id,
        "level": level,
        "title": f"Блок {new_id}",
        "free": False,
        "image": "",
        "workouts": [],
    }

    blocks.append(new_block)
    workouts_data["blocks"] = blocks
    save_data(workouts_data)

    return {"status": "ok", "block": new_block}

@app.post("/block/delete", dependencies=[Depends(verify_admin)])
def delete_block(data: Dict[str, Any]):
    """Удаляет блок по id."""
    block_id = data.get("block_id")
    if not block_id:
        raise HTTPException(status_code=400, detail="Нужен block_id")

    workouts_data = load_data()
    blocks = workouts_data.get("blocks", [])

    blocks = [b for b in blocks if b["id"] != block_id]
    workouts_data["blocks"] = blocks
    save_data(workouts_data)

    return {"status": "ok"}
    
@app.post("/exercise/add", dependencies=[Depends(verify_admin)])
def add_exercise(data: Dict[str, Any]):
    """Добавляет новое упражнение с видео."""
    name = data.get("name", "").strip()
    video_url = data.get("video_url", "").strip()

    if not name:
        raise HTTPException(status_code=400, detail="Нужно название")

    key = name.lower().strip().replace(" ", "_")

    exercises_data = load_exercises()
    exercises = exercises_data.get("exercises", {})

    if key in exercises:
        raise HTTPException(status_code=400, detail="Упражнение с таким названием уже есть")

    exercises[key] = {
        "name": name,
        "video_url": video_url,
        "description": "",
    }

    exercises_data["exercises"] = exercises
    save_exercises(exercises_data)

    return {"status": "ok", "key": key}


@app.post("/exercise/delete", dependencies=[Depends(verify_admin)])
def delete_exercise(data: Dict[str, Any]):
    """Удаляет упражнение."""
    key = data.get("key", "").strip()

    if not key:
        raise HTTPException(status_code=400, detail="Нужен key")

    exercises_data = load_exercises()
    exercises = exercises_data.get("exercises", {})

    if key in exercises:
        del exercises[key]
        exercises_data["exercises"] = exercises
        save_exercises(exercises_data)
        return {"status": "ok"}

    raise HTTPException(status_code=404, detail="Упражнение не найдено")
    
@app.post("/intro/update", dependencies=[Depends(verify_admin)])
def update_intro(data: Dict[str, Any]):
    """Обновляет вводную тренировку."""
    workouts_data = load_data()
    
    if "intro_workout" not in workouts_data:
        workouts_data["intro_workout"] = {}
    
    intro = workouts_data["intro_workout"]
    
    if "text" in data:
        intro["text"] = data["text"]
    if "video_url" in data:
        intro["video_url"] = data["video_url"]
    if "audio_url" in data:
        intro["audio_url"] = data["audio_url"]
    if "title" in data:
        intro["title"] = data["title"]
    
    workouts_data["intro_workout"] = intro
    save_data(workouts_data)
    
    return {"status": "ok", "intro": intro} 
    
@app.post("/exercise/rename", dependencies=[Depends(verify_admin)])
def rename_exercise(data: Dict[str, Any]):
    """Переименовывает упражнение."""
    key = data.get("key", "").strip()
    new_name = data.get("new_name", "").strip()

    if not key or not new_name:
        raise HTTPException(status_code=400, detail="Нужны key и new_name")

    exercises_data = load_exercises()
    exercises = exercises_data.get("exercises", {})

    if key not in exercises:
        raise HTTPException(status_code=404, detail="Упражнение не найдено")

    exercises[key]["name"] = new_name
    exercises_data["exercises"] = exercises
    save_exercises(exercises_data)

    return {"status": "ok", "key": key, "new_name": new_name}    
    
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


@app.post("/upload/profile_photo")
async def upload_profile_photo(file: UploadFile = File(...), user_id: int = 0):
    """Загружает фото профиля (в Supabase Storage, с фолбэком на диск)."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Нет файла")
    
    ext = os.path.splitext(file.filename)[1] or ".jpg"
    filename = f"profiles/user_{user_id}{ext}"
    content = await file.read()
    
    # Пытаемся загрузить в Supabase
    public_url = None
    if supabase:
        try:
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=filename,
                file=content,
                file_options={"content-type": file.content_type or "image/jpeg", "upsert": "true"}
            )
            public_url = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(filename)
        except Exception as e:
            print(f"[STORAGE ERROR] upload_profile_photo: {e}")
    
    # Обновляем пользователя (сохраняем либо полный URL, либо локальный путь)
    with users_lock:
        users_data = load_users()
        for u in users_data["users"]:
            if u["id"] == user_id:
                u["profile_photo"] = public_url if public_url else f"/uploads/{filename}"
                break
        save_users(users_data)
    
    # Если Supabase не сработал — сохраняем на диск
    if not public_url:
        filepath = os.path.join(UPLOAD_DIR, filename)
        # Создаем папку, если её нет (для фолбэка)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)   # ← ДОБАВИТЬ ЭТО
        with open(filepath, "wb") as buffer:
            buffer.write(content)
        return {"status": "ok", "path": f"/uploads/{filename}"}
    
    return {"status": "ok", "path": public_url}

@app.post("/upload/exercise_video", dependencies=[Depends(verify_admin)])
async def upload_exercise_video(file: UploadFile = File(...), exercise_key: str = ""):
    """Загружает видео для упражнения (в Supabase Storage, с фолбэком на диск)."""
    if not file.filename or not exercise_key:
        raise HTTPException(status_code=400, detail="Нужен файл и exercise_key")

    ext = os.path.splitext(file.filename)[1].lower() or ".mp4"
    video_exts = [".mp4", ".mov", ".webm", ".avi"]
    if ext not in video_exts:
        raise HTTPException(status_code=400, detail="Неподдерживаемый формат")

    content = await file.read()
    
    # Имя файла в облаке (или на диске)
    filename = f"{exercise_key}{ext}"
    filename_in_cloud = f"videos/{filename}"
    content_type = "video/mp4" if ext == ".mp4" else "video/quicktime" if ext == ".mov" else "video/webm"
    
    # Пытаемся загрузить в Supabase
    public_url = upload_to_supabase(content, filename_in_cloud, content_type)
    
    if public_url:
        # Сохраняем полный URL в exercises.json
        exercises_data = load_exercises()
        exercises = exercises_data.get("exercises", {})
        if exercise_key in exercises:
            exercises[exercise_key]["video_file"] = public_url
            exercises_data["exercises"] = exercises
            save_exercises(exercises_data)
        return {"status": "ok", "path": public_url}
    else:
        # Фолбэк на диск
        videos_dir = os.path.join(UPLOAD_DIR, "videos")
        os.makedirs(videos_dir, exist_ok=True)
        filepath = os.path.join(videos_dir, filename)
        with open(filepath, "wb") as buffer:
            buffer.write(content)
        
        # Сохраняем локальный путь в exercises.json
        exercises_data = load_exercises()
        exercises = exercises_data.get("exercises", {})
        if exercise_key in exercises:
            exercises[exercise_key]["video_file"] = f"/uploads/videos/{filename}"
            exercises_data["exercises"] = exercises
            save_exercises(exercises_data)
        
        return {"status": "ok", "path": f"/uploads/videos/{filename}"}

def convert_to_mp4(input_path: str, output_path: str):
    if not HAS_FFMPEG:
        print("[FFMPEG ERROR] Нет imageio-ffmpeg")
        return False
    try:
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        cmd = [
            ffmpeg_exe,
            "-y",
            "-i", input_path,
            "-c", "copy",           # Копируем кодеки без перекодирования (БЫСТРО)
            "-movflags", "faststart",
            output_path
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        return True
    except subprocess.TimeoutExpired:
        print("[FFMPEG ERROR] Конвертация превысила 30 секунд")
        return False
    except Exception as e:
        print(f"[FFMPEG ERROR] {e}")
        return False

def upload_to_supabase(file_bytes: bytes, filename: str, content_type: str):
    """Загружает файл в Supabase Storage и возвращает публичный URL."""
    if not supabase:
        return None
    try:
        # Загружаем файл в bucket
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=filename,
            file=file_bytes,
            file_options={"content-type": content_type, "upsert": "true"}
        )
        # Получаем публичную ссылку
        public_url = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(filename)
        print(f"[STORAGE OK] {public_url}")
        return public_url
    except Exception as e:
        print(f"[STORAGE ERROR] {e}")
        return None
        
def upload_to_supabase(file_bytes: bytes, filename: str, content_type: str):
    """Загружает файл в Supabase Storage и возвращает публичный URL."""
    if not supabase:
        return None
    try:
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=filename,
            file=file_bytes,
            file_options={"content-type": content_type, "upsert": "true"}
        )
        return supabase.storage.from_(SUPABASE_BUCKET).get_public_url(filename)
    except Exception as e:
        print(f"[STORAGE ERROR] {e}")
        return None


@app.post("/upload/comment_media")
async def upload_comment_media(file: UploadFile = File(...), user_id: int = 0):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Нет файла")

    MAX_SIZE_MB = 50
    ext = os.path.splitext(file.filename)[1].lower() or ".jpg"
    image_exts = [".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic"]
    video_exts = [".mp4", ".mov", ".avi", ".webm", ".mkv", ".3gp"]

    if ext in image_exts:
        media_type = "image"
    elif ext in video_exts:
        media_type = "video"
    else:
        raise HTTPException(status_code=400, detail="Неподдерживаемый формат")

    content = await file.read()
    if len(content) > MAX_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"Файл больше {MAX_SIZE_MB} МБ")

    comments_dir = os.path.join(UPLOAD_DIR, "comments")
    os.makedirs(comments_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    
    if media_type == "image":
        filename = f"user_{user_id}_{timestamp}{ext}"
        filepath = os.path.join(comments_dir, filename)
        with open(filepath, "wb") as buffer:
            buffer.write(content)
    else:
        # Конвертируем видео
        temp_filename = f"temp_{user_id}_{timestamp}{ext}"
        temp_filepath = os.path.join(comments_dir, temp_filename)
        with open(temp_filepath, "wb") as buffer:
            buffer.write(content)

        filename = f"user_{user_id}_{timestamp}.mp4"
        filepath = os.path.join(comments_dir, filename)
        
        success = convert_to_mp4(temp_filepath, filepath)
        
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
            
        if not success:
            raise HTTPException(status_code=500, detail="Ошибка при конвертации видео")

    # Загружаем в Supabase
    with open(filepath, "rb") as f:
        file_bytes = f.read()
    
    filename_in_cloud = f"comments/{filename}"
    content_type = "video/mp4" if media_type == "video" else "image/jpeg"
    
    print(f"[DEBUG] Перед вызовом upload_to_supabase: {filename_in_cloud}")   # ← ДОБАВИТЬ
    public_url = upload_to_supabase(file_bytes, filename_in_cloud, content_type)
    print(f"[DEBUG] После upload_to_supabase: public_url={public_url}")       # ← ДОБАВИТЬ
    
    if public_url:
        # Удаляем локальный файл (он больше не нужен)
        if os.path.exists(filepath):
            os.remove(filepath)
        url_path = public_url
    else:
        url_path = f"/uploads/comments/{filename}"

    return {
        "status": "ok",
        "path": url_path,
        "media_type": media_type,
    }

@app.get("/uploads/{filename}")
async def get_upload(filename: str):
    """Отдаёт загруженный файл."""
    filepath = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(filepath)    
    
FOODS_FILE = os.path.join(DATA_DIR, "foods.json")

def load_foods():
    if not os.path.exists(FOODS_FILE):
        return {"foods": []}
    with open(FOODS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/foods")
def get_foods():
    return load_foods()

INVENTORY_FILE = os.path.join(DATA_DIR, "inventory.json")

@app.get("/inventory")
def get_inventory():
    return load_inventory()

@app.post("/inventory", dependencies=[Depends(verify_admin)])
def update_inventory(data: Dict[str, Any]):
    save_inventory(data)
    return {"status": "ok"}    
    
class SetCurrentBlockRequest(BaseModel):
    block_id: int


@app.post("/block/set_current", dependencies=[Depends(verify_admin)])
async def set_current_block(req: SetCurrentBlockRequest):
    data = load_data()
    for b in data.get("blocks", []):
        b["is_current"] = (b["id"] == req.block_id)
    save_data(data)
    return {"status": "ok"} 
    
class BonusWorkoutRequest(BaseModel):
    id: int = 0
    title: str = ""
    text: str = ""
    video_url: str = ""
    audio_url: str = ""


@app.post("/bonus/save", dependencies=[Depends(verify_admin)])
async def save_bonus_workout(req: BonusWorkoutRequest):
    data = load_data()
    bonus = data.setdefault("bonus_workouts", [])

    if req.id:
        # Редактирование
        for bw in bonus:
            if bw["id"] == req.id:
                bw["title"] = req.title
                bw["text"] = req.text
                bw["video_url"] = req.video_url
                bw["audio_url"] = req.audio_url
                break
    else:
        # Новая
        new_id = max([bw.get("id", 0) for bw in bonus], default=9000) + 1
        bonus.append({
            "id": new_id,
            "title": req.title,
            "text": req.text,
            "video_url": req.video_url,
            "audio_url": req.audio_url,
        })

    save_data(data)
    return {"status": "ok"}


@app.post("/bonus/delete", dependencies=[Depends(verify_admin)])
async def delete_bonus_workout(req: dict):
    data = load_data()
    bonus = data.get("bonus_workouts", [])
    bid = req.get("id")
    bonus = [bw for bw in bonus if bw.get("id") != bid]
    data["bonus_workouts"] = bonus
    save_data(data)
    return {"status": "ok"}    

@app.delete("/user/{user_id}", dependencies=[Depends(verify_admin)])
def delete_user_endpoint(user_id: int):
    with users_lock:
        users_data = load_users()
        before = len(users_data["users"])
        users_data["users"] = [u for u in users_data["users"] if u["id"] != user_id]
        if len(users_data["users"]) == before:
            raise HTTPException(status_code=404, detail="User not found")
        save_users(users_data)
    return {"status": "deleted"}


@app.post("/user/{user_id}/reset_device", dependencies=[Depends(verify_admin)])
def reset_device(user_id: int):
    with users_lock:
        users_data = load_users()
        for u in users_data["users"]:
            if u["id"] == user_id:
                u["device_id"] = None
                save_users(users_data)
                return {"status": "ok"}
    raise HTTPException(status_code=404, detail="User not found")
    