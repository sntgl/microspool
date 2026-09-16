#!/usr/bin/env python3
"""
Тесты microspool.

Запуск:
    uv run --with tornado python3 test_microspool.py

Стартует нативный бинарник ./microspool (make native) на случайном порту во
временном каталоге и прогоняет: CRUD vendor/filament/spool и вложенность,
X-Total-Count, allow_archived, коды ошибок 400/404/405/411/413, формулы
веса/длины и use_length/use_weight (допуск 1e-3), персистентность (SIGTERM →
перезапуск, отложенный сброс use, битый файл → код 1 и файл не тронут),
WebSocket через tornado (ping/pong живучесть, событие deleted, use через
AsyncHTTPClient), лимит соединений и отсутствие утечки RSS на 2000 запросах,
встроенный веб-интерфейс (gzip / 406 / -U), /api/v1/setting/currency,
/api/v1/material, /api/v1/location, /api/v1/lot-number.
"""
import asyncio
import gzip
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error

BINARY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "microspool")

FAILURES = []
PASSES = 0


def check(name, cond, detail=""):
    global PASSES
    if cond:
        PASSES += 1
        print(f"[PASS] {name}")
    else:
        FAILURES.append(name)
        print(f"[FAIL] {name}" + (f" — {detail}" if detail else ""))


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------
# Управление сервером
# ---------------------------------------------------------------------------

class Server:
    def __init__(self, datafile=None, tmpdir=None):
        self.tmpdir = tmpdir or tempfile.mkdtemp(prefix="microspool_test_")
        self.datafile = datafile or os.path.join(self.tmpdir, "microspool.json")
        self.port = free_port()
        self.proc = None

    def start(self, wait_ready=True, extra_args=None):
        args = [BINARY, "-l", "127.0.0.1", "-p", str(self.port), "-d", self.datafile]
        if extra_args:
            args += extra_args
        self.proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if wait_ready:
            self.wait_ready()
        return self

    def wait_ready(self, timeout=10.0):
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out, err = self.proc.communicate()
                raise RuntimeError(f"сервер завершился при старте: code={self.proc.returncode} stderr={err.decode(errors='replace')}")
            try:
                status, _, _ = http_json(self.base_url(), "GET", "/api/v1/health")
                if status == 200:
                    return
            except Exception as e:
                last_err = e
            time.sleep(0.02)
        raise RuntimeError(f"сервер не поднялся за {timeout}s: {last_err}")

    def base_url(self):
        return f"http://127.0.0.1:{self.port}"

    def stop(self, sig=signal.SIGTERM, timeout=3.0):
        if self.proc is None or self.proc.poll() is not None:
            return self.proc.returncode if self.proc else None
        self.proc.send_signal(sig)
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        return self.proc.returncode

    def cleanup(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        shutil.rmtree(self.tmpdir, ignore_errors=True)


def http_json(base, method, path, body=None, headers=None):
    url = base + path
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read()
            j = json.loads(raw) if raw else None
            return resp.status, j, dict(resp.headers)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            j = json.loads(raw) if raw else None
        except Exception:
            j = None
        return e.code, j, dict(e.headers)


def raw_http(port, method, path, headers, body=b"", read_timeout=3.0):
    """Полностью ручной HTTP-запрос — для тестов 411/413/некорректных заголовков."""
    s = socket.create_connection(("127.0.0.1", port), timeout=read_timeout)
    lines = [f"{method} {path} HTTP/1.1", "Host: 127.0.0.1"]
    for k, v in headers.items():
        lines.append(f"{k}: {v}")
    lines.append("")
    lines.append("")
    head = "\r\n".join(lines).encode()
    s.sendall(head)
    if body:
        s.sendall(body)
    s.settimeout(read_timeout)
    data = b""
    try:
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    except (socket.timeout, ConnectionResetError):
        pass
    s.close()
    return data


def parse_status(raw_response):
    line = raw_response.split(b"\r\n", 1)[0].decode(errors="replace")
    parts = line.split(" ", 2)
    return int(parts[1]) if len(parts) >= 2 else None


def length_mm(weight_g, density, diameter_mm):
    r = diameter_mm / 2.0
    denom = density * math.pi * r * r / 1000.0
    return weight_g / denom


# ---------------------------------------------------------------------------
# CRUD и вложенность
# ---------------------------------------------------------------------------

def test_vendor_crud(base):
    status, body, _ = http_json(base, "POST", "/api/v1/vendor", {})
    check("vendor: POST без name -> 400", status == 400, f"status={status} body={body}")

    status, v, _ = http_json(base, "POST", "/api/v1/vendor", {
        "name": "Acme", "comment": "тестовый вендор", "empty_spool_weight": 212.5,
        "external_id": "ext-1"
    })
    check("vendor: POST валидный -> 200", status == 200, f"status={status} body={v}")
    check("vendor: поля в ответе", v and v.get("name") == "Acme" and v.get("comment") == "тестовый вендор"
          and v.get("empty_spool_weight") == 212.5 and v.get("external_id") == "ext-1"
          and "extra" in v and v["extra"] == {} and "registered" in v and v.get("id") == 1)

    status, lst, hdrs = http_json(base, "GET", "/api/v1/vendor")
    check("vendor: GET list -> 200", status == 200)
    check("vendor: X-Total-Count == 1", hdrs.get("X-Total-Count") == "1", str(hdrs))
    check("vendor: список содержит созданный", len(lst) == 1 and lst[0]["id"] == v["id"])

    status, got, _ = http_json(base, "GET", f"/api/v1/vendor/{v['id']}")
    check("vendor: GET by id -> 200", status == 200 and got["name"] == "Acme")

    status, _, _ = http_json(base, "GET", "/api/v1/vendor/99999")
    check("vendor: GET несуществующего -> 404", status == 404)

    status, patched, _ = http_json(base, "PATCH", f"/api/v1/vendor/{v['id']}", {"comment": "новый комментарий"})
    check("vendor: PATCH -> 200", status == 200)
    check("vendor: PATCH обновил только comment", patched["comment"] == "новый комментарий" and patched["name"] == "Acme")

    status, v2, _ = http_json(base, "POST", "/api/v1/vendor", {"name": "Другой"})
    check("vendor: второй POST -> 200, новый id", status == 200 and v2["id"] == v["id"] + 1)

    status, deleted, _ = http_json(base, "DELETE", f"/api/v1/vendor/{v2['id']}")
    check("vendor: DELETE -> 200", status == 200 and deleted["id"] == v2["id"])

    status, _, _ = http_json(base, "GET", f"/api/v1/vendor/{v2['id']}")
    check("vendor: после DELETE -> 404", status == 404)

    status, _, _ = http_json(base, "DELETE", "/api/v1/vendor/99999")
    check("vendor: DELETE несуществующего -> 404", status == 404)

    status, _, _ = http_json(base, "PUT", "/api/v1/vendor", {"name": "x"})
    check("vendor: PUT на /vendor -> 405", status == 405)

    return v["id"]


def test_filament_crud(base, vendor_id):
    status, body, _ = http_json(base, "POST", "/api/v1/filament", {"vendor_id": vendor_id})
    check("filament: POST без density/diameter -> 400", status == 400, f"{status} {body}")

    status, body, _ = http_json(base, "POST", "/api/v1/filament", {"density": -1, "diameter": 1.75})
    check("filament: density<=0 -> 400", status == 400)

    status, body, _ = http_json(base, "POST", "/api/v1/filament", {"vendor_id": 99999, "density": 1.24, "diameter": 1.75})
    check("filament: несуществующий vendor_id -> 400", status == 400, f"{status} {body}")

    status, f, _ = http_json(base, "POST", "/api/v1/filament", {
        "name": "PLA чёрный", "vendor_id": vendor_id, "material": "PLA",
        "density": 1.24, "diameter": 1.75, "weight": 1000, "spool_weight": 140,
        "color_hex": "1a2b3c", "settings_extruder_temp": 210, "settings_bed_temp": 55,
    })
    check("filament: POST валидный -> 200", status == 200, f"{status} {f}")
    check("filament: вложенный vendor", f.get("vendor", {}).get("id") == vendor_id and f["vendor"]["name"] == "Acme")
    check("filament: числовые поля", f["density"] == 1.24 and f["diameter"] == 1.75 and f["weight"] == 1000)

    status, body, _ = http_json(base, "POST", "/api/v1/filament", {"density": 1.24, "diameter": 1.75, "color_hex": "zzzzzz"})
    check("filament: некорректный color_hex -> 400", status == 400)

    status, lst, hdrs = http_json(base, "GET", "/api/v1/filament")
    check("filament: X-Total-Count", hdrs.get("X-Total-Count") == str(len(lst)))

    status, patched, _ = http_json(base, "PATCH", f"/api/v1/filament/{f['id']}", {"comment": "ok"})
    check("filament: PATCH -> 200", status == 200 and patched["comment"] == "ok")

    # каскад: удаление вендора обнуляет vendor_id у филамента
    status, v3, _ = http_json(base, "POST", "/api/v1/vendor", {"name": "Временный"})
    status, f3, _ = http_json(base, "POST", "/api/v1/filament", {"vendor_id": v3["id"], "density": 1.0, "diameter": 1.75})
    status, _, _ = http_json(base, "DELETE", f"/api/v1/vendor/{v3['id']}")
    status, f3b, _ = http_json(base, "GET", f"/api/v1/filament/{f3['id']}")
    check("filament: каскад удаления вендора -> vendor_id обнулён", "vendor" not in f3b, f3b)
    http_json(base, "DELETE", f"/api/v1/filament/{f3['id']}")

    return f["id"]


def test_spool_crud_and_weights(base, filament_id):
    density, diameter = 1.24, 1.75

    status, s, _ = http_json(base, "POST", "/api/v1/spool", {"filament_id": filament_id})
    check("spool: POST c filament_id -> 200", status == 200, f"{status} {s}")
    check("spool: initial_weight унаследован от filament.weight", s.get("initial_weight") == 1000, s)
    check("spool: used_weight по умолчанию 0", s.get("used_weight") == 0)
    check("spool: remaining_weight == initial_weight", s.get("remaining_weight") == 1000)
    exp_len = length_mm(1000, density, diameter)
    check("spool: remaining_length по формуле", abs(s.get("remaining_length", -1) - exp_len) < 1e-3,
          f"got={s.get('remaining_length')} exp={exp_len}")
    check("spool: used_length == 0", abs(s.get("used_length", -1) - 0) < 1e-3)
    check("spool: filament вложен", s["filament"]["id"] == filament_id)

    status, body, _ = http_json(base, "POST", "/api/v1/spool", {"filament_id": 99999})
    check("spool: несуществующий filament_id -> 400", status == 400)

    status, body, _ = http_json(base, "POST", "/api/v1/spool", {})
    check("spool: без filament_id -> 400", status == 400)

    status, patched, _ = http_json(base, "PATCH", f"/api/v1/spool/{s['id']}", {"remaining_weight": 800})
    check("spool: PATCH remaining_weight -> used_weight=200", status == 200 and abs(patched["used_weight"] - 200) < 1e-9, patched)

    status, body, _ = http_json(base, "PATCH", f"/api/v1/spool/{s['id']}", {"remaining_weight": 1, "used_weight": 2})
    check("spool: remaining_weight+used_weight одновременно -> 400", status == 400)

    status, used, _ = http_json(base, "PUT", f"/api/v1/spool/{s['id']}/use", {"use_weight": 50})
    check("spool: use_weight -> 200", status == 200)
    check("spool: use_weight применён (200+50=250)", abs(used["used_weight"] - 250) < 1e-3, used)
    check("spool: first_used/last_used выставлены", used.get("first_used") and used.get("last_used"))

    use_length_mm = 1000.0
    exp_delta = use_length_mm * (density * math.pi * (diameter / 2.0) ** 2 / 1000.0)
    status, used2, _ = http_json(base, "PUT", f"/api/v1/spool/{s['id']}/use", {"use_length": use_length_mm})
    check("spool: use_length -> 200", status == 200)
    check("spool: use_length -> used_weight по формуле", abs(used2["used_weight"] - (250 + exp_delta)) < 1e-3,
          f"got={used2['used_weight']} exp={250+exp_delta}")

    status, body, _ = http_json(base, "PUT", f"/api/v1/spool/{s['id']}/use", {"use_weight": 1, "use_length": 1})
    check("spool: use с обоими полями -> 400", status == 400)
    status, body, _ = http_json(base, "PUT", f"/api/v1/spool/{s['id']}/use", {})
    check("spool: use без полей -> 400", status == 400)

    # архивные катушки
    status, arch, _ = http_json(base, "POST", "/api/v1/spool", {"filament_id": filament_id, "archived": True})
    status, lst, hdrs = http_json(base, "GET", "/api/v1/spool")
    check("spool: список без allow_archived не содержит архивную", all(x["id"] != arch["id"] for x in lst))
    status, lst2, _ = http_json(base, "GET", "/api/v1/spool?allow_archived=true")
    check("spool: allow_archived=true содержит архивную", any(x["id"] == arch["id"] for x in lst2))
    status, lst3, _ = http_json(base, "GET", "/api/v1/spool?allow_archived=1")
    check("spool: allow_archived=1 содержит архивную", any(x["id"] == arch["id"] for x in lst3))
    status, lst4, hdrs4 = http_json(base, "GET", "/api/v1/spool?limit=1000")
    check("spool: ?limit=1000 игнорируется, но не ломает запрос", status == 200)

    status, deleted, _ = http_json(base, "DELETE", f"/api/v1/spool/{arch['id']}")
    check("spool: DELETE -> 200", status == 200 and deleted["id"] == arch["id"])

    # DELETE филамента, на который ссылается катушка -> 400
    status, body, _ = http_json(base, "DELETE", f"/api/v1/filament/{filament_id}")
    check("filament: DELETE при наличии ссылающейся катушки -> 400", status == 400, f"{status} {body}")

    return s["id"]


def test_errors_and_limits(base):
    status, body, _ = http_json(base, "POST", "/api/v1/vendor", {"name": 123})
    check("errors: неверный тип поля -> 400", status == 400)

    raw = raw_http(int(base.rsplit(":", 1)[1]), "POST", "/api/v1/vendor",
                    {"Content-Type": "application/json", "Content-Length": "10"},
                    body=b"{not json}")
    check("errors: битый JSON -> 400", parse_status(raw) == 400, raw[:80])

    status, _, _ = http_json(base, "GET", "/api/v1/nope")
    check("errors: неизвестный путь -> 404", status == 404)

    status, _, _ = http_json(base, "PUT", "/api/v1/filament", {})
    check("errors: неподдерживаемый метод -> 405", status == 405)

    port = int(base.rsplit(":", 1)[1])
    # Сервер отвечает 413 сразу по заголовку Content-Length, не дожидаясь тела —
    # само тело намеренно не отправляем (иначе ядро закрывает соединение через RST
    # из-за непрочитанных данных, и клиент не успевает прочитать корректный ответ).
    raw = raw_http(port, "POST", "/api/v1/vendor",
                    {"Content-Type": "application/json", "Content-Length": str(20 * 1024)},
                    body=b"")
    check("errors: тело > 16КБ -> 413", parse_status(raw) == 413, raw[:120])

    raw = raw_http(port, "POST", "/api/v1/vendor",
                    {"Content-Type": "application/json", "Transfer-Encoding": "chunked"},
                    body=b"")
    check("errors: chunked без Content-Length -> 411", parse_status(raw) == 411, raw[:120])

    status, arr, _ = http_json(base, "GET", "/api/v1/external/vendor")
    check("external/vendor -> []", status == 200 and arr == [])
    status, arr, _ = http_json(base, "GET", "/api/v1/external/filament")
    check("external/filament -> []", status == 200 and arr == [])

    status, info, _ = http_json(base, "GET", "/api/v1/info")
    check("info: version/db_type", status == 200 and info.get("version") == "0.22.1" and info.get("db_type") == "microspool")


# ---------------------------------------------------------------------------
# Встроенный веб-интерфейс, /api/v1/setting и списки material/location/lot-number
# ---------------------------------------------------------------------------

def test_ui():
    srv = Server()
    try:
        srv.start()
        port = srv.port

        raw = raw_http(port, "GET", "/", {"Accept-Encoding": "gzip"})
        status = parse_status(raw)
        check("ui: GET / с gzip -> 200", status == 200, raw[:150])
        head, _, body = raw.partition(b"\r\n\r\n")
        head_lower = head.lower()
        check("ui: Content-Encoding: gzip", b"content-encoding: gzip" in head_lower, head[:300])
        check("ui: Content-Type: text/html; charset=utf-8", b"content-type: text/html; charset=utf-8" in head_lower)
        check("ui: Cache-Control: no-cache", b"cache-control: no-cache" in head_lower)
        try:
            html = gzip.decompress(body)
        except Exception as e:
            html = b""
            check("ui: тело — валидный gzip", False, str(e))
        else:
            check("ui: тело — валидный gzip", len(html) > 0)
        check("ui: распакованный HTML содержит <title>", b"<title>" in html, html[:200])
        check("ui: похоже на HTML-документ", html.lstrip()[:15].lower() == b"<!doctype html>", html[:80])

        raw_idx = raw_http(port, "GET", "/index.html", {"Accept-Encoding": "gzip"})
        check("ui: GET /index.html с gzip -> 200", parse_status(raw_idx) == 200)

        raw_noae = raw_http(port, "GET", "/", {})
        check("ui: GET / без Accept-Encoding -> 406", parse_status(raw_noae) == 406, raw_noae[:150])

        status, _, _ = http_json(srv.base_url(), "POST", "/")
        check("ui: POST / -> 405", status == 405)
    finally:
        srv.cleanup()


def test_ui_disabled():
    srv = Server()
    try:
        srv.start(extra_args=["-U"])
        base = srv.base_url()
        status, _, _ = http_json(base, "GET", "/")
        check("ui: -U -> GET / -> 404", status == 404)
        status, _, _ = http_json(base, "GET", "/index.html")
        check("ui: -U -> GET /index.html -> 404", status == 404)
        # остальной API при -U по-прежнему работает
        status, _, _ = http_json(base, "GET", "/api/v1/health")
        check("ui: -U -> остальной API жив", status == 200)
    finally:
        srv.cleanup()


def test_ui_js_syntax():
    node = shutil.which("node")
    if not node:
        print("[SKIP] ui: node недоступен — синтаксис встроенного JS не проверен отдельным тестом")
        return
    ui_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui", "index.html")
    with open(ui_path, "r", encoding="utf-8") as f:
        html = f.read()
    m = re.search(r"<script>(.*?)</script>", html, re.S)
    check("ui: <script> найден в ui/index.html", m is not None)
    if not m:
        return
    tmp_js = os.path.join(tempfile.gettempdir(), "microspool_ui_check.js")
    with open(tmp_js, "w", encoding="utf-8") as f:
        f.write(m.group(1))
    proc = subprocess.run([node, "--check", tmp_js], capture_output=True)
    check("ui: встроенный JS проходит `node --check`", proc.returncode == 0,
          proc.stderr.decode(errors="replace"))


def test_settings_and_lists():
    srv = Server()
    try:
        srv.start()
        base = srv.base_url()

        status, body, _ = http_json(base, "GET", "/api/v1/setting/currency")
        check("setting/currency -> 200", status == 200)
        check("setting/currency: value/is_set/type", body == {"value": "\"EUR\"", "is_set": False, "type": "string"}, body)

        status, _, _ = http_json(base, "GET", "/api/v1/setting/other")
        check("setting/other -> 404", status == 404)
        status, _, _ = http_json(base, "GET", "/api/v1/setting/")
        check("setting/ (пусто) -> 404", status == 404)

        status, mats, _ = http_json(base, "GET", "/api/v1/material")
        check("material: пусто -> []", status == 200 and mats == [])
        status, locs, _ = http_json(base, "GET", "/api/v1/location")
        check("location: пусто -> []", status == 200 and locs == [])
        status, lots, _ = http_json(base, "GET", "/api/v1/lot-number")
        check("lot-number: пусто -> []", status == 200 and lots == [])

        status, v, _ = http_json(base, "POST", "/api/v1/vendor", {"name": "ListVendor"})
        status, f1, _ = http_json(base, "POST", "/api/v1/filament",
                                   {"vendor_id": v["id"], "material": "PLA", "density": 1.24, "diameter": 1.75})
        status, f2, _ = http_json(base, "POST", "/api/v1/filament",
                                   {"vendor_id": v["id"], "material": "PETG", "density": 1.27, "diameter": 1.75})
        http_json(base, "POST", "/api/v1/filament",
                  {"vendor_id": v["id"], "material": "PLA", "density": 1.24, "diameter": 1.75})
        http_json(base, "POST", "/api/v1/filament",
                  {"vendor_id": v["id"], "density": 1.0, "diameter": 1.75})  # без material
        http_json(base, "POST", "/api/v1/spool", {"filament_id": f1["id"], "location": "Shelf A", "lot_nr": "L1"})
        http_json(base, "POST", "/api/v1/spool", {"filament_id": f2["id"], "location": "Shelf B"})
        http_json(base, "POST", "/api/v1/spool", {"filament_id": f1["id"], "location": "Shelf A", "lot_nr": "L1"})
        http_json(base, "POST", "/api/v1/spool", {"filament_id": f1["id"]})  # без location/lot_nr

        status, mats, hdrs = http_json(base, "GET", "/api/v1/material")
        check("material: уникальные и отсортированы", status == 200 and mats == ["PETG", "PLA"], mats)
        check("material: X-Total-Count", hdrs.get("X-Total-Count") == "2", hdrs)

        status, locs, _ = http_json(base, "GET", "/api/v1/location")
        check("location: уникальные, отсортированы, пустые исключены", status == 200 and locs == ["Shelf A", "Shelf B"], locs)

        status, lots, _ = http_json(base, "GET", "/api/v1/lot-number")
        check("lot-number: уникальные, пустые исключены", status == 200 and lots == ["L1"], lots)
    finally:
        srv.cleanup()


# ---------------------------------------------------------------------------
# Персистентность
# ---------------------------------------------------------------------------

def test_persistence():
    srv = Server()
    try:
        srv.start()
        base = srv.base_url()
        status, v, _ = http_json(base, "POST", "/api/v1/vendor", {"name": "Persist"})
        status, f, _ = http_json(base, "POST", "/api/v1/filament", {"vendor_id": v["id"], "density": 1.24, "diameter": 1.75, "weight": 1000})
        status, s, _ = http_json(base, "POST", "/api/v1/spool", {"filament_id": f["id"]})

        rc = srv.stop(signal.SIGTERM)
        check("persistence: код выхода по SIGTERM == 0", rc == 0, f"rc={rc}")
        check("persistence: файл данных создан", os.path.exists(srv.datafile))

        srv2 = Server(datafile=srv.datafile, tmpdir=srv.tmpdir)
        srv2.start()
        base2 = srv2.base_url()
        status, v2, _ = http_json(base2, "GET", f"/api/v1/vendor/{v['id']}")
        check("persistence: вендор сохранился после перезапуска", status == 200 and v2["name"] == "Persist")
        status, s2, _ = http_json(base2, "GET", f"/api/v1/spool/{s['id']}")
        check("persistence: катушка сохранилась после перезапуска", status == 200 and s2["id"] == s["id"])
        srv2.stop()
        srv2.cleanup()
    finally:
        srv.cleanup()


def test_deferred_use_flush():
    srv = Server()
    try:
        srv.start()
        base = srv.base_url()
        status, v, _ = http_json(base, "POST", "/api/v1/vendor", {"name": "Deferred"})
        status, f, _ = http_json(base, "POST", "/api/v1/filament", {"vendor_id": v["id"], "density": 1.24, "diameter": 1.75, "weight": 1000})
        status, s, _ = http_json(base, "POST", "/api/v1/spool", {"filament_id": f["id"]})

        with open(srv.datafile, "r", encoding="utf-8") as fh:
            content_before_use = fh.read()

        status, used, _ = http_json(base, "PUT", f"/api/v1/spool/{s['id']}/use", {"use_weight": 33})
        check("deferred use: use применился в памяти", status == 200 and abs(used["used_weight"] - 33) < 1e-9)

        time.sleep(0.3)
        with open(srv.datafile, "r", encoding="utf-8") as fh:
            content_right_after_use = fh.read()
        check("deferred use: файл НЕ обновился сразу после use",
              content_right_after_use == content_before_use)

        rc = srv.stop(signal.SIGTERM)
        check("deferred use: SIGTERM сбрасывает данные (код 0)", rc == 0)
        with open(srv.datafile, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        saved_spool = next((x for x in data["spools"] if x["id"] == s["id"]), None)
        check("deferred use: use-изменение сброшено на диск при SIGTERM",
              saved_spool is not None and abs(saved_spool["used_weight"] - 33) < 1e-9, saved_spool)
    finally:
        srv.cleanup()


def test_corrupt_file():
    tmpdir = tempfile.mkdtemp(prefix="microspool_test_corrupt_")
    datafile = os.path.join(tmpdir, "microspool.json")
    with open(datafile, "w", encoding="utf-8") as f:
        f.write("это не json { битые данные")
    try:
        proc = subprocess.Popen([BINARY, "-l", "127.0.0.1", "-p", str(free_port()), "-d", datafile],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            rc = proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            rc = None
        check("corrupt file: код выхода == 1", rc == 1, f"rc={rc}")
        with open(datafile, "r", encoding="utf-8") as f:
            content = f.read()
        check("corrupt file: файл не перезаписан", content == "это не json { битые данные")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# WebSocket (tornado, как в Moonraker)
# ---------------------------------------------------------------------------

async def _ws_scenario(base, port):
    from tornado.websocket import websocket_connect
    from tornado.httpclient import AsyncHTTPClient, HTTPRequest

    ws_url = f"ws://127.0.0.1:{port}/api/v1/spool"
    conn = await websocket_connect(ws_url, connect_timeout=5.0, ping_interval=1.0, ping_timeout=20.0)

    events = []
    closed = asyncio.Event()

    async def reader():
        while True:
            msg = await conn.read_message()
            if msg is None:
                closed.set()
                return
            try:
                events.append(json.loads(msg))
            except Exception:
                pass

    reader_task = asyncio.ensure_future(reader())

    client = AsyncHTTPClient()

    async def post(path, body):
        r = await client.fetch(HTTPRequest(base + path, method="POST", body=json.dumps(body),
                                            headers={"Content-Type": "application/json"}))
        return r.code, json.loads(r.body)

    _, v = await post("/api/v1/vendor", {"name": "WSVendor"})
    _, f = await post("/api/v1/filament", {"vendor_id": v["id"], "density": 1.24, "diameter": 1.75, "weight": 1000})
    _, s = await post("/api/v1/spool", {"filament_id": f["id"]})

    await asyncio.sleep(0.3)

    r = await client.fetch(HTTPRequest(base + f"/api/v1/spool/{s['id']}/use", method="PUT",
                                        body=json.dumps({"use_weight": 42}),
                                        headers={"Content-Type": "application/json"}))
    use_status = r.code
    use_body = json.loads(r.body)

    await asyncio.sleep(0.3)

    r = await client.fetch(HTTPRequest(base + f"/api/v1/spool/{s['id']}", method="DELETE"))
    delete_status = r.code

    start = time.time()
    while time.time() - start < 5.3 and not closed.is_set():
        await asyncio.sleep(0.1)

    stayed_open = (not closed.is_set()) and (not reader_task.done())

    conn.close()
    await asyncio.sleep(0.2)
    reader_task.cancel()

    return {
        "spool_id": s["id"],
        "use_status": use_status,
        "use_body": use_body,
        "delete_status": delete_status,
        "stayed_open": stayed_open,
        "events": events,
    }


def test_websocket():
    srv = Server()
    try:
        srv.start()
        base = srv.base_url()
        result = asyncio.run(_ws_scenario(base, srv.port))

        check("ws: соединение живо >=5с (сервер отвечает на ping)", result["stayed_open"])
        check("ws: use через AsyncHTTPClient -> 200", result["use_status"] == 200)
        check("ws: use применил вес", abs(result["use_body"]["used_weight"] - 42) < 1e-3)
        check("ws: delete -> 200", result["delete_status"] == 200)

        updated_events = [e for e in result["events"] if e.get("type") == "updated" and e.get("resource") == "spool"
                           and e.get("payload", {}).get("id") == result["spool_id"]]
        deleted_events = [e for e in result["events"] if e.get("type") == "deleted" and e.get("resource") == "spool"
                           and e.get("payload", {}).get("id") == result["spool_id"]]
        check("ws: пришло событие updated (use)", len(updated_events) >= 1, result["events"])
        check("ws: пришло событие deleted (delete)", len(deleted_events) >= 1, result["events"])
        if deleted_events:
            check("ws: событие содержит date", "date" in deleted_events[0])
    finally:
        srv.cleanup()


# ---------------------------------------------------------------------------
# Лимит соединений и утечки
# ---------------------------------------------------------------------------

def test_connection_limit():
    srv = Server()
    try:
        srv.start()
        socks = []
        try:
            for i in range(8):
                s = socket.create_connection(("127.0.0.1", srv.port), timeout=3)
                socks.append(s)
            # 9-е соединение сервер обязан принять и сразу закрыть
            s9 = socket.create_connection(("127.0.0.1", srv.port), timeout=3)
            s9.settimeout(2.0)
            data = b""
            try:
                data = s9.recv(4096)
            except socket.timeout:
                pass
            check("conn limit: 9-е соединение закрыто сервером (EOF)", data == b"", f"got {data!r}")
            s9.close()

            # сервер по-прежнему жив: закрываем один слот и делаем обычный запрос
            socks[0].close()
            socks = socks[1:]
            time.sleep(0.1)
            status, body, _ = http_json(srv.base_url(), "GET", "/api/v1/health")
            check("conn limit: сервер жив после лимита", status == 200 and body.get("status") == "healthy")
        finally:
            for s in socks:
                try:
                    s.close()
                except Exception:
                    pass
    finally:
        srv.cleanup()


def get_rss_kb(pid):
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]).decode().strip()
    return int(out.split()[0]) if out else None


def test_no_leak_2000_requests():
    srv = Server()
    try:
        srv.start()
        base = srv.base_url()
        # "Разогрев" аллокатора: первые сотни запросов устанавливают арены/бины
        # малого размера и дают самый большой (не связанный с утечкой) скачок RSS;
        # снимаем базовую линию уже после того, как это устаканится.
        for _ in range(2000):
            http_json(base, "GET", "/api/v1/health")
        time.sleep(0.1)
        rss_before = get_rss_kb(srv.proc.pid)

        for i in range(2000):
            http_json(base, "GET", "/api/v1/health")

        time.sleep(0.2)
        rss_after = get_rss_kb(srv.proc.pid)
        delta = rss_after - rss_before
        check(f"no leak: RSS вырос не более чем на 200КБ за 2000 запросов (before={rss_before}KB after={rss_after}KB delta={delta}KB)",
              delta <= 200, f"delta={delta}KB")
    finally:
        srv.cleanup()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    if not os.path.exists(BINARY):
        print(f"не найден бинарник {BINARY}; сначала выполните `make native`", file=sys.stderr)
        return 1

    srv = Server()
    try:
        srv.start()
        base = srv.base_url()
        vendor_id = test_vendor_crud(base)
        filament_id = test_filament_crud(base, vendor_id)
        test_spool_crud_and_weights(base, filament_id)
        test_errors_and_limits(base)
    finally:
        srv.cleanup()

    test_ui()
    test_ui_disabled()
    test_ui_js_syntax()
    test_settings_and_lists()
    test_persistence()
    test_deferred_use_flush()
    test_corrupt_file()
    test_websocket()
    test_connection_limit()
    test_no_leak_2000_requests()

    print(f"\n{PASSES} passed, {len(FAILURES)} failed")
    if FAILURES:
        print("Провалившиеся тесты:")
        for name in FAILURES:
            print(f"  - {name}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
