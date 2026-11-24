import asyncio
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("unity_seller_bot")

# ================== КОНФИГУРАЦИЯ ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
DEFAULT_AUTHOR = os.getenv("DEFAULT_AUTHOR", "highlose").strip() or "highlose"
DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
RUB_PROVIDER_TOKEN = os.getenv("RUB_PAYMENT_PROVIDER_TOKEN", "").strip()

FILES_DIR = DATA_DIR / "product_files"
BACKUPS_DIR = DATA_DIR / "backups"
DB_PATH = DATA_DIR / "shop.db"
LATEST_SNAPSHOT_PATH = BACKUPS_DIR / "latest_snapshot.db"
LATEST_JSON_PATH = BACKUPS_DIR / "latest_snapshot.json"

for folder in (DATA_DIR, FILES_DIR, BACKUPS_DIR):
    folder.mkdir(parents=True, exist_ok=True)


def is_admin(user_id: Optional[int]) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


# ================== БАЗА ДАННЫХ И АВТОСЕЙФ ==================
class DatabaseManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    author TEXT NOT NULL,
                    photo_file_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    original_filename TEXT NOT NULL,
                    price_stars INTEGER NOT NULL DEFAULT 0,
                    price_rub INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    product_id INTEGER NOT NULL,
                    currency TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    telegram_charge_id TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def restore_if_needed(self) -> str:
        """
        При старте проверяет БД:
        - если основной файл БД отсутствует или повреждён/пуст, восстанавливает из latest_snapshot.db;
        - если основной файл есть, фиксирует метку старта и создаёт свежий снапшот.
        """
        db_exists = self.db_path.exists() and self.db_path.stat().st_size > 0
        snapshot_exists = (
            LATEST_SNAPSHOT_PATH.exists() and LATEST_SNAPSHOT_PATH.stat().st_size > 0
        )

        if not db_exists and snapshot_exists:
            shutil.copy2(LATEST_SNAPSHOT_PATH, self.db_path)
            self.init_schema()
            self._set_meta("last_restore", datetime.utcnow().isoformat())
            return "restored_from_snapshot"

        if db_exists:
            try:
                self.init_schema()
                with self._connect() as conn:
                    conn.execute("SELECT COUNT(*) FROM products").fetchone()
            except sqlite3.DatabaseError:
                if snapshot_exists:
                    corrupted = self.db_path.with_suffix(".corrupted.db")
                    shutil.move(self.db_path, corrupted)
                    shutil.copy2(LATEST_SNAPSHOT_PATH, self.db_path)
                    self.init_schema()
                    return "recovered_from_corrupted_db"
                raise

        self.init_schema()
        self._set_meta("last_boot", datetime.utcnow().isoformat())
        self.save_snapshot_sync()
        return "initialized_and_saved"

    def _set_meta(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO meta (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            conn.commit()

    def save_snapshot_sync(self) -> None:
        """Безопасно сохраняет консистентный снапшот SQLite через SQLite backup API + JSON."""
        if not self.db_path.exists():
            return
        now_iso = datetime.utcnow().isoformat()
        self._set_meta("last_autosave", now_iso)

        with self._connect() as src:
            dst = sqlite3.connect(LATEST_SNAPSHOT_PATH)
            try:
                src.backup(dst)
            finally:
                dst.close()

            products = [dict(r) for r in src.execute("SELECT * FROM products ORDER BY id")]
            orders = [dict(r) for r in src.execute("SELECT * FROM orders ORDER BY id")]

        payload = {
            "saved_at": now_iso,
            "products": products,
            "orders": orders,
        }
        tmp_json = LATEST_JSON_PATH.with_suffix(".tmp")
        tmp_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_json.replace(LATEST_JSON_PATH)

    async def autosave(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self.save_snapshot_sync)

    async def add_product(
        self,
        title: str,
        description: str,
        author: str,
        photo_file_id: str,
        file_path: str,
        original_filename: str,
        price_stars: int,
        price_rub: int,
    ) -> int:
        async with self._lock:
            def _insert() -> int:
                with self._connect() as conn:
                    cur = conn.execute(
                        """
                        INSERT INTO products (
                            title, description, author, photo_file_id,
                            file_path, original_filename, price_stars,
                            price_rub, is_active, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                        """,
                        (
                            title,
                            description,
                            author,
                            photo_file_id,
                            file_path,
                            original_filename,
                            price_stars,
                            price_rub,
                            datetime.utcnow().isoformat(),
                        ),
                    )
                    conn.commit()
                    product_id = int(cur.lastrowid)
                self.save_snapshot_sync()
                return product_id

            return await asyncio.to_thread(_insert)

    async def get_active_products(self) -> List[Dict[str, Any]]:
        async with self._lock:
            def _query() -> List[Dict[str, Any]]:
                with self._connect() as conn:
                    rows = conn.execute(
                        "SELECT * FROM products WHERE is_active = 1 ORDER BY id ASC"
                    ).fetchall()
                return [dict(r) for r in rows]

            return await asyncio.to_thread(_query)

    async def get_all_products(self) -> List[Dict[str, Any]]:
        async with self._lock:
            def _query() -> List[Dict[str, Any]]:
                with self._connect() as conn:
                    rows = conn.execute("SELECT * FROM products ORDER BY id ASC").fetchall()
                return [dict(r) for r in rows]

            return await asyncio.to_thread(_query)

    async def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            def _query() -> Optional[Dict[str, Any]]:
                with self._connect() as conn:
                    row = conn.execute(
                        "SELECT * FROM products WHERE id = ?", (product_id,)
                    ).fetchone()
                return dict(row) if row else None

            return await asyncio.to_thread(_query)

    async def update_prices(
        self, product_id: int, price_stars: int, price_rub: int
    ) -> bool:
        async with self._lock:
            def _update() -> bool:
                with self._connect() as conn:
                    cur = conn.execute(
                        """
                        UPDATE products
                        SET price_stars = ?, price_rub = ?
                        WHERE id = ?
                        """,
                        (price_stars, price_rub, product_id),
                    )
                    conn.commit()
                    changed = cur.rowcount > 0
                if changed:
                    self.save_snapshot_sync()
                return changed

            return await asyncio.to_thread(_update)

    async def toggle_active(self, product_id: int) -> Optional[int]:
        async with self._lock:
            def _toggle() -> Optional[int]:
                with self._connect() as conn:
                    row = conn.execute(
                        "SELECT is_active FROM products WHERE id = ?", (product_id,)
                    ).fetchone()
                    if not row:
                        return None
                    new_state = 0 if int(row["is_active"]) == 1 else 1
                    conn.execute(
                        "UPDATE products SET is_active = ? WHERE id = ?",
                        (new_state, product_id),
                    )
                    conn.commit()
                self.save_snapshot_sync()
                return new_state

            return await asyncio.to_thread(_toggle)

    async def delete_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            def _delete() -> Optional[Dict[str, Any]]:
                with self._connect() as conn:
                    row = conn.execute(
                        "SELECT * FROM products WHERE id = ?", (product_id,)
                    ).fetchone()
                    if not row:
                        return None
                    prod = dict(row)
                    conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
                    conn.commit()
                self.save_snapshot_sync()
                return prod

            return await asyncio.to_thread(_delete)

    async def record_order(
        self,
        user_id: int,
        username: Optional[str],
        product_id: int,
        currency: str,
        amount: int,
        telegram_charge_id: str,
    ) -> int:
        async with self._lock:
            def _insert() -> int:
                with self._connect() as conn:
                    cur = conn.execute(
                        """
                        INSERT INTO orders (
                            user_id, username, product_id, currency,
                            amount, telegram_charge_id, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            username or "",
                            product_id,
                            currency,
                            amount,
                            telegram_charge_id,
                            datetime.utcnow().isoformat(),
                        ),
                    )
                    conn.commit()
                    order_id = int(cur.lastrowid)
                self.save_snapshot_sync()
                return order_id

            return await asyncio.to_thread(_insert)

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            def _stats() -> Dict[str, Any]:
                with self._connect() as conn:
                    total_products = conn.execute(
                        "SELECT COUNT(*) FROM products"
                    ).fetchone()[0]
                    active_products = conn.execute(
                        "SELECT COUNT(*) FROM products WHERE is_active = 1"
                    ).fetchone()[0]
                    total_orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                    stars_sum = conn.execute(
                        "SELECT COALESCE(SUM(amount), 0) FROM orders WHERE currency = 'XTR'"
                    ).fetchone()[0]
                    rub_sum = conn.execute(
                        "SELECT COALESCE(SUM(amount), 0) FROM orders WHERE currency = 'RUB'"
                    ).fetchone()[0]
                    meta_rows = {
                        r["key"]: r["value"]
                        for r in conn.execute("SELECT key, value FROM meta").fetchall()
                    }
                return {
                    "total_products": total_products,
                    "active_products": active_products,
                    "total_orders": total_orders,
                    "stars_sum": stars_sum,
                    "rub_sum": rub_sum,
                    "last_autosave": meta_rows.get("last_autosave", "—"),
                }

            return await asyncio.to_thread(_stats)


db = DatabaseManager(DB_PATH)


# ================== FSM СОСТОЯНИЯ ==================
class AddProductFSM(StatesGroup):
    photo = State()
    title = State()
    description = State()
    author = State()
    price_stars = State()
    price_rub = State()
    file = State()


class EditPriceFSM(StatesGroup):
    price_stars = State()
    price_rub = State()


# ================== КЛАВИАТУРЫ И ФОРМАТИРОВАНИЕ ==================
def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="🛍 Витрина Unity-проектов", callback_data="showcase:0"
            )
        ]
    ]
    if is_admin(user_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text="➕ Добавить проект", callback_data="admin:add_product"
                ),
                InlineKeyboardButton(
                    text="⚙️ Управление товарами", callback_data="admin:products:0"
                ),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="📊 Статистика", callback_data="admin:stats"
                ),
                InlineKeyboardButton(
                    text="💾 Сохранить сейчас", callback_data="admin:force_save"
                ),
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cancel_kb(back_cb: str = "menu:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=back_cb)]
        ]
    )


def format_prices(product: Dict[str, Any]) -> str:
    parts: List[str] = []
    stars = int(product.get("price_stars") or 0)
    rub = int(product.get("price_rub") or 0)
    if stars > 0:
        parts.append(f"⭐️ <b>{stars}</b> звёзд")
    if rub > 0:
        parts.append(f"₽ <b>{rub}</b> руб.")
    return " / ".join(parts) if parts else "Цена не указана"


def format_product_card(product: Dict[str, Any], idx: int, total: int) -> str:
    return (
        f"📦 <b>{product['title']}</b> ({idx + 1}/{total})\n"
        f"👤 <b>Автор:</b> {product['author']}\n"
        f"💰 <b>Цена:</b> {format_prices(product)}\n\n"
        f"{product['description']}"
    )


def showcase_kb(product: Dict[str, Any], idx: int, total: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    pay_row: List[InlineKeyboardButton] = []
    if int(product.get("price_stars") or 0) > 0:
        pay_row.append(
            InlineKeyboardButton(
                text=f"Купить за {product['price_stars']} ⭐️",
                callback_data=f"buy:stars:{product['id']}:{idx}",
            )
        )
    if int(product.get("price_rub") or 0) > 0:
        pay_row.append(
            InlineKeyboardButton(
                text=f"Купить за {product['price_rub']} ₽",
                callback_data=f"buy:rub:{product['id']}:{idx}",
            )
        )
    if pay_row:
        rows.append(pay_row)

    nav_row: List[InlineKeyboardButton] = []
    if total > 1:
        prev_idx = (idx - 1) % total
        next_idx = (idx + 1) % total
        nav_row.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"showcase:{prev_idx}")
        )
        nav_row.append(
            InlineKeyboardButton(text="Далее ▶️", callback_data=f"showcase:{next_idx}")
        )
        rows.append(nav_row)

    rows.append(
        [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:main")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_product_manage_kb(
    product: Dict[str, Any], idx: int, total: int
) -> InlineKeyboardMarkup:
    status_label = "🟢 Активен (скрыть)" if product["is_active"] else "🔴 Скрыт (показать)"
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✏️ Изменить цену",
                callback_data=f"admin:edit_price:{product['id']}:{idx}",
            ),
            InlineKeyboardButton(
                text=status_label,
                callback_data=f"admin:toggle:{product['id']}:{idx}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="🗑 Удалить проект",
                callback_data=f"admin:delete:{product['id']}:{idx}",
            )
        ],
    ]
    if total > 1:
        prev_idx = (idx - 1) % total
        next_idx = (idx + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    text="◀️ Пред.", callback_data=f"admin:products:{prev_idx}"
                ),
                InlineKeyboardButton(
                    text="След. ▶️", callback_data=f"admin:products:{next_idx}"
                ),
            ]
        )
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ================== РОУТЕР И ХЭНДЛЕРЫ ==================
router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    role_note = "\n🛠 Режим администратора активен." if is_admin(message.from_user.id) else ""
    await message.answer(
        f"Привет! Это витрина Unity-проектов от <b>{DEFAULT_AUTHOR}</b>.{role_note}\n\n"
        "Выбери действие в меню ниже:",
        reply_markup=main_menu_kb(message.from_user.id),
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        "Действие отменено.", reply_markup=main_menu_kb(message.from_user.id)
    )


@router.callback_query(F.data == "menu:main")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    text = (
        f"Главное меню витрины <b>{DEFAULT_AUTHOR}</b>.\n"
        "Листай проекты, выбирай оплату в ⭐️ или ₽ и получай файл сразу после оплаты."
    )
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(
            text, reply_markup=main_menu_kb(callback.from_user.id)
        )
    else:
        await callback.message.edit_text(
            text, reply_markup=main_menu_kb(callback.from_user.id)
        )
    await callback.answer()


# ---------- ВИТРИНА ПОКУПАТЕЛЯ ----------
async def _render_showcase_page(
    target_message: Message, idx: int, edit_existing: bool = False
) -> None:
    products = await db.get_active_products()
    if not products:
        empty_text = "Пока в витрине нет активных проектов. Загляни позже!"
        if edit_existing and not target_message.photo:
            await target_message.edit_text(
                empty_text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:main")]
                    ]
                ),
            )
        else:
            if edit_existing and target_message.photo:
                await target_message.delete()
            await target_message.answer(
                empty_text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:main")]
                    ]
                ),
            )
        return

    safe_idx = idx % len(products)
    product = products[safe_idx]
    caption = format_product_card(product, safe_idx, len(products))
    kb = showcase_kb(product, safe_idx, len(products))

    if edit_existing and target_message.photo:
        from aiogram.types import InputMediaPhoto

        await target_message.edit_media(
            media=InputMediaPhoto(media=product["photo_file_id"], caption=caption),
            reply_markup=kb,
        )
    else:
        if edit_existing:
            await target_message.delete()
        await target_message.answer_photo(
            photo=product["photo_file_id"], caption=caption, reply_markup=kb
        )


@router.callback_query(F.data.startswith("showcase:"))
async def cb_showcase(callback: CallbackQuery) -> None:
    idx = int(callback.data.split(":")[1])
    await _render_showcase_page(callback.message, idx=idx, edit_existing=True)
    await callback.answer()


# ---------- ОПЛАТА (TELEGRAM STARS И РУБЛИ) ----------
@router.callback_query(F.data.startswith("buy:"))
async def cb_buy(callback: CallbackQuery, bot: Bot) -> None:
    _, currency_mode, prod_id_str, _ = callback.data.split(":")
    product = await db.get_product(int(prod_id_str))
    if not product or not product["is_active"]:
        await callback.answer("Товар недоступен.", show_alert=True)
        return

    payload = f"order:{product['id']}:{currency_mode}:{callback.from_user.id}"

    if currency_mode == "stars":
        stars_amount = int(product["price_stars"])
        if stars_amount <= 0:
            await callback.answer("Оплата звёздами не задана.", show_alert=True)
            return
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=product["title"][:32],
            description=product["description"][:255],
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=product["title"][:32], amount=stars_amount)],
        )
        await callback.answer("Счёт на оплату звёздами отправлен.")
        return

    if currency_mode == "rub":
        rub_amount = int(product["price_rub"])
        if rub_amount <= 0:
            await callback.answer("Оплата в рублях не задана.", show_alert=True)
            return
        if not RUB_PROVIDER_TOKEN:
            await callback.answer(
                "Оплата в рублях пока не подключена (укажи RUB_PAYMENT_PROVIDER_TOKEN).",
                show_alert=True,
            )
            return
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=product["title"][:32],
            description=product["description"][:255],
            payload=payload,
            provider_token=RUB_PROVIDER_TOKEN,
            currency="RUB",
            prices=[
                LabeledPrice(label=product["title"][:32], amount=rub_amount * 100)
            ],
        )
        await callback.answer("Счёт на оплату в рублях отправлен.")


@router.pre_checkout_query()
async def on_pre_checkout(pre_checkout_query: PreCheckoutQuery) -> None:
    parts = pre_checkout_query.invoice_payload.split(":")
    if len(parts) < 4 or parts[0] != "order":
        await pre_checkout_query.answer(ok=False, error_message="Некорректный заказ.")
        return
    product = await db.get_product(int(parts[1]))
    if not product or not product["is_active"]:
        await pre_checkout_query.answer(
            ok=False, error_message="Этот товар больше недоступен."
        )
        return
    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def on_successful_payment(message: Message, bot: Bot) -> None:
    sp = message.successful_payment
    parts = sp.invoice_payload.split(":")
    product_id = int(parts[1])
    product = await db.get_product(product_id)

    currency = sp.currency
    amount = sp.total_amount if currency == "XTR" else sp.total_amount // 100

    await db.record_order(
        user_id=message.from_user.id,
        username=message.from_user.username,
        product_id=product_id,
        currency=currency,
        amount=amount,
        telegram_charge_id=sp.telegram_payment_charge_id,
    )

    if not product:
        await message.answer(
            "Оплата прошла, но товар не найден в БД. Напиши администратору и укажи ID заказа."
        )
        return

    file_path = Path(product["file_path"])
    if not file_path.exists():
        await message.answer(
            f"Оплата за <b>{product['title']}</b> прошла! Файл временно отсутствует на диске — свяжись с автором."
        )
    else:
        doc = FSInputFile(path=file_path, filename=product["original_filename"])
        await message.answer_document(
            document=doc,
            caption=(
                f"✅ Спасибо за покупку <b>{product['title']}</b>!\n"
                f"👤 Автор: {product['author']}\n"
                "Файл проекта прикреплён выше."
            ),
        )

    for admin_id in ADMIN_IDS:
        try:
            buyer_tag = (
                f"@{message.from_user.username}"
                if message.from_user.username
                else f"ID {message.from_user.id}"
            )
            await bot.send_message(
                admin_id,
                f"💸 <b>Новая продажа!</b>\n"
                f"Проект: <b>{product['title']}</b>\n"
                f"Покупатель: {buyer_tag}\n"
                f"Сумма: <b>{amount}</b> {currency}",
            )
        except Exception:
            logger.exception("Не удалось уведомить админа %s о продаже", admin_id)


# ---------- АДМИН-ПАНЕЛЬ: ДОБАВЛЕНИЕ ТОВАРА ----------
@router.callback_query(F.data == "admin:add_product")
async def cb_admin_add_product(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    await state.clear()
    await state.set_state(AddProductFSM.photo)
    await callback.message.answer(
        "Шаг 1/7: Отправь <b>скриншот</b> Unity-проекта (фото).",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(AddProductFSM.photo, F.photo)
async def fsm_add_photo(message: Message, state: FSMContext) -> None:
    photo_id = message.photo[-1].file_id
    await state.update_data(photo_file_id=photo_id)
    await state.set_state(AddProductFSM.title)
    await message.answer(
        "Шаг 2/7: Введи <b>название</b> проекта:", reply_markup=cancel_kb()
    )


@router.message(AddProductFSM.photo)
async def fsm_add_photo_invalid(message: Message) -> None:
    await message.answer("Нужно отправить именно картинку (скриншот).")


@router.message(AddProductFSM.title, F.text)
async def fsm_add_title(message: Message, state: FSMContext) -> None:
    await state.update_data(title=message.text.strip())
    await state.set_state(AddProductFSM.description)
    await message.answer(
        "Шаг 3/7: Введи <b>описание</b> проекта:", reply_markup=cancel_kb()
    )


@router.message(AddProductFSM.description, F.text)
async def fsm_add_description(message: Message, state: FSMContext) -> None:
    await state.update_data(description=message.text.strip())
    await state.set_state(AddProductFSM.author)
    await message.answer(
        f"Шаг 4/7: Введи <b>автора</b> (или отправь <code>-</code>, чтобы поставить <b>{DEFAULT_AUTHOR}</b>):",
        reply_markup=cancel_kb(),
    )


@router.message(AddProductFSM.author, F.text)
async def fsm_add_author(message: Message, state: FSMContext) -> None:
    raw = message.text.strip()
    author = DEFAULT_AUTHOR if raw in ("-", "") else raw
    await state.update_data(author=author)
    await state.set_state(AddProductFSM.price_stars)
    await message.answer(
        "Шаг 5/7: Введи цену в <b>Telegram Stars</b> (целое число, например <code>250</code>; если только за рубли — введи <code>0</code>):",
        reply_markup=cancel_kb(),
    )


@router.message(AddProductFSM.price_stars, F.text)
async def fsm_add_price_stars(message: Message, state: FSMContext) -> None:
    raw = message.text.strip()
    if not raw.isdigit():
        await message.answer("Введи целое неотрицательное число (например, 250 или 0).")
        return
    await state.update_data(price_stars=int(raw))
    await state.set_state(AddProductFSM.price_rub)
    await message.answer(
        "Шаг 6/7: Введи цену в <b>рублях</b> (целое число, например <code>500</code>; если только за звёзды — введи <code>0</code>):",
        reply_markup=cancel_kb(),
    )


@router.message(AddProductFSM.price_rub, F.text)
async def fsm_add_price_rub(message: Message, state: FSMContext) -> None:
    raw = message.text.strip()
    if not raw.isdigit():
        await message.answer("Введи целое неотрицательное число (например, 500 или 0).")
        return
    data = await state.get_data()
    price_rub = int(raw)
    if int(data["price_stars"]) <= 0 and price_rub <= 0:
        await message.answer(
            "Хотя бы одна цена (в звёздах или в рублях) должна быть больше 0. Введи цену в рублях ещё раз:"
        )
        return
    await state.update_data(price_rub=price_rub)
    await state.set_state(AddProductFSM.file)
    await message.answer(
        "Шаг 7/7: Теперь отправь сам <b>файл проекта</b> (документом — например `.zip`, `.unitypackage`, `.rar`):",
        reply_markup=cancel_kb(),
    )


@router.message(AddProductFSM.file, F.document)
async def fsm_add_file(message: Message, state: FSMContext, bot: Bot) -> None:
    doc = message.document
    safe_name = Path(doc.file_name or "unity_project.zip").name
    stored_name = f"{int(datetime.utcnow().timestamp())}_{safe_name}"
    dest_path = FILES_DIR / stored_name

    await bot.download(doc, destination=dest_path)

    data = await state.get_data()
    product_id = await db.add_product(
        title=data["title"],
        description=data["description"],
        author=data["author"],
        photo_file_id=data["photo_file_id"],
        file_path=str(dest_path),
        original_filename=safe_name,
        price_stars=data["price_stars"],
        price_rub=data["price_rub"],
    )
    await state.clear()

    await message.answer(
        f"✅ Проект <b>#{product_id} — {data['title']}</b> добавлен в витрину и сохранён в автосейв!",
        reply_markup=main_menu_kb(message.from_user.id),
    )


@router.message(AddProductFSM.file)
async def fsm_add_file_invalid(message: Message) -> None:
    await message.answer("Отправь проект именно как файл (документ), а не сжатое фото.")


# ---------- АДМИН-ПАНЕЛЬ: УПРАВЛЕНИЕ ТОВАРАМИ, ЦЕНАМИ И СТАТИСТИКОЙ ----------
async def _render_admin_product(
    target_message: Message, idx: int, edit_existing: bool = False
) -> None:
    products = await db.get_all_products()
    if not products:
        text = "Список проектов пуст. Нажми «➕ Добавить проект», чтобы выложить первый."
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить проект", callback_data="admin:add_product")],
                [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:main")],
            ]
        )
        if edit_existing and not target_message.photo:
            await target_message.edit_text(text, reply_markup=kb)
        else:
            if edit_existing:
                await target_message.delete()
            await target_message.answer(text, reply_markup=kb)
        return

    safe_idx = idx % len(products)
    prod = products[safe_idx]
    status_text = "Активен" if prod["is_active"] else "Скрыт"
    caption = (
        f"🛠 <b>Управление #{prod['id']}</b> ({safe_idx + 1}/{len(products)})\n"
        f"Статус: <b>{status_text}</b>\n"
        f"Название: <b>{prod['title']}</b>\n"
        f"Автор: {prod['author']}\n"
        f"Цена: {format_prices(prod)}\n"
        f"Файл: <code>{prod['original_filename']}</code>\n\n"
        f"{prod['description']}"
    )
    kb = admin_product_manage_kb(prod, safe_idx, len(products))

    if edit_existing and target_message.photo:
        from aiogram.types import InputMediaPhoto

        await target_message.edit_media(
            media=InputMediaPhoto(media=prod["photo_file_id"], caption=caption),
            reply_markup=kb,
        )
    else:
        if edit_existing:
            await target_message.delete()
        await target_message.answer_photo(
            photo=prod["photo_file_id"], caption=caption, reply_markup=kb
        )


@router.callback_query(F.data.startswith("admin:products:"))
async def cb_admin_products(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    idx = int(callback.data.split(":")[2])
    await _render_admin_product(callback.message, idx=idx, edit_existing=True)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:toggle:"))
async def cb_admin_toggle(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    _, _, prod_id_str, idx_str = callback.data.split(":")
    await db.toggle_active(int(prod_id_str))
    await _render_admin_product(callback.message, idx=int(idx_str), edit_existing=True)
    await callback.answer("Статус обновлён.")


@router.callback_query(F.data.startswith("admin:delete:"))
async def cb_admin_delete(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    _, _, prod_id_str, idx_str = callback.data.split(":")
    deleted = await db.delete_product(int(prod_id_str))
    if deleted and deleted.get("file_path"):
        try:
            Path(deleted["file_path"]).unlink(missing_ok=True)
        except OSError:
            pass
    await _render_admin_product(callback.message, idx=int(idx_str), edit_existing=True)
    await callback.answer("Проект удалён.")


@router.callback_query(F.data.startswith("admin:edit_price:"))
async def cb_admin_edit_price(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    _, _, prod_id_str, idx_str = callback.data.split(":")
    await state.clear()
    await state.set_state(EditPriceFSM.price_stars)
    await state.update_data(product_id=int(prod_id_str), return_idx=int(idx_str))
    await callback.message.answer(
        "Введи новую цену в <b>Telegram Stars</b> (целое число, 0 если отключить):",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(EditPriceFSM.price_stars, F.text)
async def fsm_edit_price_stars(message: Message, state: FSMContext) -> None:
    raw = message.text.strip()
    if not raw.isdigit():
        await message.answer("Введи целое неотрицательное число.")
        return
    await state.update_data(new_stars=int(raw))
    await state.set_state(EditPriceFSM.price_rub)
    await message.answer(
        "Теперь введи новую цену в <b>рублях</b> (целое число, 0 если отключить):",
        reply_markup=cancel_kb(),
    )


@router.message(EditPriceFSM.price_rub, F.text)
async def fsm_edit_price_rub(message: Message, state: FSMContext) -> None:
    raw = message.text.strip()
    if not raw.isdigit():
        await message.answer("Введи целое неотрицательное число.")
        return
    data = await state.get_data()
    new_stars = int(data["new_stars"])
    new_rub = int(raw)
    if new_stars <= 0 and new_rub <= 0:
        await message.answer("Хотя бы одна цена должна быть больше 0. Введи цену в рублях:")
        return

    await db.update_prices(int(data["product_id"]), new_stars, new_rub)
    return_idx = int(data.get("return_idx", 0))
    await state.clear()
    await message.answer("✅ Цены обновлены и сохранены.")
    await _render_admin_product(message, idx=return_idx, edit_existing=False)


@router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    stats = await db.get_stats()
    text = (
        "📊 <b>Статистика магазина</b>\n\n"
        f"📦 Проектов всего: <b>{stats['total_products']}</b> (активных: <b>{stats['active_products']}</b>)\n"
        f"🧾 Продаж всего: <b>{stats['total_orders']}</b>\n"
        f"⭐️ Выручка в звёздах: <b>{stats['stars_sum']}</b>\n"
        f"₽ Выручка в рублях: <b>{stats['rub_sum']}</b>\n"
        f"💾 Последний автосейв: <code>{stats['last_autosave']}</code>"
    )
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=main_menu_kb(callback.from_user.id))
    else:
        await callback.message.edit_text(
            text, reply_markup=main_menu_kb(callback.from_user.id)
        )
    await callback.answer()


@router.callback_query(F.data == "admin:force_save")
async def cb_admin_force_save(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    await db.autosave()
    await callback.answer("Снапшот БД и JSON сохранён!", show_alert=True)


# ---------- ФОНОВЫЙ АВТОСЕЙФ КАЖДЫЕ 5 МИНУТ ----------
async def autosave_loop(interval_seconds: int = 300) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await db.autosave()
            logger.info("Периодический автосейв БД выполнен (интервал %sс).", interval_seconds)
        except Exception:
            logger.exception("Ошибка при выполнении периодического автосейва")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задана переменная окружения BOT_TOKEN")

    status = db.restore_if_needed()
    logger.info("Инициализация БД завершена со статусом: %s", status)

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    autosave_task = asyncio.create_task(autosave_loop(interval_seconds=300))
    try:
        await dp.start_polling(bot)
    finally:
        autosave_task.cancel()
        try:
            await autosave_task
        except asyncio.CancelledError:
            pass
        await db.autosave()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
