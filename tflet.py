import requests
import flet as ft
import pymysql
import base64
from pymysql.constants import CLIENT
from datetime import datetime
from PIL import Image
from io import BytesIO
from typing import Optional, Dict, List, Tuple
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Заменяем статические значения на переменные окружения
API_KEY = os.getenv("STEAM_API_KEY")

colors = ft.Colors
BorderSide = ft.border.BorderSide

class SteamAPIManager:
    BASE_URL = "https://api.steampowered.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_player_summary(self, steam_id: str) -> Optional[Dict]:
        try:
            steam_id_int = int(steam_id)
        except ValueError:
            return None

        params = {"key": self.api_key, "steamids": steam_id_int}
        endpoint = f"{self.BASE_URL}/ISteamUser/GetPlayerSummaries/v2/"

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('response', {}).get('players', [{}])[0]
        except Exception as e:
            print(f"Error getting player summary: {e}")
            return None

    def get_owned_games(self, steam_id: str) -> List[Dict]:
        endpoint = f"{self.BASE_URL}/IPlayerService/GetOwnedGames/v1/"
        params = {
            "key": self.api_key,
            "steamid": steam_id,
            "include_appinfo": True,
            "include_played_free_games": True
        }
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            return response.json().get('response', {}).get('games', [])
        except Exception as e:
            print(f"Error getting owned games: {e}")
            return []

    def get_player_achievements(self, steam_id: str, app_id: int) -> List[Dict]:
        endpoint = f"{self.BASE_URL}/ISteamUserStats/GetPlayerAchievements/v1/"
        params = {"key": self.api_key, "steamid": steam_id, "appid": app_id}
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            return response.json().get('playerstats', {}).get('achievements', [])
        except Exception as e:
            print(f"Error getting achievements for app {app_id}: {e}")
            return []

    def get_achievement_schema(self, app_id: int) -> Dict[str, float]:
        endpoint = f"{self.BASE_URL}/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/"
        params = {"gameid": app_id}  # Используем правильный параметр gameid вместо appid

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Правильный путь к данным в ответе API
            achievements = data.get('achievementpercentages', {}).get('achievements', [])

            return {ach['name']: ach.get('percent', 0.0) for ach in achievements}

        except Exception as e:
            print(f"Error getting schema for app {app_id}: {e}")
            return {}

    def get_avatar_image(self, url: str) -> Optional[bytes]:
        try:
            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()
            with Image.open(BytesIO(response.content)) as img:
                img.thumbnail((100, 100))
                buffer = BytesIO()
                img.save(buffer, format="PNG")
                return buffer.getvalue()
        except Exception as e:
            print(f"Error downloading avatar: {e}")
            return None

class DBManager:
    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host="localhost",
                user="root",
                password="123456fF",
                database="acch",
                client_flag=CLIENT.MULTI_STATEMENTS,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            print("Database connection established")
        except pymysql.Error as e:
            print(f"Database connection failed: {e}")
            raise

    def execute_query(self, query: str, params: Tuple = None) -> Optional[List[Dict]]:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except pymysql.Error as e:
            print(f"Query execution failed: {e}")
            return None

    def execute_update(self, query: str, params=None, many=False) -> bool:
        try:
            with self.connection.cursor() as cursor:
                if many:
                    cursor.executemany(query, params)
                else:
                    cursor.execute(query, params)
                self.connection.commit()
                return True
        except pymysql.Error as e:
            self.connection.rollback()
            print(f"Update failed: {e}")
            return False

    def reconnect(self):
        """Reconnect to the database"""
        self.disconnect()
        self.connect()

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            try:
                self.connection.close()
                print("Database connection closed")
            except Exception as e:
                print(f"Error closing connection: {e}")
            finally:
                self.connection = None
                self.cursor = None

    # Profile-related operations
    def get_profile_statistics(self, steam_id: str) -> Optional[Dict]:
        try:
            steam_id_int = int(steam_id)
        except ValueError:
            return None
        query = """
            WITH profile_stats AS (
                SELECT 
                    p.steam_id,
                    p.nickname,
                    p.registration_date,
                    p.avatar_url,

                    -- Total Achievements
                    (SELECT COALESCE(COUNT(*), 0)
                     FROM profile_achievements pa
                     WHERE pa.profile_id = p.steam_id 
                       AND pa.completeness = 1) AS total_achievements,

                    -- Total Games
                    (SELECT COALESCE(COUNT(*), 0)
                     FROM profile_games pg
                     WHERE pg.profile_id = p.steam_id) AS total_games,

                    -- Completed Games
                    (SELECT COALESCE(COUNT(DISTINCT a.game_id), 0)
                     FROM profile_achievements pa
                     JOIN achievements a ON pa.achievement_id = a.id
                     WHERE pa.profile_id = p.steam_id
                       AND pa.completeness = 1
                       AND NOT EXISTS (
                           SELECT 1 
                           FROM achievements a2
                           WHERE a2.game_id = a.game_id
                             AND NOT EXISTS (
                                 SELECT 1 
                                 FROM profile_achievements pa2
                                 WHERE pa2.achievement_id = a2.id
                                   AND pa2.profile_id = p.steam_id
                                   AND pa2.completeness = 1
                             )
                       )) AS completed_games,

                    -- Total Playtime
                    (SELECT COALESCE(SUM(pg.playtime), 0) / 60 
                     FROM profile_games pg
                     WHERE pg.profile_id = p.steam_id) AS total_playtime_hours,

                    -- Rare Achievements
                    (SELECT COALESCE(COUNT(*), 0)
                     FROM profile_achievements pa
                     JOIN achievements a ON pa.achievement_id = a.id
                     WHERE pa.profile_id = p.steam_id
                       AND pa.completeness = 1
                       AND a.rarity < 10) AS rare_achievements,

                    -- Average Completion
                    (SELECT COALESCE(AVG(pa.completeness * 100), 0)
                     FROM profile_achievements pa
                     JOIN achievements a ON pa.achievement_id = a.id
                     WHERE pa.profile_id = p.steam_id) AS avg_achievement_completion

                FROM profiles p
                WHERE p.steam_id = %s
            )
            SELECT * FROM profile_stats;
        """
        return self.execute_query(query, (steam_id_int,))

    def insert_profile(self, nickname: str, steam_id: str,
                       registration_date: datetime, avatar_url: str) -> bool:
        try:
            steam_id_int = int(steam_id)
        except ValueError:
            return False

        query = """
            INSERT INTO profiles (steam_id, nickname, registration_date, avatar_url)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                nickname = VALUES(nickname),
                registration_date = VALUES(registration_date),
                avatar_url = VALUES(avatar_url)
        """
        return self.execute_update(query, (steam_id_int, nickname, registration_date, avatar_url))

    def get_profile_games(self, steam_id: str) -> List[Dict]:
        """Get games with completion stats for a profile using steam_id"""
        query = """
            SELECT 
                g.name,
                COUNT(a.id) AS total_achievements,
                COALESCE(SUM(pa.completeness), 0) AS completed_achievements,
                CASE 
                    WHEN COUNT(a.id) > 0 
                    THEN ROUND(SUM(pa.completeness) * 100.0 / COUNT(a.id), 2) 
                    ELSE 0 
                END AS completion_percent
            FROM profile_games pg
            JOIN games g ON pg.game_id = g.app_id
            LEFT JOIN achievements a ON g.app_id = a.game_id
            LEFT JOIN profile_achievements pa 
                ON a.id = pa.achievement_id 
                AND pa.profile_id = pg.profile_id
            WHERE pg.profile_id = %s
            GROUP BY g.name
            ORDER BY g.name
        """
        return self.execute_query(query, (steam_id,))

    def get_profile_id_by_steam_id(self, steam_id: str) -> Optional[int]:
        """Get profile ID by steam_id"""
        query = """
            SELECT id FROM profiles WHERE steam_id = %s
        """
        result = self.execute_query(query, (steam_id,))
        return result[0]['id'] if result else None

    def get_profile_nickname_by_steam_id(self, steam_id: str) -> Optional[str]:
        """Get profile nickname by steam_id"""
        query = """
            SELECT nickname FROM profiles WHERE steam_id = %s
        """
        result = self.execute_query(query, (steam_id,))
        return result[0].get('nickname') if result else None


class SteamStatsApp:
    """Main application controller with UI management"""

    def __init__(self, page: ft.Page):
        self.page = page

        self.configure_window()

        self.api = SteamAPIManager(API_KEY)
        self.db = DBManager()
        self.db.connect()


        self.initialize_ui()
        self.load_profiles()
        self.loading_indicator = self.create_loading_indicator()
        self.page.overlay.append(self.loading_indicator)
        self.processing = False  # Флаг блокировки


    def create_loading_indicator(self):
        return ft.Container(
            content=ft.Column(
                [
                    ft.ProgressRing(width=50, height=50, stroke_width=3),
                    ft.Text("Processing...", size=16)
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER
            ),
            alignment=ft.alignment.center,
            bgcolor=ft.Colors.with_opacity(0.7, ft.Colors.BLACK),
            border_radius=10,
            padding=30,
            visible=False
        )

    def toggle_loading(self, visible: bool):
        self.loading_indicator.visible = visible
        self.processing = visible
        # Блокируем/разблокируем кнопки
        self.top_panel.disabled = visible
        self.page.update()

    def load_profiles(self):
        """Populate profile dropdown from database using steam_id"""
        profiles = self.db.execute_query("SELECT steam_id, nickname FROM profiles")
        if profiles:
            self.profile_combo.options = [
                ft.dropdown.Option(text=profile['nickname'], key=profile['steam_id'])
                for profile in profiles
            ]
            self.page.update()

    def update_display(self, e=None):
        """Update all UI elements with current profile data using steam_id"""
        selected_steam_id = self.profile_combo.value
        if not selected_steam_id:
            return

        stats = self.db.get_profile_statistics(selected_steam_id)

        if not stats or not stats[0]:
            print(f"No statistics found for steam_id: {selected_steam_id}")
            return

        # Получаем актуальные данные
        stats_data = stats[0]
        print("Debug Stats:", stats_data)  # Вывод данных для отладки

        # Обновляем интерфейс
        self.update_stats_table(stats_data)
        self.update_progress_chart(stats_data)
        self.update_rarity_chart(stats_data)
        self.update_avatar(stats_data.get('avatar_url', ''))
        self.nickname_label.value = self.db.get_profile_nickname_by_steam_id(selected_steam_id) or "Unknown"
        self.page.update()

    def update_profile_data(self, e=None):
        """Update the selected profile's data from Steam API using steam_id"""
        try:
            self.toggle_loading(True)  # Показываем индикатор загрузки
            selected_steam_id = self.profile_combo.value
            if not selected_steam_id:
                print("No profile selected.")
                return

            # Fetch updated profile data from Steam API
            profile_info = self.api.get_player_summary(selected_steam_id)
            if not profile_info:
                print("Failed to fetch updated profile data from Steam API.")
                return

            # Update profile data in the database
            self.db.execute_update(
                """
                UPDATE profiles 
                SET registration_date = %s, avatar_url = %s 
                WHERE steam_id = %s
                """,
                (
                    datetime.fromtimestamp(profile_info.get('timecreated', 0)),
                    profile_info.get('avatar', ''),
                    selected_steam_id
                )
            )

            # Clear old game and achievement data
            self.db.execute_update(
                "DELETE FROM profile_games WHERE profile_id = %s",
                (selected_steam_id,)
            )
            self.db.execute_update(
                "DELETE FROM profile_achievements WHERE profile_id = %s",
                (selected_steam_id,)
            )

            # Reload games and achievements
            self.load_games_and_achievements(selected_steam_id)

            # Refresh the UI
            self.update_display()
            print(f"Profile data for {selected_steam_id} updated successfully.")

        except Exception as e:
            print(f"Error updating profile data: {e}")
        finally:
            self.toggle_loading(False)  # Скрываем индикатор загрузки

    def show_games_list(self, e=None):
        """Display a dialog with the list of games for the selected profile using steam_id"""
        selected_steam_id = self.profile_combo.value
        if not selected_steam_id:
            print("No profile selected.")
            return

        try:
            games = self.db.get_profile_games(selected_steam_id)
            if not games:
                print("No games found for this profile.")
                return

            # Создаем таблицу с проверкой значений
            games_table = ft.DataTable(
                columns=[
                    ft.DataColumn(ft.Text("Игра")),
                    ft.DataColumn(ft.Text("Достижений")),
                    ft.DataColumn(ft.Text("Получено")),
                    ft.DataColumn(ft.Text("Завершенность %")),
                ],
                rows=[
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(game['name'])),
                            ft.DataCell(ft.Text(str(game['total_achievements'] or 0))),
                            ft.DataCell(ft.Text(str(game['completed_achievements'] or 0))),
                            ft.DataCell(ft.Text(
                                f"{game['completion_percent'] or 0:.1f}%"
                                if game['completion_percent'] is not None
                                else "0.0%"
                            )),
                        ]
                    )
                    for game in games
                ]
            )

            self.dialog = ft.AlertDialog(
                title=ft.Text("Games List"),
                content=ft.Column(
                    controls=[games_table],
                    scroll=ft.ScrollMode.AUTO,
                    height=400,
                    width=600
                ),
                actions=[ft.ElevatedButton("Закрыть", on_click=self.close_dialog)],
            )

            self.page.overlay.append(self.dialog)
            self.dialog.open = True
            self.page.update()

        except Exception as e:
            print(f"Error displaying games list: {e}")

    def close_dialog(self, e=None):
        """Close the currently open dialog."""
        if hasattr(self, 'dialog'):
            self.dialog.open = False
            self.page.update()

    def configure_window(self):
        """Set up window properties"""
        self.page.title = "Steam Statistics"
        self.page.window.width = 750
        self.page.window.height = 800
        self.page.window.min_height = 800
        self.page.window.min_width = 600
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.padding = 20

    def initialize_ui(self):
        """Initialize all UI components"""
        self.create_profile_controls()
        self.create_stats_display()
        self.create_charts()
        self.assemble_layout()

    def create_profile_controls(self):
        """Create profile selection and action controls"""
        self.profile_combo = ft.Dropdown(
            label="Выбрать профиль",
            expand=True,
            on_change=self.update_display
        )

        control_buttons = [
            ft.ElevatedButton("Добавить", on_click=self.show_add_profile_dialog),
            ft.ElevatedButton("Обновить", on_click=self.update_profile_data),
            ft.ElevatedButton("Список игр", on_click=self.show_games_list)
        ]

        self.top_panel = ft.Row(
            controls=[self.profile_combo, *control_buttons],
            spacing=20,
        )

    def create_stats_display(self):
        """Create profile info and statistics display without visible column headers"""
        self.profile_icon = ft.Image(
            src="profile_icon.png",
            width=120,
            height=120,
            fit=ft.ImageFit.CONTAIN
        )

        self.nickname_label = ft.Text(
            "Select a profile",
            size=24,
            weight=ft.FontWeight.BOLD
        )

        # Создаем таблицу с заголовками, но скрываем их
        self.stats_table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Метрика", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Значение", weight=ft.FontWeight.BOLD)),
            ],
            vertical_lines=ft.BorderSide(1, ft.Colors.GREY_300),
            horizontal_lines=ft.BorderSide(1, ft.Colors.GREY_300),
            heading_row_height=0,  # Устанавливаем высоту строки заголовков в 0
            heading_row_color=ft.Colors.TRANSPARENT,  # Делаем фон заголовков прозрачным
        )

    def create_charts(self):
        """Initialize data visualization elements with labels"""
        # Прогресс прохождения
        self.progress_chart = ft.PieChart(
            sections=[],
            sections_space=5,
            center_space_radius=2,
            height=200,
            width=200
        )
        self.progress_label = ft.Text("0%", size=20, weight="bold")

        # Редкие достижения
        self.rarity_chart = ft.PieChart(
            sections=[],
            sections_space=5,
            center_space_radius=2,
            height=200,
            width=200
        )
        self.rarity_label = ft.Text("0%", size=20, weight="bold")

    def assemble_layout(self):
        """Arrange UI components with legend and center alignment"""
        # Создаем контейнеры для диаграмм с легендой
        progress_container = ft.Column(
            [
                ft.Row(
                    [
                        ft.Container(width=20, height=20, bgcolor=ft.Colors.BLUE),
                        ft.Text("Завершенные игры"),
                        self.progress_label
                    ],
                    spacing=10,
                    alignment=ft.MainAxisAlignment.CENTER
                ),
                ft.Container(self.progress_chart, alignment=ft.alignment.center),
            ],
            spacing=10,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER  # Центрируем содержимое
        )

        rarity_container = ft.Column(
            [
                ft.Row(
                    [
                        ft.Container(width=20, height=20, bgcolor=ft.Colors.AMBER),
                        ft.Text("Редкие достижения"),
                        self.rarity_label
                    ],
                    spacing=10,
                    alignment=ft.MainAxisAlignment.CENTER
                ),
                ft.Container(self.rarity_chart, alignment=ft.alignment.center),
            ],
            spacing=10,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER  # Центрируем содержимое
        )

        # Строка с диаграммами
        charts_row = ft.Row(
            [progress_container, rarity_container],
            spacing=40,
            alignment=ft.MainAxisAlignment.CENTER,
            vertical_alignment=ft.CrossAxisAlignment.CENTER
        )

        # Основной контент
        main_column = ft.Column(
            controls=[
                self.top_panel,
                ft.Divider(height=10),
                ft.Row(
                    [self.profile_icon, self.stats_table],
                    spacing=40,
                    alignment=ft.MainAxisAlignment.CENTER  # Центрируем профиль и таблицу
                ),
                ft.Divider(height=10),
                charts_row
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER  # Центрируем всё содержимое
        )

        # Добавляем основной контент на страницу
        self.page.add(main_column)

    def update_stats_table(self, stats: Dict):
        """Populate statistics table with current data"""
        self.stats_table.rows.clear()

        metrics = [
            ("Всего достижений", stats.get('total_achievements', 0)),
            ("Всего игр", stats.get('total_games', 0)),
            ("Дата регистрации", stats.get('registration_date', datetime.now()).strftime("%Y-%m-%d")),
            ("Завершенные игры", stats.get('completed_games', 0)),
            ("Время в играх", f"{stats.get('total_playtime_hours', 0.0):.1f}"),
            ("Редкие достижения", stats.get('rare_achievements', 0)),
            ("Средняя завершенность игр", f"{stats.get('avg_achievement_completion', 0.0):.1f}%")
        ]

        for label, value in metrics:
            self.stats_table.rows.append(
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(label)),
                    ft.DataCell(ft.Text(str(value)))
                ])
            )

    def update_avatar(self, url: str):
        """Update profile avatar image"""
        if image_data := self.api.get_avatar_image(url):
            self.profile_icon.src_base64 = base64.b64encode(image_data).decode()
            self.page.update()

    # Profile management methods
    def show_add_profile_dialog(self, e):
        """Display profile addition dialog"""
        if self.processing:
            return  # Игнорируем нажатие, если уже идет обработка

        url_field = ft.TextField(label="Steam Profile URL", expand=True)

        def save_profile(e):
            if url := url_field.value:
                self.process_new_profile(url)
                self.load_profiles()
                self.dialog.open = False
                self.page.update()

        self.dialog = ft.AlertDialog(
            title=ft.Text("Добавить новый профиль"),
            content=url_field,
            actions=[ft.ElevatedButton("Сохранить", on_click=save_profile)]
        )

        self.page.dialog = self.dialog
        self.dialog.open = True
        self.page.update()

    def process_new_profile(self, profile_url: str):
        """Handle new profile addition logic"""
        try:
            self.toggle_loading(True)  # Показываем индикатор загрузки
            steam_id = self.extract_steam_id(profile_url)
            if not steam_id:
                return

            if profile_data := self.api.get_player_summary(steam_id):
                success = self.db.insert_profile(
                    nickname=profile_data.get('personaname', 'Unknown'),
                    steam_id=steam_id,
                    registration_date=datetime.fromtimestamp(profile_data.get('timecreated', 0)),
                    avatar_url=profile_data.get('avatar', '')
                )

                if success:
                    self.load_games_and_achievements(steam_id)
                    self.load_profiles()  # Обновляем список профилей после добавления
                    self.page.update()
        except Exception as e:
            print(f"Error processing new profile: {e}")
        finally:
            self.toggle_loading(False)  # Скрываем индикатор загрузки

    def extract_steam_id(self, url: str) -> Optional[str]:
        """Extract SteamID from profile URL"""
        patterns = [
            ("steamcommunity.com/id/", lambda x: x.split("/")[0]),
            ("steamcommunity.com/profiles/", lambda x: x.split("/")[0])
        ]

        for pattern, processor in patterns:
            if pattern in url:
                return processor(url.split(pattern)[1])
        return None

    def load_games_and_achievements(self, steam_id: str):
        try:
            steam_id_int = int(steam_id)
            games = self.api.get_owned_games(steam_id)
            total_games = len(games)

            schema_cache = {}
            game_batch = []
            profile_game_batch = []
            achievement_batch = []
            profile_achievement_batch = []

            for i, game in enumerate(games):
                game_id = game['appid']

                # Добавляем игру в профиль
                profile_game_batch.append((
                    steam_id_int,
                    game_id,
                    game.get('playtime_forever', 0)
                ))

                # Добавляем информацию об игре
                game_batch.append((game_id, game.get('name', 'Unknown')))

                # Получаем достижения только для игр со статистикой
                if game.get('has_community_visible_stats', 0) == 1:
                    try:
                        # Получаем данные об игровых достижениях
                        player_achievements = self.api.get_player_achievements(steam_id, game_id)

                        # Получаем глобальную статистику достижений
                        if game_id not in schema_cache:
                            schema_cache[game_id] = self.api.get_achievement_schema(game_id)

                        # Обрабатываем каждое достижение
                        for ach in player_achievements:
                            apiname = ach.get('apiname')
                            if not apiname:
                                continue

                            # Получаем редкость из кэша
                            rarity = schema_cache[game_id].get(apiname, 0.0)

                            achievement_batch.append((
                                game_id,
                                apiname,
                                rarity  # Добавляем показатель редкости
                            ))

                            profile_achievement_batch.append((
                                apiname,
                                int(ach.get('achieved', 0))
                            ))

                    except Exception as e:
                        print(f"Error processing achievements for app {game_id}: {e}")

                # Обновление прогресса
                if i % 10 == 0 or i == total_games - 1:
                    self.update_progress(i + 1, total_games)

            # Пакетная вставка данных
            self.bulk_insert_games(game_batch)
            self.bulk_insert_profile_games(profile_game_batch)
            self.bulk_insert_achievements(achievement_batch)
            self.bulk_insert_profile_achievements(profile_achievement_batch, steam_id_int)

            self.show_completion_message()

        except Exception as e:
            print(f"Error in load_games_and_achievements: {e}")
            raise
        finally:
            self.toggle_loading(False)

    def update_progress(self, processed: int, total: int):
        self.loading_indicator.content.controls[1].value = f"Processed {processed}/{total} games"
        self.page.update()

    def bulk_insert_games(self, batch):
        if batch:
            self.db.execute_update(
                """INSERT INTO games (app_id, name)
                   VALUES (%s, %s)
                   ON DUPLICATE KEY UPDATE name = VALUES(name)""",
                batch,
                many=True
            )

    def bulk_insert_profile_games(self, batch):
        if batch:
            self.db.execute_update(
                """INSERT INTO profile_games (profile_id, game_id, playtime)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE playtime = VALUES(playtime)""",
                batch,
                many=True
            )

    def bulk_insert_achievements(self, batch):
        if batch:
            self.db.execute_update(
                """INSERT INTO achievements (game_id, achievement_name, rarity)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE 
                       achievement_name = VALUES(achievement_name),
                       rarity = VALUES(rarity)""",
                batch,
                many=True
            )

    def bulk_insert_profile_achievements(self, batch, steam_id):
        if batch:
            achievement_ids = self.get_achievement_ids([a[0] for a in batch])
            final_batch = [
                (steam_id, achievement_ids[apiname], completeness)
                for apiname, completeness in batch
                if apiname in achievement_ids
            ]
            if final_batch:
                self.db.execute_update(
                    """INSERT INTO profile_achievements (profile_id, achievement_id, completeness)
                       VALUES (%s, %s, %s)
                       ON DUPLICATE KEY UPDATE completeness = VALUES(completeness)""",
                    final_batch,
                    many=True
                )

    def get_achievement_ids(self, apinames):
        achievement_ids = {}
        for i in range(0, len(apinames), 100):
            batch = apinames[i:i + 100]
            placeholders = ",".join(["%s"] * len(batch))
            res = self.db.execute_query(
                f"SELECT id, achievement_name FROM achievements WHERE achievement_name IN ({placeholders})",
                batch
            )
            achievement_ids.update({row['achievement_name']: row['id'] for row in res})
        return achievement_ids

    def show_completion_message(self):
        self.loading_indicator.content.controls[1].value = "Processing completed"
        self.page.update()

    def update_progress_chart(self, stats: Dict):
        """Update progress chart with real data and external label"""
        completed = stats.get('completed_games', 0)
        total = stats.get('total_games', 0)

        percent = (completed / total) * 100 if total > 0 else 0
        self.progress_label.value = f"{percent:.1f}%"

        self.progress_chart.sections = [
            ft.PieChartSection(percent, color=ft.Colors.BLUE, radius=100),
            ft.PieChartSection(100 - percent, color=ft.Colors.GREY_300, radius=100),
        ]
        self.progress_chart.update()

    def update_rarity_chart(self, stats: Dict):
        """Update rarity chart with real data and external label"""
        rare = stats.get('rare_achievements', 0)
        total = stats.get('total_achievements', 0)

        percent = (rare / total) * 100 if total > 0 else 0
        self.rarity_label.value = f"{percent:.1f}%"

        self.rarity_chart.sections = [
            ft.PieChartSection(percent, color=ft.Colors.AMBER, radius=100),
            ft.PieChartSection(100 - percent, color=ft.Colors.GREEN_300, radius=100),
        ]
        self.rarity_chart.update()

def main(page: ft.Page):
    SteamStatsApp(page)


ft.app(target=main)