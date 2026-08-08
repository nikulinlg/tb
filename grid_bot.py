"""
Спотовый грид-бот для торговли в Тинькофф Инвестициях
Этот бот реализует стратегию сеточной торговли (grid trading) на спотовом рынке.

Стратегия:
- Создаёт сетку ордеров в заданном ценовом диапазоне
- Покупает при падении цены на определённый шаг
- Продаёт при росте цены на определённый шаг
- Получает прибыль от колебаний цены в боковике

Для запуска необходимо:
1. Получить токен в личном кабинете Тинькофф Инвестиций
2. Установить токен в переменную окружения TINKOFF_TOKEN
3. Настроить параметры бота в конфигурации
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict
from dataclasses import dataclass, field
import json

from tinkoff.investments import TinkoffInvestmentsRESTClient, Environment, CandleResolution
from tinkoff.investments.api.user import UserAPI
from tinkoff.investments.api.market import MarketCandlesAPI, MarketOrderBooksAPI
from tinkoff.investments.api.orders import OrdersAPI
from tinkoff.investments.api.portfolio import PortfolioAPI
from tinkoff.investments.model.operations import OperationType as ModelOperationType
from tinkoff.investments.model.orders import Order, PlacedLimitOrder
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grid_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class GridConfig:
    """Конфигурация грид-бота"""
    # Торговые параметры
    figi: str = "BBG004730N88"  # FIGI инструмента (по умолчанию SBER)
    quantity_per_grid: int = 10  # Количество лотов в одной ячейке сетки
    
    # Параметры сетки
    grid_levels: int = 10  # Количество уровней сетки
    grid_step_percent: float = 1.0  # Шаг сетки в процентах
    
    # Ценовой диапазон (если None, будет определён автоматически)
    price_lower: Optional[float] = None  # Нижняя граница
    price_upper: Optional[float] = None  # Верхняя граница
    
    # Управление рисками
    max_investment_rub: float = 100000.0  # Максимальная сумма инвестиций в рублях
    stop_loss_percent: Optional[float] = None  # Стоп-лосс в процентах (опционально)
    
    # Режимы работы
    demo_mode: bool = True  # Демонстрационный режим (без реальных сделок)
    rebalance_enabled: bool = True  # Автоматическая ребалансировка сетки


@dataclass
class GridOrder:
    """Ордер в сетке"""
    level: int  # Уровень сетки
    order_type: str  # 'buy' или 'sell'
    price: float  # Цена ордера
    quantity: int  # Количество лотов
    order_id: Optional[str] = None  # ID ордера в бирже
    status: str = 'pending'  # pending, active, filled, cancelled
    created_at: datetime = field(default_factory=datetime.now)
    filled_at: Optional[datetime] = None
    fill_price: Optional[float] = None


@dataclass
class Trade:
    """Запись о совершённой сделке"""
    timestamp: datetime
    order_type: str
    price: float
    quantity: int
    amount: float
    profit: float = 0.0
    grid_level: int = 0


class SpotGridBot:
    """Спотовый грид-бот для Тинькофф Инвестиций"""
    
    def __init__(self, config: GridConfig):
        self.config = config
        self.token = os.getenv('TINKOFF_TOKEN', '')
        
        if not self.token and not config.demo_mode:
            raise ValueError("Необходимо указать токен TINKOFF_TOKEN для работы без демо-режима")
        
        # Клиент Тинькофф
        self.client = TinkoffInvestmentsRESTClient(token=self.token) if self.token else None
        
        # Состояние бота
        self.grid_orders: List[GridOrder] = []
        self.trades: List[Trade] = []
        self.current_price: float = 0.0
        self.is_running: bool = False
        
        # Статистика
        self.total_profit: float = 0.0
        self.total_trades: int = 0
        self.start_time: Optional[datetime] = None
        
        # История цен для визуализации
        self.price_history: List[Dict] = []
        
        logger.info(f"Грид-бот инициализирован в режиме: {'DEMO' if config.demo_mode else 'LIVE'}")
        logger.info(f"Инструмент: {config.figi}")
        logger.info(f"Уровней сетки: {config.grid_levels}")
        logger.info(f"Шаг сетки: {config.grid_step_percent}%")
    
    async def connect(self):
        """Подключение к API Тинькофф"""
        if self.client and not self.config.demo_mode:
            try:
                # Проверка подключения
                account = await self.client.user.get_accounts()
                logger.info(f"Подключено к аккаунту: {account}")
            except Exception as e:
                logger.error(f"Ошибка подключения: {e}")
                raise
    
    async def get_current_price(self) -> float:
        """Получение текущей цены инструмента"""
        if self.config.demo_mode:
            # В демо-режиме используем случайные колебания для симуляции
            if not self.price_history:
                self.current_price = 100.0  # Начальная цена
            else:
                # Симуляция случайного блуждания цены
                import random
                change = random.uniform(-0.5, 0.5)
                self.current_price *= (1 + change / 100)
            
            self.price_history.append({
                'timestamp': datetime.now(),
                'price': self.current_price
            })
            
            return self.current_price
        
        try:
            # Получение последней цены из свечей
            from_date = datetime.now() - timedelta(minutes=5)
            candles = await self.client.market.candles.get_candles(
                figi=self.config.figi,
                from_=from_date,
                to=datetime.now(),
                interval='1min'
            )
            
            if candles and len(candles) > 0:
                self.current_price = float(candles[-1].close)
                self.price_history.append({
                    'timestamp': datetime.now(),
                    'price': self.current_price
                })
                return self.current_price
            
            # Если нет свечей, пробуем получить цену из стакана
            orderbook = await self.client.market.orderbooks.get_orderbook(figi=self.config.figi, depth=1)
            if orderbook:
                self.current_price = (orderbook.bids[0].price + orderbook.asks[0].price) / 2
                return self.current_price
                
        except Exception as e:
            logger.error(f"Ошибка получения цены: {e}")
        
        return self.current_price if self.current_price > 0 else 100.0
    
    def calculate_grid(self, current_price: float) -> List[GridOrder]:
        """Расчёт уровней сетки на основе текущей цены"""
        orders = []
        
        # Определение ценового диапазона
        if self.config.price_lower and self.config.price_upper:
            lower = self.config.price_lower
            upper = self.config.price_upper
        else:
            # Автоматический расчёт диапазона
            range_percent = self.config.grid_step_percent * self.config.grid_levels / 2
            lower = current_price * (1 - range_percent / 100)
            upper = current_price * (1 + range_percent / 100)
        
        step = (upper - lower) / self.config.grid_levels
        
        logger.info(f"Диапазон сетки: {lower:.2f} - {upper:.2f}, шаг: {step:.2f}")
        
        # Создание уровней сетки
        for i in range(self.config.grid_levels + 1):
            price = lower + i * step
            
            # Чередование buy/sell ордеров относительно текущей цены
            if price < current_price:
                order_type = 'buy'
            elif price > current_price:
                order_type = 'sell'
            else:
                continue  # Пропускаем уровень текущей цены
            
            order = GridOrder(
                level=i,
                order_type=order_type,
                price=price,
                quantity=self.config.quantity_per_grid
            )
            orders.append(order)
        
        return orders
    
    async def place_order(self, order: GridOrder) -> bool:
        """Размещение ордера на бирже"""
        if self.config.demo_mode:
            logger.info(f"[DEMO] Ордер: {order.order_type.upper()} {order.quantity} @ {order.price:.2f}")
            order.status = 'active'
            order.order_id = f"demo_{len(self.trades)}"
            return True
        
        try:
            if order.order_type == 'buy':
                response = await self.client.orders.create_limit_order(
                    figi=self.config.figi,
                    quantity=order.quantity,
                    price=Decimal(str(order.price)),
                    direction=OrderDirection.BUY
                )
            else:
                response = await self.client.orders.create_limit_order(
                    figi=self.config.figi,
                    quantity=order.quantity,
                    price=Decimal(str(order.price)),
                    direction=OrderDirection.SELL
                )
            
            order.order_id = response.order_id
            order.status = 'active'
            logger.info(f"Ордер размещён: {order.order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка размещения ордера: {e}")
            order.status = 'cancelled'
            return False
    
    async def check_orders(self):
        """Проверка исполнения ордеров"""
        for order in self.grid_orders:
            if order.status != 'active':
                continue
            
            # Проверка исполнения ордера
            if order.order_type == 'buy' and self.current_price <= order.price:
                await self.execute_order(order, 'buy')
            elif order.order_type == 'sell' and self.current_price >= order.price:
                await self.execute_order(order, 'sell')
    
    async def execute_order(self, order: GridOrder, order_type: str):
        """Исполнение ордера"""
        order.status = 'filled'
        order.filled_at = datetime.now()
        order.fill_price = self.current_price
        
        amount = order.price * order.quantity
        
        # Расчёт прибыли для sell-ордера
        profit = 0.0
        if order_type == 'sell':
            # Находим соответствующий buy-ордер для расчёта прибыли
            for buy_order in self.grid_orders:
                if buy_order.order_type == 'buy' and buy_order.level < order.level:
                    buy_amount = buy_order.price * buy_order.quantity
                    profit = amount - buy_amount
                    break
        
        trade = Trade(
            timestamp=datetime.now(),
            order_type=order_type,
            price=order.price,
            quantity=order.quantity,
            amount=amount,
            profit=profit,
            grid_level=order.level
        )
        
        self.trades.append(trade)
        self.total_trades += 1
        self.total_profit += profit
        
        logger.info(f"Исполнен ордер: {order_type.upper()} {order.quantity} @ {order.price:.2f}, прибыль: {profit:.2f}")
        
        # Пересоздание ордера на противоположной стороне
        if self.config.rebalance_enabled:
            await self.replace_order(order)
    
    async def replace_order(self, filled_order: GridOrder):
        """Пересоздание ордера после исполнения"""
        new_level = filled_order.level
        
        # Если был buy, создаём sell на уровне выше
        if filled_order.order_type == 'buy':
            new_level = min(filled_order.level + 1, self.config.grid_levels)
            new_type = 'sell'
        # Если был sell, создаём buy на уровне ниже
        else:
            new_level = max(filled_order.level - 1, 0)
            new_type = 'buy'
        
        new_order = GridOrder(
            level=new_level,
            order_type=new_type,
            price=self.grid_orders[new_level].price if new_level < len(self.grid_orders) else filled_order.price,
            quantity=self.config.quantity_per_grid
        )
        
        self.grid_orders.append(new_order)
        await self.place_order(new_order)
    
    async def run(self, duration_hours: int = 24):
        """Запуск бота"""
        self.is_running = True
        self.start_time = datetime.now()
        
        logger.info(f"Запуск бота на {duration_hours} ч.")
        
        # Получение начальной цены
        current_price = await self.get_current_price()
        logger.info(f"Начальная цена: {current_price:.2f}")
        
        # Расчёт и размещение начальной сетки
        self.grid_orders = self.calculate_grid(current_price)
        
        for order in self.grid_orders:
            await self.place_order(order)
            await asyncio.sleep(0.1)  # Задержка между ордерами
        
        # Основной цикл
        end_time = self.start_time + timedelta(hours=duration_hours)
        check_interval = 10  # Проверка каждые 10 секунд
        
        while self.is_running and datetime.now() < end_time:
            try:
                # Обновление цены
                current_price = await self.get_current_price()
                
                # Проверка ордеров
                await self.check_orders()
                
                # Логирование статистики
                if len(self.trades) % 10 == 0 and len(self.trades) > 0:
                    logger.info(f"Статистика: сделок={self.total_trades}, прибыль={self.total_profit:.2f}")
                
                await asyncio.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("Остановка по команде пользователя")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле: {e}")
                await asyncio.sleep(60)  # Пауза при ошибке
        
        await self.stop()
    
    async def stop(self):
        """Остановка бота"""
        self.is_running = False
        logger.info("Бот остановлен")
        
        # Отмена активных ордеров в live-режиме
        if not self.config.demo_mode and self.client:
            for order in self.grid_orders:
                if order.status == 'active' and order.order_id:
                    try:
                        await self.client.orders.cancel(order_id=order.order_id)
                    except Exception as e:
                        logger.error(f"Ошибка отмены ордера: {e}")
        
        # Сохранение статистики
        self.save_statistics()
    
    def save_statistics(self):
        """Сохранение статистики в файл"""
        stats = {
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': datetime.now().isoformat(),
            'total_trades': self.total_trades,
            'total_profit': self.total_profit,
            'trades': [
                {
                    'timestamp': t.timestamp.isoformat(),
                    'type': t.order_type,
                    'price': t.price,
                    'quantity': t.quantity,
                    'amount': t.amount,
                    'profit': t.profit,
                    'level': t.grid_level
                }
                for t in self.trades
            ]
        }
        
        with open('grid_bot_stats.json', 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info("Статистика сохранена в grid_bot_stats.json")
    
    def create_visualization(self, output_file: str = 'grid_bot_visualization.html'):
        """Создание интерактивной визуализации работы бота"""
        if not self.price_history:
            logger.warning("Нет данных для визуализации")
            return
        
        # Подготовка данных
        df_prices = pd.DataFrame(self.price_history)
        df_prices['timestamp'] = pd.to_datetime(df_prices['timestamp'])
        df_prices.set_index('timestamp', inplace=True)
        
        # Создание подграфиков
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.5, 0.25, 0.25],
            subplot_titles=(
                'Цена и уровни сетки',
                'Совершённые сделки',
                'Накопленная прибыль'
            )
        )
        
        # 1. График цены с уровнями сетки
        fig.add_trace(
            go.Scatter(
                x=df_prices.index,
                y=df_prices['price'],
                mode='lines',
                name='Цена',
                line=dict(color='blue', width=2)
            ),
            row=1, col=1
        )
        
        # Добавление уровней сетки
        for i, order in enumerate(self.grid_orders[:20]):  # Ограничим количество для читаемости
            color = 'green' if order.order_type == 'buy' else 'red'
            fig.add_trace(
                go.Scatter(
                    x=[df_prices.index[0], df_prices.index[-1]],
                    y=[order.price, order.price],
                    mode='lines',
                    name=f"{'Buy' if order.order_type == 'buy' else 'Sell'} L{order.level}",
                    line=dict(color=color, width=1, dash='dash'),
                    opacity=0.5
                ),
                row=1, col=1
            )
        
        # 2. График сделок
        if self.trades:
            df_trades = pd.DataFrame([
                {
                    'timestamp': t.timestamp,
                    'price': t.price,
                    'type': t.order_type,
                    'profit': t.profit
                }
                for t in self.trades
            ])
            df_trades['timestamp'] = pd.to_datetime(df_trades['timestamp'])
            
            buy_trades = df_trades[df_trades['type'] == 'buy']
            sell_trades = df_trades[df_trades['type'] == 'sell']
            
            fig.add_trace(
                go.Scatter(
                    x=buy_trades['timestamp'],
                    y=buy_trades['price'],
                    mode='markers',
                    name='Покупки',
                    marker=dict(color='green', size=10, symbol='triangle-up')
                ),
                row=2, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=sell_trades['timestamp'],
                    y=sell_trades['price'],
                    mode='markers',
                    name='Продажи',
                    marker=dict(color='red', size=10, symbol='triangle-down')
                ),
                row=2, col=1
            )
        
        # 3. График накопленной прибыли
        if self.trades:
            cumulative_profit = []
            total = 0
            for t in self.trades:
                total += t.profit
                cumulative_profit.append(total)
            
            df_trades_sorted = df_trades.sort_values('timestamp')
            
            fig.add_trace(
                go.Scatter(
                    x=df_trades_sorted['timestamp'],
                    y=cumulative_profit,
                    mode='lines+markers',
                    name='Прибыль',
                    line=dict(color='purple', width=2),
                    fill='tozeroy'
                ),
                row=3, col=1
            )
        
        # Настройка макета
        fig.update_layout(
            height=900,
            title_text=f"Визуализация работы Грид-бота\nИнструмент: {self.config.figi} | "
                      f"Сделок: {self.total_trades} | Прибыль: {self.total_profit:.2f}",
            showlegend=True,
            hovermode='x unified',
            template='plotly_white'
        )
        
        # Настройка осей
        fig.update_xaxes(title_text="Время", row=3, col=1)
        fig.update_yaxes(title_text="Цена (RUB)", row=1, col=1)
        fig.update_yaxes(title_text="Цена сделки", row=2, col=1)
        fig.update_yaxes(title_text="Прибыль (RUB)", row=3, col=1)
        
        # Сохранение
        fig.write_html(output_file)
        logger.info(f"Визуализация сохранена в {output_file}")
        
        return fig


async def main():
    """Основная функция запуска"""
    # Конфигурация бота
    config = GridConfig(
        figi="BBG004730N88",  # SBER
        quantity_per_grid=10,
        grid_levels=10,
        grid_step_percent=1.0,
        max_investment_rub=100000.0,
        demo_mode=True,  # Включить демо-режим для тестирования
        rebalance_enabled=True
    )
    
    # Создание и запуск бота
    bot = SpotGridBot(config)
    
    try:
        # Подключение
        await bot.connect()
        
        # Запуск на 1 час (для демонстрации)
        await bot.run(duration_hours=1)
        
    except KeyboardInterrupt:
        logger.info("Остановка бота пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        # Создание визуализации
        bot.create_visualization()
        
        # Вывод итоговой статистики
        print("\n" + "="*50)
        print("ИТОГОВАЯ СТАТИСТИКА")
        print("="*50)
        print(f"Всего сделок: {bot.total_trades}")
        print(f"Общая прибыль: {bot.total_profit:.2f} RUB")
        print(f"Файл статистики: grid_bot_stats.json")
        print(f"Визуализация: grid_bot_visualization.html")
        print("="*50)


if __name__ == "__main__":
    asyncio.run(main())
