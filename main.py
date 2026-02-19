import qstock as qs
import pandas as pd
import numpy as np
import datetime
import time
import warnings
import sys
import os
import matplotlib.pyplot as plt
import matplotlib

warnings.filterwarnings('ignore')
pd.set_option('display.unicode.east_asian_width', True)
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'Heiti TC', 'SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei']
plt.rcParams['axes.unicode_minus'] = False

# ==========================================
# 数据缓存类
# ==========================================
class DataCache:
    def __init__(self, cache_duration=300, data_dir='market_data'):
        self.cache = {}
        self.cache_duration = cache_duration
        self.last_fetch_time = None
        self.realtime_data = None
        self.data_dir = data_dir
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

    def _get_today_filename(self):
        today = datetime.datetime.now().strftime('%Y%m%d')
        return os.path.join(self.data_dir, f'market_data_{today}.csv')

    def get_realtime_data(self, force_refresh=False):
        current_time = time.time()
        today_file = self._get_today_filename()
        if not force_refresh and os.path.exists(today_file):
            print(f"📁 读取本地缓存数据: {today_file}")
            try:
                df = pd.read_csv(today_file)
                df = clean_data(df)
                self.realtime_data = df
                self.last_fetch_time = current_time
                return df
            except Exception as e:
                print(f"⚠️ 读取本地文件失败，将重新获取: {e}")
        try:
            print("🔄 正在获取实时行情数据...")
            df = qs.realtime_data()
            if df is not None and len(df) > 100:
                df.to_csv(today_file, index=False, encoding='utf-8-sig')
                print(f"💾 数据已保存至: {today_file}")
                self.realtime_data = df
                self.last_fetch_time = current_time
                return df
        except Exception as e:
            print(f"❌ 获取失败: {e}")
            return None

    def get_history_data(self, code, start_date, end_date, force_refresh=False):
        cache_key = f"{code}_{start_date}_{end_date}"
        current_time = time.time()
        if not force_refresh and cache_key in self.cache:
            cache_time, data = self.cache[cache_key]
            if current_time - cache_time < self.cache_duration * 2:
                return data
        try:
            df = qs.get_data(code, start=start_date, end=end_date)
            if df is not None and not df.empty:
                self.cache[cache_key] = (current_time, df)
                return df
        except:
            return None

    def batch_get_history_data(self, code_list, start_date, end_date, show_progress=True):
        results = {}
        total = len(code_list)
        for i, code in enumerate(code_list):
            if show_progress and (i + 1) % 10 == 0:
                print(f"  进度: {i + 1}/{total}")
            cache_key = f"{code}_{start_date}_{end_date}"
            current_time = time.time()
            if cache_key in self.cache:
                cache_time, data = self.cache[cache_key]
                if current_time - cache_time < self.cache_duration * 2:
                    results[code] = data
                    continue
            try:
                time.sleep(0.02)
                df = qs.get_data(code, start=start_date, end=end_date)
                if df is not None and not df.empty:
                    self.cache[cache_key] = (current_time, df)
                    results[code] = df
            except:
                continue
        return results

    def clear_expired_cache(self):
        current_time = time.time()
        expired_keys = []
        for key, (cache_time, _) in self.cache.items():
            if current_time - cache_time > self.cache_duration * 3:
                expired_keys.append(key)
        for key in expired_keys:
            del self.cache[key]
        if expired_keys:
            print(f"🧹 已清除 {len(expired_keys)} 个过期缓存")

cache = DataCache()

# ==========================================
# Helper Functions
# ==========================================
def clean_data(df):
    rename_map = {
        '代码': 'code', '名称': 'name',
        '最新': 'close', '最新价': 'close',
        '涨幅': 'pct_chg', '涨跌幅': 'pct_chg',
        '换手率': 'turnover', '换手': 'turnover',
        '市盈率': 'pe', '市盈率(动)': 'pe',
        '成交量': 'volume', '成交额': 'amount',
        '量比': 'vol_ratio', '流通市值': 'float_mv'
    }
    existing_cols = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_cols)
    numeric_cols = ['close', 'pe', 'turnover', 'pct_chg', 'volume', 'amount', 'float_mv']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def is_gem_stock(code):
    try:
        return str(code).zfill(6).startswith('300')
    except:
        return False

def is_kcb_stock(code):
    try:
        return str(code).zfill(6).startswith('688')
    except:
        return False

def is_bse_stock(code):
    try:
        return str(code).zfill(6).startswith('8')
    except:
        return False

def is_st_stock(name):
    try:
        return 'ST' in name or '退' in name or '*' in name or 'N' in name
    except:
        return False

# ==========================================
# 裸K形态识别函数
# ==========================================
def is_pinbar(kline, idx, lookback=20):
    """
    Pinbar（锤子线/倒锤子线）- 出现在支撑位附近
    """
    if idx < lookback or idx >= len(kline):
        return False
    
    recent_lows = kline['low'].iloc[idx-lookback:idx].min()
    current_low = kline['low'].iloc[idx]
    near_support = current_low <= recent_lows * 1.03
    
    open_ = kline['open'].iloc[idx]
    high = kline['high'].iloc[idx]
    low = kline['low'].iloc[idx]
    close = kline['close'].iloc[idx]
    
    body = abs(close - open_)
    lower_shadow = min(open_, close) - low
    upper_shadow = high - max(open_, close)
    
    if body == 0:
        return False
    
    is_hammer = lower_shadow > 2 * body and upper_shadow < body and near_support
    is_shooting = upper_shadow > 2 * body and lower_shadow < body and near_support
    
    return is_hammer or is_shooting

def is_engulfing_at_support(kline, idx, lookback=20):
    """
    看涨吞没形态 - 出现在支撑位
    """
    if idx < 1 or idx >= len(kline):
        return False
    
    recent_lows = kline['low'].iloc[max(0, idx-lookback):idx].min()
    current_low = kline['low'].iloc[idx]
    near_support = current_low <= recent_lows * 1.03
    
    prev_open = kline['open'].iloc[idx-1]
    prev_close = kline['close'].iloc[idx-1]
    curr_open = kline['open'].iloc[idx]
    curr_close = kline['close'].iloc[idx]
    
    if prev_close >= prev_open or curr_close <= curr_open:
        return False
    
    engulfing = curr_open < prev_close and curr_close > prev_open
    
    return engulfing and near_support

def is_breakout_with_volume(kline, idx, period=20):
    """
    带量突破
    """
    if idx < period or idx >= len(kline):
        return False
    
    high_period = kline['high'].iloc[idx-period:idx].max()
    close = kline['close'].iloc[idx]
    
    if close <= high_period:
        return False
    
    if 'volume' in kline.columns and idx > 0:
        avg_vol = kline['volume'].iloc[idx-10:idx].mean()
        current_vol = kline['volume'].iloc[idx]
        vol_confirm = current_vol > avg_vol * 1.2
    else:
        vol_confirm = True
    
    return vol_confirm

# ==========================================
# 市场分析器
# ==========================================
class MarketAnalyzer:
    def __init__(self):
        self.market_data = None
        self.last_update = None

    def update_market_data(self, start_date, end_date):
        try:
            self.market_data = cache.get_history_data('000001', start_date, end_date)
            if self.market_data is None or len(self.market_data) < 60:
                return False
            self.last_update = datetime.datetime.now()
            return True
        except:
            return False

    def is_market_above_ma20(self, idx):
        if self.market_data is None or idx >= len(self.market_data):
            return True
        market_close = self.market_data['close'].iloc[idx]
        market_ma20 = self.market_data['close'].rolling(20).mean().iloc[idx] if idx >= 20 else market_close
        return market_close > market_ma20

# ==========================================
# 今日推荐（排除创业板/科创板）
# ==========================================
def recommend_latest(engine, top_n=10):
    """
    根据最新交易日推荐符合裸K形态的股票（剔除创业板、科创板、北交所）
    """
    print("\n" + "="*70)
    print("【🔥 今日裸K信号推荐】")
    print("="*70)

    base_conditions = (
        (engine.realtime_data['close'] > 5) &
        (engine.realtime_data['close'] < 200) &
        (engine.realtime_data['turnover'] > 2) &
        (engine.realtime_data['turnover'] < 50) &
        (engine.realtime_data['pe'].notna()) &
        (~engine.realtime_data['name'].apply(is_st_stock))
    )
    pool = engine.realtime_data[base_conditions].copy()
    print(f"基础筛选后: {len(pool)} 只")
    
    pool = pool[~pool['code'].apply(is_gem_stock)]
    pool = pool[~pool['code'].apply(is_kcb_stock)]
    pool = pool[~pool['code'].apply(is_bse_stock)]
    print(f"排除创业板/科创板/北交所后: {len(pool)} 只")
    
    if pool.empty:
        print("❌ 无候选股票")
        return pd.DataFrame()

    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime('%Y%m%d')
    codes = [str(c).zfill(6) for c in pool['code']]
    history_dict = cache.batch_get_history_data(codes, start_date, end_date)

    results = []
    for _, row in pool.iterrows():
        code = str(row['code']).zfill(6)
        name = row['name']
        if code not in history_dict:
            continue
        kline = history_dict[code]
        if kline is None or len(kline) < 60:
            continue

        idx = len(kline) - 1
        signal = None
        if is_pinbar(kline, idx):
            signal = "Pinbar"
        elif is_engulfing_at_support(kline, idx):
            signal = "吞没"
        elif is_breakout_with_volume(kline, idx):
            signal = "突破"

        if signal:
            ma60 = kline['close'].rolling(60).mean().iloc[-1]
            above_ma60 = kline['close'].iloc[-1] > ma60
            if above_ma60:
                results.append({
                    '代码': code,
                    '名称': name,
                    '信号类型': signal,
                    '最新价': round(kline['close'].iloc[-1], 2),
                    '信号日期': kline.index[-1].strftime('%Y-%m-%d'),
                    '市盈率': row['pe'],
                    '换手率%': row['turnover']
                })

    if not results:
        print("❌ 今日无符合信号的股票")
        return pd.DataFrame()

    df_res = pd.DataFrame(results)
    df_res = df_res.sort_values('信号类型').head(top_n)
    print("\n" + "="*70)
    print(f"🏆 【今日推荐 - 前{len(df_res)}只】")
    print("="*70)
    print(df_res.to_string(index=False))

    filename = f'today_recommend_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    df_res.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n💾 结果已保存至: {filename}")
    return df_res

# ==========================================
# 扫描2026年裸K交易机会
# ==========================================
def scan_for_2026(engine, target_date='20260218'):
    print("\n" + "="*70)
    print(f"【🔍 扫描2026年裸K交易机会】截止日期: {target_date}")
    print("="*70)
    
    base_conditions = (
        (engine.realtime_data['close'] > 5) &
        (engine.realtime_data['close'] < 200) &
        (engine.realtime_data['turnover'] > 2) &
        (engine.realtime_data['turnover'] < 50) &
        (engine.realtime_data['pe'].notna()) &
        (~engine.realtime_data['name'].apply(is_st_stock))
    )
    pool = engine.realtime_data[base_conditions].copy()
    print(f"基础筛选后: {len(pool)} 只")
    
    pool = pool[~pool['code'].apply(is_gem_stock)]
    pool = pool[~pool['code'].apply(is_kcb_stock)]
    pool = pool[~pool['code'].apply(is_bse_stock)]
    print(f"排除创业板/科创板/北交所后: {len(pool)} 只")
    
    if pool.empty:
        print("❌ 没有股票通过筛选")
        return pd.DataFrame()
    
    analysis_pool = pool.nlargest(300, 'turnover')
    
    start_date = '20250101'
    end_date = target_date
    
    print(f"\n📅 数据区间: {start_date} 至 {end_date}")
    codes = [str(c).zfill(6) for c in analysis_pool['code']]
    history_dict = cache.batch_get_history_data(codes, start_date, end_date)
    
    results = []
    market_analyzer = MarketAnalyzer()
    market_analyzer.update_market_data(start_date, end_date)
    
    for _, row in analysis_pool.iterrows():
        code = str(row['code']).zfill(6)
        name = row['name']
        if code not in history_dict:
            continue
        kline = history_dict[code]
        if kline is None or len(kline) < 60:
            continue
        
        kline_2026 = kline[kline.index >= '2026-01-01']
        if len(kline_2026) == 0:
            continue
        
        signal_found = False
        signal_price = 0
        signal_date = None
        signal_type = ""
        
        for idx in range(len(kline)):
            if kline.index[idx] < pd.Timestamp('2026-01-01'):
                continue
            
            if is_pinbar(kline, idx):
                signal_found = True
                signal_price = kline['close'].iloc[idx]
                signal_date = kline.index[idx]
                signal_type = "Pinbar"
                break
            
            if is_engulfing_at_support(kline, idx):
                signal_found = True
                signal_price = kline['close'].iloc[idx]
                signal_date = kline.index[idx]
                signal_type = "吞没"
                break
            
            if is_breakout_with_volume(kline, idx):
                signal_found = True
                signal_price = kline['close'].iloc[idx]
                signal_date = kline.index[idx]
                signal_type = "突破"
                break
        
        if signal_found:
            ma60 = kline['close'].rolling(60).mean()
            above_ma60 = kline['close'].iloc[idx] > ma60.iloc[idx] if idx >= 60 else True
            
            if above_ma60:
                results.append({
                    'code': code,
                    'name': name,
                    'signal_date': signal_date.strftime('%Y-%m-%d'),
                    'signal_type': signal_type,
                    'signal_price': round(signal_price, 2),
                    'pe': row['pe'],
                    'turnover': row['turnover']
                })
    
    if not results:
        print("❌ 2026年未找到符合条件的裸K信号")
        return pd.DataFrame()
    
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('signal_date').head(10)
    
    print("\n" + "="*70)
    print("🏆 【2026年裸K交易机会 - 前10个】")
    print("="*70)
    display_df = df_results[['code', 'name', 'signal_date', 'signal_type', 'signal_price', 'pe', 'turnover']].copy()
    display_df.columns = ['代码', '名称', '信号日期', '信号类型', '信号价格', '市盈率', '换手率%']
    print(display_df.to_string(index=False))
    
    filename = f'naked_2026_signals_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    df_results.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n💾 结果已保存至: {filename}")
    
    return df_results

# ==========================================
# 2026年信号回测
# ==========================================
def backtest_2026_signal(code, name, signal_date, signal_price, signal_type, plot=True):
    print("\n" + "-"*60)
    print(f"【📈 2026年裸K回测】 {name}({code}) - 信号: {signal_type} @ {signal_date}")
    print("-"*60)
    
    signal_dt = pd.to_datetime(signal_date)
    start_date = (signal_dt - datetime.timedelta(days=90)).strftime('%Y%m%d')
    end_date = (signal_dt + datetime.timedelta(days=60)).strftime('%Y%m%d')
    
    try:
        code = str(code).zfill(6)
        df = cache.get_history_data(code, start_date, end_date)
        if df is None or df.empty:
            print("❌ 无法获取数据")
            return None
        print(f"✅ 获取数据成功，共 {len(df)} 个交易日")
    except:
        print("❌ 获取数据失败")
        return None
    
    signal_idx = None
    for i in range(len(df)):
        if df.index[i].strftime('%Y-%m-%d') == signal_date:
            signal_idx = i
            break
    
    if signal_idx is None:
        print("❌ 未找到信号日期对应的数据")
        return None
    
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    df['tr'] = np.maximum(df['high'] - df['low'], 
                          np.maximum(abs(df['high'] - df['close'].shift(1)), 
                                     abs(df['low'] - df['close'].shift(1))))
    df['atr'] = df['tr'].rolling(14).mean()
    
    buy_price = signal_price
    buy_idx = signal_idx
    
    sell_idx = None
    atr_at_buy = df['atr'].iloc[buy_idx] if not pd.isna(df['atr'].iloc[buy_idx]) else buy_price * 0.05
    stop_loss = buy_price - 2 * atr_at_buy
    take_profit = buy_price + 4 * atr_at_buy
    highest_price = buy_price
    
    for i in range(buy_idx + 1, len(df)):
        current_price = df['close'].iloc[i]
        highest_price = max(highest_price, current_price)
        
        if current_price >= take_profit:
            sell_idx = i
            sell_reason = 'take_profit'
            break
        if current_price <= stop_loss:
            sell_idx = i
            sell_reason = 'stop_loss'
            break
        if highest_price > buy_price + 1 * atr_at_buy:
            drawdown = (highest_price - current_price) / atr_at_buy
            if drawdown > 1.5:
                sell_idx = i
                sell_reason = 'trailing_stop'
                break
        if i - buy_idx > 20:
            sell_idx = i
            sell_reason = 'time_stop'
            break
    
    if sell_idx is None:
        sell_idx = len(df) - 1
        sell_reason = 'force_sell'
    
    sell_price = df['close'].iloc[sell_idx]
    sell_date = df.index[sell_idx]
    
    strategy_return = (sell_price / buy_price - 1) * 100
    holding_days = (sell_date - df.index[buy_idx]).days
    
    print(f"\n【📊 回测结果】")
    print(f"  买入日期: {signal_date} 价格: {buy_price:.2f}")
    print(f"  卖出日期: {sell_date.strftime('%Y-%m-%d')} 价格: {sell_price:.2f} 原因: {sell_reason}")
    print(f"  持有天数: {holding_days}")
    print(f"  策略收益: {strategy_return:.2f}%")
    
    if plot:
        fig, ax = plt.subplots(figsize=(14, 7))
        ax.plot(df.index, df['close'], label='收盘价', color='black', linewidth=1)
        ax.plot(df.index, df['ma20'], label='MA20', color='blue', linestyle='--', alpha=0.7)
        
        ax.scatter(df.index[buy_idx], buy_price, marker='^', color='red', s=200, label='信号点', zorder=5)
        ax.scatter(df.index[sell_idx], sell_price, marker='v', color='green', s=200, label='卖出点', zorder=5)
        ax.axvspan(df.index[buy_idx], df.index[sell_idx], alpha=0.2, color='yellow')
        
        ax.set_title(f'{name} ({code}) 2026年裸K回测 (收益: {strategy_return:.1f}%)', fontsize=14)
        ax.set_xlabel('日期')
        ax.set_ylabel('价格')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        filename = f'{code}_{name}_2026_backtest.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"📊 图表已保存至: {filename}")
        plt.close()
    
    return {
        'name': name,
        'signal_date': signal_date,
        'signal_type': signal_type,
        'buy_price': round(buy_price, 2),
        'sell_price': round(sell_price, 2),
        'sell_date': sell_date.strftime('%Y-%m-%d'),
        'strategy_return': round(strategy_return, 2),
        'holding_days': holding_days,
        'sell_reason': sell_reason
    }

# ==========================================
# 单只股票历史回测（寻找所有买卖点）
# ==========================================
def backtest_stock_history(engine, stock_code, start_date=None, end_date=None):
    """
    对单只股票进行裸K策略历史回测，寻找所有买点卖点
    参数:
        engine: SimpleEngine对象（包含realtime_data，用于基础筛选，但此处可忽略）
        stock_code: 股票代码（如 '000001' 或 '平安银行'）
        start_date: 起始日期，格式 'YYYYMMDD'，默认自动从足够数据开始
        end_date: 结束日期，格式 'YYYYMMDD'，默认当前日期
    """
    print("\n" + "="*70)
    print(f"【📈 裸K策略历史回测 - {stock_code}】")
    print("="*70)

    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.datetime.strptime(end_date, '%Y%m%d') - datetime.timedelta(days=5*365)).strftime('%Y%m%d')
    else:
        start_date = start_date

    print(f"回测区间: {start_date} 至 {end_date}")

    code = str(stock_code).zfill(6)
    df = cache.get_history_data(code, start_date, end_date)
    if df is None or df.empty:
        print("❌ 无法获取该股票的历史数据")
        return None
    print(f"✅ 获取数据成功，共 {len(df)} 个交易日")

    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    df['tr'] = np.maximum(df['high'] - df['low'], 
                          np.maximum(abs(df['high'] - df['close'].shift(1)), 
                                     abs(df['low'] - df['close'].shift(1))))
    df['atr'] = df['tr'].rolling(14).mean()

    trades = []
    i = 60  # 从第60根K线开始，确保有足够数据计算MA60等

    while i < len(df):
        signal = None
        if is_pinbar(df, i):
            signal = "Pinbar"
        elif is_engulfing_at_support(df, i):
            signal = "吞没"
        elif is_breakout_with_volume(df, i):
            signal = "突破"

        if signal and df['close'].iloc[i] > df['ma60'].iloc[i]:
            buy_price = df['close'].iloc[i]
            buy_date = df.index[i]
            print(f"发现信号: {buy_date.strftime('%Y-%m-%d')} 价格 {buy_price:.2f} 类型 {signal}")

            atr_at_buy = df['atr'].iloc[i] if not pd.isna(df['atr'].iloc[i]) else buy_price * 0.05
            stop_loss = buy_price - 2 * atr_at_buy
            take_profit = buy_price + 4 * atr_at_buy
            highest_price = buy_price
            sell_idx = None
            sell_reason = None

            for j in range(i+1, len(df)):
                current_price = df['close'].iloc[j]
                highest_price = max(highest_price, current_price)

                if current_price >= take_profit:
                    sell_idx = j
                    sell_reason = 'take_profit'
                    break
                if current_price <= stop_loss:
                    sell_idx = j
                    sell_reason = 'stop_loss'
                    break
                if highest_price > buy_price + 1 * atr_at_buy:
                    drawdown = (highest_price - current_price) / atr_at_buy
                    if drawdown > 1.5:
                        sell_idx = j
                        sell_reason = 'trailing_stop'
                        break
                if j - i > 20:
                    sell_idx = j
                    sell_reason = 'time_stop'
                    break

            if sell_idx is None:
                sell_idx = len(df) - 1
                sell_reason = 'force_sell'

            sell_price = df['close'].iloc[sell_idx]
            sell_date = df.index[sell_idx]
            profit_pct = (sell_price / buy_price - 1) * 100
            trades.append({
                '买入日期': buy_date.strftime('%Y-%m-%d'),
                '买入价': round(buy_price, 2),
                '卖出日期': sell_date.strftime('%Y-%m-%d'),
                '卖出价': round(sell_price, 2),
                '持有天数': (sell_date - buy_date).days,
                '盈亏%': round(profit_pct, 2),
                '卖出原因': sell_reason,
                '信号类型': signal
            })

            i = sell_idx + 1
            continue
        i += 1

    if not trades:
        print("❌ 回测区间内未找到任何交易信号")
        return pd.DataFrame()

    df_trades = pd.DataFrame(trades)
    win_trades = df_trades[df_trades['盈亏%'] > 0]
    loss_trades = df_trades[df_trades['盈亏%'] <= 0]

    print("\n" + "="*70)
    print("🏆 【交易明细】")
    print("="*70)
    print(df_trades.to_string(index=False))

    print("\n" + "="*70)
    print("📊 【交易统计】")
    print("="*70)
    print(f"总交易次数: {len(df_trades)}")
    print(f"盈利次数: {len(win_trades)}")
    print(f"亏损次数: {len(loss_trades)}")
    print(f"胜率: {len(win_trades)/len(df_trades)*100:.2f}%")
    print(f"总盈亏: {df_trades['盈亏%'].sum():.2f}%")
    print(f"平均盈亏: {df_trades['盈亏%'].mean():.2f}%")
    if len(win_trades) > 0:
        print(f"平均盈利: {win_trades['盈亏%'].mean():.2f}%")
    if len(loss_trades) > 0:
        print(f"平均亏损: {loss_trades['盈亏%'].mean():.2f}%")

    filename = f'backtest_{code}_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    df_trades.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n💾 交易明细已保存至: {filename}")
    return df_trades

# ==========================================
# 主程序
# ==========================================
if __name__ == "__main__":
    print("="*70)
    print("          裸K交易系统 v4.0 (含单只股票历史回测)")
    print("="*70)

    realtime_df = cache.get_realtime_data(force_refresh=False)
    if realtime_df is None:
        print("❌ 获取实时数据失败，程序退出")
        sys.exit(0)
    realtime_df = clean_data(realtime_df)

    class SimpleEngine:
        def __init__(self):
            self.realtime_data = realtime_df

    engine = SimpleEngine()

    print("\n请选择功能：")
    print("  1. 🔥 获取今日裸K信号推荐")
    print("  2. 📈 对2026年进行裸K回测")
    print("  3. 🕰️ 单只股票历史回测")
    print("  0. ❌ 退出")
    print("="*70)
    choice = input("请输入数字 (0-3): ").strip()

    if choice == '0':
        print("👋 程序退出")
        sys.exit(0)
    elif choice == '1':
        recommend_latest(engine, top_n=10)
    elif choice == '2':
        target_date = '20260218'
        print("\n🔍 正在测试数据源是否包含2026年数据...")
        test_code = '000001'
        test_df = cache.get_history_data(test_code, '20260101', target_date)
        if test_df is None or len(test_df) == 0:
            print(f"❌ 错误：数据源没有2026年的数据！")
            print(f"   建议尝试：1) 更新 qstock 库；2) 更换数据源；3) 使用其他年份")
            sys.exit(1)
        else:
            print(f"✅ 数据源包含2026年数据，最后日期: {test_df.index[-1].strftime('%Y-%m-%d')}")

        signals = scan_for_2026(engine, target_date)
        if not signals.empty:
            print("\n" + "="*70)
            print("【🔄 开始对2026年信号进行回测】")
            print("="*70)
            results = []
            for idx, (_, row) in enumerate(signals.iterrows()):
                print(f"\n[{idx+1}/10] 回测: {row['name']} - {row['signal_type']} @ {row['signal_date']}")
                res = backtest_2026_signal(
                    row['code'], 
                    row['name'], 
                    row['signal_date'], 
                    row['signal_price'],
                    row['signal_type'],
                    plot=True
                )
                if res:
                    results.append(res)
                time.sleep(1)
            if results:
                df_res = pd.DataFrame(results)
                df_res = df_res.sort_values('strategy_return', ascending=False)
                print("\n" + "="*70)
                print("🏆 【2026年裸K交易回测汇总】")
                print("="*70)
                display_cols = ['name', 'signal_date', 'signal_type', 'buy_price', 'sell_price', 'sell_date', 'strategy_return', 'holding_days', 'sell_reason']
                display_df = df_res[display_cols].copy()
                display_df.columns = ['名称', '信号日期', '信号类型', '买入价', '卖出价', '卖出日期', '策略收益%', '持有天数', '卖出原因']
                print(display_df.to_string(index=False))
                
                filename = f'naked_2026_summary_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                df_res.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"\n💾 结果已保存至: {filename}")
    elif choice == '3':
        stock_code = input("请输入股票代码（如 000001）：").strip()
        if stock_code:
            backtest_stock_history(engine, stock_code)
        else:
            print("❌ 股票代码不能为空")
    else:
        print("❌ 输入错误")

    cache.clear_expired_cache()
    print("\n" + "="*70)
    print("✨ 程序执行完成！")
    print("="*70)
